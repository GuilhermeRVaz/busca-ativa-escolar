import logging
import re
import unicodedata
from pathlib import Path
from typing import Optional

import gspread
import pandas as pd

from config import Settings, get_settings
from whatsapp_bot import WhatsAppLinkBuilder


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


class ActiveSchoolSearchProcessor:
    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or get_settings()
        self.link_builder = WhatsAppLinkBuilder(
            self.settings.whatsapp_message_template,
        )

    def load_absence_report(self, report_path: Optional[Path] = None) -> pd.DataFrame:
        path = Path(report_path or self.settings.consolidated_report_path)
        logger.info("Lendo relatório consolidado: %s", path)

        raw_df = pd.read_excel(path, header=None)
        header_row = self._find_absence_header_row(raw_df)
        df = pd.read_excel(path, header=header_row)
        df = df.rename(columns={"Nome": "student_name", "RA": "ra_raw"})
        df = df.dropna(subset=["student_name", "ra_raw"], how="any")

        day_columns = [
            column
            for column in df.columns
            if isinstance(column, (int, float))
            or (isinstance(column, str) and str(column).strip().isdigit())
        ]

        prepared = pd.DataFrame()
        first_column = df.columns[0] if len(df.columns) > 0 else None
        if "Turma" in df.columns:
            class_series = df["Turma"]
        elif first_column not in {"N°", "Nome", "RA"}:
            class_series = df[first_column]
        else:
            class_series = pd.Series([""] * len(df), index=df.index)
        prepared["class_name"] = class_series.fillna("").astype(str).str.strip()
        prepared["student_name"] = df["student_name"].astype(str).str.strip()
        prepared["ra_raw"] = df["ra_raw"].astype(str).str.strip()
        prepared["ra_base"] = prepared["ra_raw"].apply(self.extract_ra_base)
        prepared["ra_digit"] = prepared["ra_raw"].apply(self.extract_ra_digit)
        prepared["ra_key"] = prepared.apply(
            lambda row: self.build_ra_key(row["ra_base"], row["ra_digit"]),
            axis=1,
        )
        normalized_days = df[day_columns].apply(
            lambda column: column.map(self._absence_cell_to_int),
        )
        prepared["total_absences"] = normalized_days.sum(axis=1).astype(int)
        prepared["absence_days_with_records"] = normalized_days.gt(0).sum(axis=1).astype(int)
        prepared = prepared[prepared["ra_key"].notna()].copy()
        prepared = prepared[prepared["total_absences"] > 0].copy()

        logger.info("Relatório processado com %s aluno(s) com falta.", len(prepared))
        return prepared

    def load_contacts_from_google_sheet(self) -> pd.DataFrame:
        if not self.settings.google_sheet_url:
            raise ValueError("Defina GOOGLE_SHEET_URL no arquivo .env.")

        credentials_file = self.settings.google_service_account_file
        if not credentials_file.exists():
            raise FileNotFoundError(
                f"Arquivo de credenciais não encontrado: {credentials_file}",
            )

        logger.info(
            "Lendo Google Sheets: aba '%s'",
            self.settings.google_sheet_worksheet,
        )
        client = gspread.service_account(filename=str(credentials_file))
        workbook = client.open_by_url(self.settings.google_sheet_url)
        worksheet = workbook.worksheet(self.settings.google_sheet_worksheet)
        records = worksheet.get_all_records()

        if not records:
            raise ValueError("A aba do Google Sheets está vazia.")

        contacts_df = pd.DataFrame(records)
        logger.info("Planilha carregada com %s registro(s).", len(contacts_df))
        return self.prepare_contacts_dataframe(contacts_df)

    def prepare_contacts_dataframe(self, contacts_df: pd.DataFrame) -> pd.DataFrame:
        df = contacts_df.copy()
        renamed_columns = {column: self._normalize_column_name(column) for column in df.columns}
        df = df.rename(columns=renamed_columns)

        ra_column = self._pick_column(df, ["ra"])
        ra_digit_column = self._pick_column(df, ["dig_ra", "digito_ra"])
        parent_name_column = self._pick_column(
            df,
            ["nome_responsavel", "responsavel", "nome_do_responsavel"],
        )
        phone_column = self._pick_column(
            df,
            ["telefone_1", "telefone1", "telefone", "celular", "fone_1"],
        )
        student_name_column = self._pick_column(df, ["nome_aluno", "aluno", "nome"])

        if not ra_column or not phone_column:
            raise KeyError(
                "A planilha precisa ter, no mínimo, as colunas RA e Telefone 1.",
            )

        prepared = pd.DataFrame()
        prepared["ra_base"] = df[ra_column].apply(self.extract_ra_base)
        prepared["ra_digit"] = (
            df[ra_digit_column].astype(str).str.strip().str.upper()
            if ra_digit_column
            else ""
        )
        prepared["ra_key"] = prepared.apply(
            lambda row: self.build_ra_key(row["ra_base"], row["ra_digit"]),
            axis=1,
        )
        prepared["parent_name"] = (
            df[parent_name_column].astype(str).str.strip()
            if parent_name_column
            else "Responsável"
        )
        prepared["contact_student_name"] = (
            df[student_name_column].astype(str).str.strip()
            if student_name_column
            else ""
        )
        prepared["phone_raw"] = df[phone_column].astype(str).str.strip()
        prepared["phone_sanitized"] = prepared["phone_raw"].apply(self.sanitize_phone_number)
        prepared["contact_found"] = True

        prepared = prepared.dropna(subset=["ra_key"]).drop_duplicates(subset=["ra_key"])
        logger.info("Contatos válidos após limpeza: %s", len(prepared))
        return prepared

    def merge_absences_with_contacts(
        self,
        absence_df: pd.DataFrame,
        contacts_df: pd.DataFrame,
    ) -> pd.DataFrame:
        logger.info("Executando merge por RA normalizado.")
        merged = absence_df.merge(
            contacts_df,
            how="left",
            on="ra_key",
            suffixes=("", "_contact"),
        )
        merged["parent_name"] = merged["parent_name"].fillna("Responsável")
        merged["phone_sanitized"] = merged["phone_sanitized"].fillna("")
        merged["contact_status"] = merged.apply(self._build_contact_status, axis=1)
        merged["whatsapp_message"] = merged.apply(
            lambda row: self.link_builder.build_message(
                row["parent_name"],
                row["student_name"],
            ),
            axis=1,
        )
        merged["whatsapp_link"] = merged.apply(
            lambda row: self.link_builder.build_link(
                row["phone_sanitized"],
                row["whatsapp_message"],
            )
            if row["phone_sanitized"]
            else "",
            axis=1,
        )
        return merged

    def export_ready_to_send(
        self,
        merged_df: pd.DataFrame,
        output_path: Optional[Path] = None,
    ) -> Path:
        path = Path(output_path or self.settings.ready_to_send_output_path)
        logger.info("Salvando planilha final: %s", path)
        merged_df.to_excel(path, index=False)
        return path

    def run(self) -> pd.DataFrame:
        absence_df = self.load_absence_report()
        contacts_df = self.load_contacts_from_google_sheet()
        merged_df = self.merge_absences_with_contacts(absence_df, contacts_df)
        self.export_ready_to_send(merged_df)
        logger.info("Pipeline da Fase 2 concluído.")
        return merged_df

    @staticmethod
    def _find_absence_header_row(df: pd.DataFrame) -> int:
        for index, row in df.iterrows():
            values = [str(value).strip().upper() for value in row.tolist() if pd.notna(value)]
            if {"N°", "NOME", "RA"}.issubset(set(values)):
                return index
        raise ValueError("Não foi possível localizar a linha de cabeçalho do relatório.")

    @staticmethod
    def _absence_cell_to_int(value: object) -> int:
        digits = re.sub(r"\D", "", str(value or ""))
        return int(digits) if digits else 0

    @staticmethod
    def extract_ra_base(value: object) -> Optional[str]:
        text = str(value or "").upper()
        match = re.search(r"(\d+)\s*-\s*([\dX])", text)
        if match:
            return match.group(1).lstrip("0") or "0"

        digits = re.sub(r"\D", "", text)
        if not digits:
            return None
        return digits.lstrip("0") or "0"

    @staticmethod
    def extract_ra_digit(value: object) -> str:
        text = str(value or "").upper()
        match = re.search(r"(\d+)\s*-\s*([\dX])", text)
        if match:
            return match.group(2)
        return ""

    @staticmethod
    def build_ra_key(ra_base: Optional[str], ra_digit: object) -> Optional[str]:
        if not ra_base:
            return None
        digit = str(ra_digit or "").strip().upper()
        return f"{ra_base}-{digit}" if digit else ra_base

    def sanitize_phone_number(self, value: object) -> str:
        digits = re.sub(r"\D", "", str(value or ""))
        if not digits:
            return ""

        country_code = self.settings.default_country_code
        default_ddd = self.settings.default_ddd

        if digits.startswith(country_code) and len(digits) in {12, 13}:
            return digits
        if len(digits) in {10, 11}:
            return f"{country_code}{digits}"
        if len(digits) in {8, 9}:
            return f"{country_code}{default_ddd}{digits}"
        if digits.startswith("0") and len(digits[1:]) in {10, 11}:
            return f"{country_code}{digits[1:]}"
        return ""

    @staticmethod
    def _normalize_column_name(value: object) -> str:
        text = str(value or "").strip().lower()
        text = "".join(
            char
            for char in unicodedata.normalize("NFKD", text)
            if not unicodedata.combining(char)
        )
        text = re.sub(r"[^a-z0-9]+", "_", text)
        return text.strip("_")

    @staticmethod
    def _pick_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
        for candidate in candidates:
            if candidate in df.columns:
                return candidate
        return None

    @staticmethod
    def _build_contact_status(row: pd.Series) -> str:
        if pd.isna(row.get("contact_found")):
            return "RA não encontrado na planilha de contatos"
        if not row.get("phone_sanitized"):
            return "Contato encontrado sem telefone válido"
        return "Pronto para envio"


if __name__ == "__main__":
    processor = ActiveSchoolSearchProcessor()
    processor.run()
