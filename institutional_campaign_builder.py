import argparse
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from campaign_builder import CAMPAIGN_COLUMNS
from config import Settings, get_settings
from data_processor import ActiveSchoolSearchProcessor
from institutional_message_catalog import InstitutionalMessageCatalog


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_OUTPUT_DIR = Path("relatorios/campanhas_institucionais")
DEFAULT_LEDGER_PATH = DEFAULT_OUTPUT_DIR / "Institutional_Campaign_Ledger.xlsx"
SEND_STATUS_PENDING = "pendente"
RESPONSE_STATUS_PENDING = "sem_resposta"
DEFAULT_CAMPAIGN_NAME = "provas_bimestrais_abril"
DEFAULT_MESSAGE_CONTEXT = "Aviso institucional sobre provas bimestrais ate 17/04."
GRADE_PRIORITY = {"8": 1, "9": 2, "7": 3, "6": 4}


@dataclass(frozen=True)
class InstitutionalCampaignBuildResult:
    campaign_id: str
    campaign_path: Path
    ledger_path: Path
    included_rows: int


class InstitutionalCampaignBuilder:
    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or get_settings()
        self.processor = ActiveSchoolSearchProcessor(self.settings)
        self.message_catalog = InstitutionalMessageCatalog(school_name=self.settings.school_name)

    def build_campaign(
        self,
        campaign_name: str = DEFAULT_CAMPAIGN_NAME,
        ledger_path: Optional[Path] = None,
        campaign_date: Optional[datetime] = None,
        output_dir: Optional[Path] = None,
    ) -> InstitutionalCampaignBuildResult:
        created_at = campaign_date or datetime.now()
        destination_dir = Path(output_dir or DEFAULT_OUTPUT_DIR)
        history_path = Path(ledger_path or DEFAULT_LEDGER_PATH)
        destination_dir.mkdir(parents=True, exist_ok=True)
        history_path.parent.mkdir(parents=True, exist_ok=True)

        contacts_df = self._load_contacts_raw()
        campaign_id = self._build_campaign_id(campaign_name, created_at, destination_dir)
        campaign_df = self._prepare_campaign_dataframe(contacts_df, campaign_id, created_at)

        campaign_path = destination_dir / f"{campaign_id}.xlsx"
        self._write_campaign_file(campaign_df, campaign_path)
        ledger_df = self._load_or_create_ledger(history_path)
        updated_ledger_df = self._append_campaign_to_ledger(ledger_df, campaign_df)
        self._write_ledger_file(updated_ledger_df, history_path)

        logger.info(
            "Campanha institucional %s criada com %s registro(s).",
            campaign_id,
            len(campaign_df),
        )
        return InstitutionalCampaignBuildResult(
            campaign_id=campaign_id,
            campaign_path=campaign_path,
            ledger_path=history_path,
            included_rows=len(campaign_df),
        )

    def _load_contacts_raw(self) -> pd.DataFrame:
        if not self.settings.google_sheet_url:
            raise ValueError("Defina GOOGLE_SHEET_URL no arquivo .env.")

        credentials_file = self.settings.google_service_account_file
        if not credentials_file.exists():
            raise FileNotFoundError(f"Arquivo de credenciais nao encontrado: {credentials_file}")

        client = self.processor._connect_gspread_with_retry(credentials_file)
        workbook = client.open_by_url(self.settings.google_sheet_url)

        worksheet_setting = self.settings.google_sheet_worksheet.strip()
        if worksheet_setting and worksheet_setting != "*":
            tab_names = [tab.strip() for tab in worksheet_setting.split(",")]
        else:
            tab_names = [worksheet.title for worksheet in workbook.worksheets()]

        all_records: list[pd.DataFrame] = []
        logger.info("Lendo Google Sheets para campanha institucional: abas %s", tab_names)
        for tab in tab_names:
            worksheet = workbook.worksheet(tab)
            records = worksheet.get_all_records()
            if not records:
                continue
            df_tab = pd.DataFrame(records)
            df_tab["_tab"] = tab
            all_records.append(df_tab)
            logger.info("Aba '%s': %s registro(s).", tab, len(df_tab))

        if not all_records:
            raise ValueError("Nenhuma aba do Google Sheets continha dados validos.")

        raw_df = pd.concat(all_records, ignore_index=True)
        prepared_df = self._prepare_primary_and_fallback_contacts(raw_df)
        logger.info("Campanha institucional carregou %s contato(s) validos.", len(prepared_df))
        return prepared_df

    def _prepare_primary_and_fallback_contacts(self, contacts_df: pd.DataFrame) -> pd.DataFrame:
        prepared = self.processor.prepare_contacts_dataframe(contacts_df)
        if prepared.empty:
            raise ValueError("Nenhum contato valido foi encontrado para a campanha institucional.")

        prepared = prepared.rename(columns={"contact_student_name": "student_name"})
        prepared["student_name"] = prepared["student_name"].fillna("").astype(str).str.strip()
        prepared["ra_raw"] = prepared["ra_base"].fillna("").astype(str)
        prepared["class_name"] = ""
        prepared = self._merge_class_names_from_consolidated(prepared)
        prepared["class_name_short"] = prepared["class_name"].apply(self._build_class_name_short)
        prepared = prepared.apply(self._arrange_contact_priority, axis=1)
        prepared["serie_ordem"] = prepared["class_name_short"].apply(self._extract_grade_priority)
        prepared = prepared.sort_values(
            by=["serie_ordem", "class_name_short", "student_name", "contact_slot", "phone_sanitized"],
            kind="stable",
        ).reset_index(drop=True)
        return prepared

    def _merge_class_names_from_consolidated(self, contacts_df: pd.DataFrame) -> pd.DataFrame:
        class_map_df = self._load_class_mapping_from_consolidated()
        if class_map_df.empty:
            return contacts_df

        merged = contacts_df.merge(
            class_map_df,
            on="ra_key",
            how="left",
            suffixes=("", "_consolidated"),
        )
        merged["class_name"] = merged["class_name"].fillna("").astype(str).str.strip()
        merged["class_name_consolidated"] = merged["class_name_consolidated"].fillna("").astype(str).str.strip()
        merged["class_name"] = merged.apply(
            lambda row: row["class_name"] or row["class_name_consolidated"],
            axis=1,
        )
        return merged.drop(columns=["class_name_consolidated"])

    def _load_class_mapping_from_consolidated(self) -> pd.DataFrame:
        path = Path(self.settings.consolidated_report_path)
        if not path.exists():
            logger.warning("Relatorio consolidado nao encontrado em %s. Turmas podem ficar sem identificacao.", path)
            return pd.DataFrame(columns=["ra_key", "class_name_consolidated"])

        raw_df = pd.read_excel(path, header=None)
        header_row = self.processor._find_absence_header_row(raw_df)
        df = pd.read_excel(path, header=header_row)
        df = df.rename(columns={"Nome": "student_name", "RA": "ra_raw"})
        df = df.dropna(subset=["ra_raw"], how="any").copy()

        first_column = df.columns[0] if len(df.columns) > 0 else None
        if "Turma" in df.columns:
            class_series = df["Turma"]
        elif first_column not in {"N°", "NÂ°", "Nome", "RA"}:
            class_series = df[first_column]
        else:
            class_series = pd.Series([""] * len(df), index=df.index)

        mapping_df = pd.DataFrame()
        mapping_df["class_name_consolidated"] = class_series.fillna("").astype(str).str.strip()
        mapping_df["ra_base"] = df["ra_raw"].apply(self.processor.extract_ra_base)
        mapping_df["ra_digit"] = df["ra_raw"].apply(self.processor.extract_ra_digit)
        mapping_df["ra_key"] = mapping_df.apply(
            lambda row: self.processor.build_ra_key(row["ra_base"], row["ra_digit"]),
            axis=1,
        )
        mapping_df = mapping_df.dropna(subset=["ra_key"]).copy()
        mapping_df = mapping_df[mapping_df["class_name_consolidated"].ne("")].copy()
        return mapping_df[["ra_key", "class_name_consolidated"]].drop_duplicates(subset=["ra_key"], keep="first")

    def _prepare_campaign_dataframe(
        self,
        contacts_df: pd.DataFrame,
        campaign_id: str,
        created_at: datetime,
    ) -> pd.DataFrame:
        campaign_df = contacts_df.copy()
        template_details = campaign_df.apply(
            lambda row: self.message_catalog.build_message(
                parent_name=self._safe_text(row["parent_name"]),
                student_name=self._safe_text(row["student_name"]),
                class_name_short=self._safe_text(row["class_name_short"]),
                campaign_id=campaign_id,
                unique_key=f"{row['ra_key']}|{row['phone_sanitized']}|{row['contact_slot']}",
            ),
            axis=1,
        )
        campaign_df["message_template_id"] = template_details.apply(lambda value: value[0])
        campaign_df["whatsapp_message"] = template_details.apply(lambda value: value[1])
        campaign_df["absence_days"] = ""
        campaign_df.insert(
            0,
            "campaign_row_id",
            campaign_df.apply(
                lambda row: (
                    f"{campaign_id}|{self._safe_text(row['ra_key'])}|"
                    f"{self._safe_text(row['phone_sanitized'])}|{self._safe_text(row['contact_slot'])}"
                ),
                axis=1,
            ),
        )
        campaign_df.insert(0, "observacao", DEFAULT_MESSAGE_CONTEXT)
        campaign_df.insert(0, "status_resposta", RESPONSE_STATUS_PENDING)
        campaign_df.insert(0, "data_envio", "")
        campaign_df.insert(0, "status_envio", SEND_STATUS_PENDING)
        campaign_df.insert(0, "data_criacao", created_at.strftime("%Y-%m-%d %H:%M:%S"))
        campaign_df.insert(0, "campaign_id", campaign_id)
        for column in CAMPAIGN_COLUMNS:
            if column not in campaign_df.columns:
                campaign_df[column] = ""
        return campaign_df[CAMPAIGN_COLUMNS]

    def _load_or_create_ledger(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            empty_df = pd.DataFrame(columns=CAMPAIGN_COLUMNS)
            self._write_ledger_file(empty_df, path)
            return empty_df

        ledger_df = pd.read_excel(path, sheet_name="Historico")
        for column in CAMPAIGN_COLUMNS:
            if column not in ledger_df.columns:
                ledger_df[column] = ""
        return ledger_df[CAMPAIGN_COLUMNS].copy()

    def _append_campaign_to_ledger(self, ledger_df: pd.DataFrame, campaign_df: pd.DataFrame) -> pd.DataFrame:
        combined = pd.concat([ledger_df, campaign_df], ignore_index=True)
        dedup_keys = ["campaign_row_id"] if "campaign_row_id" in combined.columns else ["campaign_id", "ra_key", "phone_sanitized", "contact_slot"]
        combined = combined.drop_duplicates(subset=dedup_keys, keep="first")
        return combined[CAMPAIGN_COLUMNS].copy()

    def _write_campaign_file(self, df: pd.DataFrame, path: Path) -> None:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Campanha", index=False)

    def _write_ledger_file(self, df: pd.DataFrame, path: Path) -> None:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Historico", index=False)

    def _build_campaign_id(self, campaign_name: str, created_at: datetime, destination_dir: Path) -> str:
        base_name = self._sanitize_campaign_name(campaign_name)
        stem = f"Campanha_Institucional_{base_name}_{created_at:%Y_%m_%d}"
        candidate = stem
        sequence = 1
        while (destination_dir / f"{candidate}.xlsx").exists():
            candidate = f"{stem}_{sequence:02d}"
            sequence += 1
        return candidate

    @staticmethod
    def _sanitize_campaign_name(value: str) -> str:
        text = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip())
        text = text.strip("_")
        return text or "Campanha"

    @staticmethod
    def _slot_label_from_columns(name_column: str, phone_column: str) -> str:
        joined = f"{name_column}_{phone_column}"
        match = re.search(r"_(\d)$", joined)
        if match:
            return f"responsavel_{match.group(1)}"
        return "responsavel_1"

    @staticmethod
    def _arrange_contact_priority(row: pd.Series) -> pd.Series:
        contacts: list[tuple[str, str, str]] = []
        for index in range(1, 4):
            phone = InstitutionalCampaignBuilder._safe_text(row.get(f"phone_sanitized_{index}"))
            if not phone:
                continue
            parent = InstitutionalCampaignBuilder._safe_text(row.get(f"parent_name_{index}")) or "Responsavel"
            contacts.append((parent, phone, f"responsavel_{index}"))

        for target_index in range(3):
            if target_index < len(contacts):
                parent, phone, slot = contacts[target_index]
            else:
                parent, phone, slot = ("", "", "")
            if target_index == 0:
                row["parent_name"] = parent or "Responsavel"
                row["phone_sanitized"] = phone
                row["contact_slot"] = slot or "responsavel_1"
            else:
                suffix = target_index + 1
                row[f"parent_name_{suffix}"] = parent
                row[f"phone_sanitized_{suffix}"] = phone
                row[f"contact_slot_{suffix}"] = slot
        return row

    @staticmethod
    def _build_class_name_short(class_name: str) -> str:
        text = InstitutionalCampaignBuilder._safe_text(class_name).upper()
        if not text:
            return ""

        grade_match = re.search(r"\b([6-9])\s*ANO\b", text)
        slot_match = re.search(r"\b[6-9]\s*([A-Z])\b", text)
        if slot_match is None:
            slot_match = re.search(r"\b([A-Z])\b", text)

        grade = grade_match.group(1) if grade_match else ""
        slot = slot_match.group(1) if slot_match else ""

        if grade and slot:
            return f"{grade} ANO {slot}"
        if grade:
            return f"{grade} ANO"
        return text

    @staticmethod
    def _extract_grade_priority(class_name_short: str) -> int:
        match = re.search(r"\b([6-9])\s*ANO\b", InstitutionalCampaignBuilder._safe_text(class_name_short).upper())
        if not match:
            return 99
        return GRADE_PRIORITY.get(match.group(1), 99)

    @staticmethod
    def _safe_text(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and pd.isna(value):
            return ""
        text = str(value).strip()
        return "" if text.lower() == "nan" else text


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera uma campanha institucional separada para recados gerais.",
    )
    parser.add_argument(
        "--campaign-name",
        default=DEFAULT_CAMPAIGN_NAME,
        help="Nome logico da campanha institucional.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Pasta de saida da campanha institucional.",
    )
    parser.add_argument(
        "--ledger-path",
        default=str(DEFAULT_LEDGER_PATH),
        help="Caminho do ledger institucional.",
    )
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    builder = InstitutionalCampaignBuilder()
    result = builder.build_campaign(
        campaign_name=args.campaign_name,
        output_dir=Path(args.output_dir),
        ledger_path=Path(args.ledger_path),
    )
    logger.info(
        "Campanha institucional finalizada: %s | registros=%s | ledger=%s",
        result.campaign_path,
        result.included_rows,
        result.ledger_path,
    )


if __name__ == "__main__":
    main()
