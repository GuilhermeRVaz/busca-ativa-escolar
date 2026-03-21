import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from campaign_builder import CAMPAIGN_COLUMNS
from config import Settings, get_settings
from data_processor import ActiveSchoolSearchProcessor
from message_catalog import MessageCatalog


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_DAILY_LEDGER = Path("relatorios/Daily_Campaign_Ledger.xlsx")
RESPONSE_STATUS_RESPONDED = "respondido"


@dataclass(frozen=True)
class DailyCampaignBuildResult:
    campaign_id: str
    campaign_path: Path
    ledger_path: Path
    target_day: int
    included_rows: int
    excluded_rows: int


class DailyCampaignBuilder:
    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or get_settings()
        self.processor = ActiveSchoolSearchProcessor(self.settings)
        self.message_catalog = MessageCatalog()

    def build_campaign(
        self,
        target_day: Optional[int] = None,
        mode: str = "last-available",
        report_path: Optional[Path] = None,
        ledger_path: Optional[Path] = None,
        output_dir: Optional[Path] = None,
    ) -> DailyCampaignBuildResult:
        consolidated_path = Path(report_path or self.settings.consolidated_report_path)
        destination_dir = Path(output_dir or consolidated_path.parent)
        destination_dir.mkdir(parents=True, exist_ok=True)
        daily_ledger_path = Path(ledger_path or DEFAULT_DAILY_LEDGER)

        absence_df, resolved_day = self._load_daily_absences(
            report_path=consolidated_path,
            target_day=target_day,
            mode=mode,
        )
        ledger_df = self._load_or_create_ledger(daily_ledger_path)

        campaign_date = datetime.now()
        campaign_id = self._build_campaign_id(campaign_date, resolved_day, destination_dir)
        if absence_df.empty:
            logger.info("Nenhum aluno com falta no dia %s. Gerando campanha diaria vazia.", resolved_day)
            campaign_df = self._empty_campaign_dataframe(campaign_id, campaign_date)
            filtered_df = absence_df
            merged_df = absence_df
        else:
            contacts_df = self.processor.load_contacts_from_google_sheet()
            merged_df = self.processor.merge_absences_with_contacts(absence_df, contacts_df)
            merged_df["absence_days"] = str(resolved_day)
            filtered_df = self._exclude_responded_students(merged_df, ledger_df)
            campaign_df = self._prepare_campaign_dataframe(
                filtered_df,
                campaign_id,
                campaign_date,
                resolved_day,
            )
        campaign_path = destination_dir / f"{campaign_id}.xlsx"

        self._write_campaign_file(campaign_df, campaign_path)
        updated_ledger_df = self._append_campaign_to_ledger(ledger_df, campaign_df)
        self._write_ledger_file(updated_ledger_df, daily_ledger_path)

        logger.info(
            "Campanha diaria %s criada com %s registro(s) para o dia %s.",
            campaign_id,
            len(campaign_df),
            resolved_day,
        )

        return DailyCampaignBuildResult(
            campaign_id=campaign_id,
            campaign_path=campaign_path,
            ledger_path=daily_ledger_path,
            target_day=resolved_day,
            included_rows=len(campaign_df),
            excluded_rows=len(merged_df) - len(filtered_df),
        )

    def _load_daily_absences(
        self,
        report_path: Path,
        target_day: Optional[int],
        mode: str,
    ) -> tuple[pd.DataFrame, int]:
        if not report_path.exists():
            raise FileNotFoundError(f"Relatorio consolidado nao encontrado: {report_path}")

        logger.info("Lendo relatorio consolidado para campanha diaria: %s", report_path)
        raw_df = pd.read_excel(report_path, header=None)
        header_row = self.processor._find_absence_header_row(raw_df)
        df = pd.read_excel(report_path, header=header_row)
        df = df.rename(columns={"Nome": "student_name", "RA": "ra_raw"})
        df = df.dropna(subset=["student_name", "ra_raw"], how="any")

        day_columns = [
            column
            for column in df.columns
            if isinstance(column, (int, float))
            or (isinstance(column, str) and str(column).strip().isdigit())
        ]
        if not day_columns:
            raise ValueError("Nao foi possivel identificar colunas de dias no relatorio consolidado.")

        resolved_day = self._resolve_target_day(day_columns=day_columns, target_day=target_day, mode=mode)
        logger.info("Dia alvo da campanha diaria: %s", resolved_day)

        normalized_days = df[day_columns].apply(
            lambda column: column.map(self.processor._absence_cell_to_int),
        )
        target_series = normalized_days[self._find_day_column(day_columns, resolved_day)]

        prepared = pd.DataFrame()
        first_column = df.columns[0] if len(df.columns) > 0 else None
        if "Turma" in df.columns:
            class_series = df["Turma"]
        elif first_column not in {"N°", "NOME", "Nome", "RA"}:
            class_series = df[first_column]
        else:
            class_series = pd.Series([""] * len(df), index=df.index)

        prepared["class_name"] = class_series.fillna("").astype(str).str.strip()
        prepared["student_name"] = df["student_name"].astype(str).str.strip()
        prepared["ra_raw"] = df["ra_raw"].astype(str).str.strip()
        prepared["ra_base"] = prepared["ra_raw"].apply(self.processor.extract_ra_base)
        prepared["ra_digit"] = prepared["ra_raw"].apply(self.processor.extract_ra_digit)
        prepared["ra_key"] = prepared.apply(
            lambda row: self.processor.build_ra_key(row["ra_base"], row["ra_digit"]),
            axis=1,
        )
        prepared["total_absences"] = normalized_days.sum(axis=1).astype(int)
        prepared["absence_days_with_records"] = normalized_days.gt(0).sum(axis=1).astype(int)
        prepared["absence_days"] = str(resolved_day)
        prepared["target_day_absence_count"] = target_series.astype(int)

        prepared = prepared[prepared["ra_key"].notna()].copy()
        prepared = prepared[prepared["target_day_absence_count"] > 0].copy()
        logger.info("Alunos com falta no dia %s: %s", resolved_day, len(prepared))
        return prepared, resolved_day

    def _resolve_target_day(
        self,
        day_columns: list[object],
        target_day: Optional[int],
        mode: str,
    ) -> int:
        numeric_days = sorted({int(str(column).strip()) for column in day_columns})
        if target_day is not None:
            if target_day not in numeric_days:
                raise ValueError(f"Dia {target_day} nao encontrado no relatorio consolidado.")
            return target_day

        today = datetime.now().day
        if mode == "today":
            if today not in numeric_days:
                raise ValueError(f"O dia atual {today} nao existe no relatorio consolidado.")
            return today
        if mode == "yesterday":
            yesterday = today - 1
            if yesterday not in numeric_days:
                raise ValueError(f"O dia anterior {yesterday} nao existe no relatorio consolidado.")
            return yesterday
        return max(numeric_days)

    @staticmethod
    def _find_day_column(day_columns: list[object], target_day: int) -> object:
        for column in day_columns:
            if int(str(column).strip()) == target_day:
                return column
        raise KeyError(f"Coluna do dia {target_day} nao encontrada.")

    def _load_or_create_ledger(self, path: Path) -> pd.DataFrame:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            logger.info("Daily ledger nao encontrado. Criando novo em %s", path)
            empty_df = pd.DataFrame(columns=CAMPAIGN_COLUMNS)
            self._write_ledger_file(empty_df, path)
            return empty_df
        ledger_df = pd.read_excel(path, sheet_name="Historico")
        for column in CAMPAIGN_COLUMNS:
            if column not in ledger_df.columns:
                ledger_df[column] = ""
        return ledger_df[CAMPAIGN_COLUMNS].copy()

    def _exclude_responded_students(self, merged_df: pd.DataFrame, ledger_df: pd.DataFrame) -> pd.DataFrame:
        if ledger_df.empty:
            return merged_df.copy()
        responded_students = set(
            ledger_df.loc[
                ledger_df["status_resposta"].fillna("").astype(str).str.strip().str.lower().eq(RESPONSE_STATUS_RESPONDED),
                "ra_key",
            ]
            .fillna("")
            .astype(str)
            .str.strip(),
        )
        if not responded_students:
            return merged_df.copy()
        return merged_df.loc[~merged_df["ra_key"].isin(responded_students)].copy()

    def _prepare_campaign_dataframe(
        self,
        merged_df: pd.DataFrame,
        campaign_id: str,
        created_at: datetime,
        resolved_day: int,
    ) -> pd.DataFrame:
        campaign_df = merged_df[
            [
                "class_name",
                "student_name",
                "ra_raw",
                "ra_key",
                "parent_name",
                "phone_sanitized",
                "absence_days",
                "contact_slot",
            ]
        ].copy()
        campaign_df = campaign_df.drop_duplicates(
            subset=["ra_key", "phone_sanitized", "contact_slot"],
            keep="first",
        )

        template_details = campaign_df.apply(
            lambda row: self.message_catalog.build_message(
                parent_name=str(row["parent_name"]),
                student_name=str(row["student_name"]),
                absence_days=str(row["absence_days"]),
                campaign_id=campaign_id,
                unique_key=f"{row['ra_key']}|{row['phone_sanitized']}|{row['contact_slot']}",
            ),
            axis=1,
        )
        campaign_df["message_template_id"] = template_details.apply(lambda value: value[0])
        campaign_df["whatsapp_message"] = template_details.apply(lambda value: value[1])

        campaign_df.insert(0, "observacao", f"Campanha diaria referente ao dia {resolved_day}.")
        campaign_df.insert(0, "status_resposta", "sem_resposta")
        campaign_df.insert(0, "data_envio", "")
        campaign_df.insert(0, "status_envio", "pendente")
        campaign_df.insert(0, "data_criacao", created_at.strftime("%Y-%m-%d %H:%M:%S"))
        campaign_df.insert(0, "campaign_id", campaign_id)
        return campaign_df[CAMPAIGN_COLUMNS]

    @staticmethod
    def _empty_campaign_dataframe(campaign_id: str, created_at: datetime) -> pd.DataFrame:
        empty_df = pd.DataFrame(columns=CAMPAIGN_COLUMNS)
        if empty_df.empty:
            return empty_df
        empty_df["campaign_id"] = campaign_id
        empty_df["data_criacao"] = created_at.strftime("%Y-%m-%d %H:%M:%S")
        return empty_df

    @staticmethod
    def _write_campaign_file(df: pd.DataFrame, path: Path) -> None:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Campanha", index=False)

    @staticmethod
    def _append_campaign_to_ledger(ledger_df: pd.DataFrame, campaign_df: pd.DataFrame) -> pd.DataFrame:
        combined = pd.concat([ledger_df, campaign_df], ignore_index=True)
        dedup_keys = ["campaign_id", "ra_key", "phone_sanitized", "contact_slot"]
        combined = combined.drop_duplicates(subset=dedup_keys, keep="first")
        return combined[CAMPAIGN_COLUMNS].copy()

    @staticmethod
    def _write_ledger_file(df: pd.DataFrame, path: Path) -> None:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Historico", index=False)

    @staticmethod
    def _build_campaign_id(created_at: datetime, resolved_day: int, output_dir: Path) -> str:
        base_id = f"Campanha_Diaria_{created_at:%Y_%m_%d}_dia_{resolved_day:02d}"
        candidate = base_id
        index = 1
        while (output_dir / f"{candidate}.xlsx").exists():
            candidate = f"{base_id}_{index:02d}"
            index += 1
        return candidate


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gera campanha diaria a partir do relatorio consolidado e da planilha de contatos.",
    )
    parser.add_argument(
        "--day",
        type=int,
        help="Dia do mes para buscar faltas especificas.",
    )
    parser.add_argument(
        "--mode",
        choices=["last-available", "yesterday", "today"],
        default="last-available",
        help="Modo automatico para resolver o dia alvo. Padrao: last-available",
    )
    parser.add_argument(
        "--report",
        help="Caminho do relatorio consolidado.",
    )
    parser.add_argument(
        "--ledger",
        help="Caminho do daily ledger.",
    )
    parser.add_argument(
        "--output-dir",
        help="Pasta de saida da campanha diaria.",
    )
    args = parser.parse_args()

    builder = DailyCampaignBuilder()
    try:
        result = builder.build_campaign(
            target_day=args.day,
            mode=args.mode,
            report_path=Path(args.report) if args.report else None,
            ledger_path=Path(args.ledger) if args.ledger else None,
            output_dir=Path(args.output_dir) if args.output_dir else None,
        )
    except Exception as exc:
        logger.exception("Falha ao construir campanha diaria: %s", exc)
        raise SystemExit(1) from exc

    logger.info(
        "Campanha diaria finalizada: %s | dia=%s | incluidos=%s | excluidos=%s | ledger=%s",
        result.campaign_path,
        result.target_day,
        result.included_rows,
        result.excluded_rows,
        result.ledger_path,
    )


if __name__ == "__main__":
    main()
