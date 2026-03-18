import argparse
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from config import Settings, get_settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


CAMPAIGN_COLUMNS = [
    "campaign_id",
    "data_criacao",
    "status_envio",
    "data_envio",
    "status_resposta",
    "observacao",
    "class_name",
    "student_name",
    "ra_raw",
    "ra_key",
    "parent_name",
    "phone_sanitized",
    "absence_days",
    "whatsapp_message",
    "contact_slot",
]

RESPONSE_STATUS_RESPONDED = "respondido"
SEND_STATUS_PENDING = "pendente"
RESPONSE_STATUS_PENDING = "sem_resposta"


@dataclass(frozen=True)
class CampaignBuildResult:
    campaign_id: str
    campaign_path: Path
    ledger_path: Path
    included_rows: int
    excluded_rows: int


class CampaignBuilder:
    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or get_settings()

    def build_campaign(
        self,
        ready_to_send_path: Optional[Path] = None,
        ledger_path: Optional[Path] = None,
        campaign_date: Optional[datetime] = None,
        output_dir: Optional[Path] = None,
    ) -> CampaignBuildResult:
        ready_path = Path(ready_to_send_path or self.settings.ready_to_send_output_path)
        history_path = Path(ledger_path or self.settings.campaign_ledger_path)
        created_at = campaign_date or datetime.now()
        destination_dir = Path(output_dir or ready_path.parent)
        destination_dir.mkdir(parents=True, exist_ok=True)

        ready_df = self._load_ready_to_send(ready_path)
        ledger_df = self._load_or_create_ledger(history_path)

        eligible_df = self._filter_valid_contacts(ready_df)
        filtered_df = self._exclude_responded_students(eligible_df, ledger_df)
        campaign_id = self._build_campaign_id(created_at, destination_dir)
        campaign_df = self._prepare_campaign_dataframe(filtered_df, campaign_id, created_at)

        campaign_path = destination_dir / f"{campaign_id}.xlsx"
        self._write_campaign_file(campaign_df, campaign_path)
        updated_ledger_df = self._append_campaign_to_ledger(ledger_df, campaign_df)
        self._write_ledger_file(updated_ledger_df, history_path)

        logger.info(
            "Campanha %s criada com %s registro(s); %s registro(s) excluido(s) por historico.",
            campaign_id,
            len(campaign_df),
            len(eligible_df) - len(filtered_df),
        )

        return CampaignBuildResult(
            campaign_id=campaign_id,
            campaign_path=campaign_path,
            ledger_path=history_path,
            included_rows=len(campaign_df),
            excluded_rows=len(eligible_df) - len(filtered_df),
        )

    def _load_ready_to_send(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"Arquivo Ready_To_Send nao encontrado: {path}")

        logger.info("Lendo arquivo Ready_To_Send: %s", path)
        df = pd.read_excel(path, sheet_name="Todos")
        required_columns = {
            "class_name",
            "student_name",
            "ra_raw",
            "ra_key",
            "parent_name",
            "phone_sanitized",
            "absence_days",
            "whatsapp_message",
            "contact_slot",
        }
        missing_columns = sorted(required_columns.difference(df.columns))
        if missing_columns:
            raise KeyError(
                "Ready_To_Send sem colunas obrigatorias: "
                + ", ".join(missing_columns),
            )
        return df.copy()

    def _load_or_create_ledger(self, path: Path) -> pd.DataFrame:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            logger.info("Ledger nao encontrado. Criando arquivo novo em %s", path)
            empty_df = pd.DataFrame(columns=CAMPAIGN_COLUMNS)
            self._write_ledger_file(empty_df, path)
            return empty_df

        logger.info("Lendo ledger historico: %s", path)
        ledger_df = pd.read_excel(path, sheet_name="Historico")
        for column in CAMPAIGN_COLUMNS:
            if column not in ledger_df.columns:
                ledger_df[column] = ""
        return ledger_df[CAMPAIGN_COLUMNS].copy()

    def _filter_valid_contacts(self, df: pd.DataFrame) -> pd.DataFrame:
        prepared = df.copy()
        prepared["phone_sanitized"] = prepared["phone_sanitized"].apply(self._normalize_phone)
        prepared["ra_key"] = prepared["ra_key"].fillna("").astype(str).str.strip()
        prepared = prepared[prepared["phone_sanitized"].ne("")].copy()
        prepared = prepared[prepared["ra_key"].ne("")].copy()
        return prepared

    def _exclude_responded_students(
        self,
        ready_df: pd.DataFrame,
        ledger_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if ledger_df.empty:
            return ready_df.copy()

        responded_students = set(
            ledger_df.loc[
                ledger_df["status_resposta"].apply(self._normalize_status).eq(
                    RESPONSE_STATUS_RESPONDED,
                ),
                "ra_key",
            ]
            .fillna("")
            .astype(str)
            .str.strip(),
        )
        if not responded_students:
            return ready_df.copy()

        return ready_df.loc[~ready_df["ra_key"].isin(responded_students)].copy()

    def _prepare_campaign_dataframe(
        self,
        df: pd.DataFrame,
        campaign_id: str,
        created_at: datetime,
    ) -> pd.DataFrame:
        campaign_df = df[
            [
                "class_name",
                "student_name",
                "ra_raw",
                "ra_key",
                "parent_name",
                "phone_sanitized",
                "absence_days",
                "whatsapp_message",
                "contact_slot",
            ]
        ].copy()
        before_dedup = len(campaign_df)
        campaign_df = campaign_df.drop_duplicates(
            subset=["ra_key", "phone_sanitized", "contact_slot"],
            keep="first",
        )
        duplicate_count = before_dedup - len(campaign_df)
        if duplicate_count:
            logger.warning(
                "Removendo %s registro(s) duplicado(s) na montagem da campanha.",
                duplicate_count,
            )
        campaign_df.insert(0, "observacao", "")
        campaign_df.insert(0, "status_resposta", RESPONSE_STATUS_PENDING)
        campaign_df.insert(0, "data_envio", "")
        campaign_df.insert(0, "status_envio", SEND_STATUS_PENDING)
        campaign_df.insert(0, "data_criacao", created_at.strftime("%Y-%m-%d %H:%M:%S"))
        campaign_df.insert(0, "campaign_id", campaign_id)
        return campaign_df[CAMPAIGN_COLUMNS]

    def _write_campaign_file(self, df: pd.DataFrame, path: Path) -> None:
        logger.info("Salvando campanha semanal em %s", path)
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Campanha", index=False)

    def _append_campaign_to_ledger(
        self,
        ledger_df: pd.DataFrame,
        campaign_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if campaign_df.empty:
            return ledger_df.copy()

        combined = pd.concat([ledger_df, campaign_df], ignore_index=True)
        dedup_keys = ["campaign_id", "ra_key", "phone_sanitized", "contact_slot"]
        combined = combined.drop_duplicates(subset=dedup_keys, keep="first")
        return combined[CAMPAIGN_COLUMNS].copy()

    def _write_ledger_file(self, df: pd.DataFrame, path: Path) -> None:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Historico", index=False)

    @staticmethod
    def _normalize_phone(value: object) -> str:
        if pd.isna(value):
            return ""

        if isinstance(value, float) and value.is_integer():
            raw_value = str(int(value))
        else:
            raw_value = str(value).strip()
            if raw_value.endswith(".0"):
                raw_value = raw_value[:-2]

        digits = re.sub(r"\D", "", raw_value)
        if len(digits) not in {12, 13}:
            return ""
        return digits

    @staticmethod
    def _normalize_status(value: object) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _build_campaign_id(created_at: datetime, output_dir: Path) -> str:
        base_id = f"Campanha_{created_at:%Y_%m_%d}"
        candidate = base_id
        index = 1
        while (output_dir / f"{candidate}.xlsx").exists():
            candidate = f"{base_id}_{index:02d}"
            index += 1
        return candidate


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Constroi campanha semanal a partir da Ready_To_Send e do Campaign_Ledger.",
    )
    parser.add_argument(
        "--ready-to-send",
        dest="ready_to_send_path",
        help="Caminho do arquivo Ready_To_Send.xlsx.",
    )
    parser.add_argument(
        "--ledger",
        dest="ledger_path",
        help="Caminho do arquivo Campaign_Ledger.xlsx.",
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        help="Pasta onde o arquivo da campanha sera salvo.",
    )
    args = parser.parse_args()

    builder = CampaignBuilder()
    try:
        result = builder.build_campaign(
            ready_to_send_path=Path(args.ready_to_send_path) if args.ready_to_send_path else None,
            ledger_path=Path(args.ledger_path) if args.ledger_path else None,
            output_dir=Path(args.output_dir) if args.output_dir else None,
        )
    except Exception as exc:
        logger.exception("Falha ao construir campanha: %s", exc)
        raise SystemExit(1) from exc

    logger.info(
        "Campanha finalizada: %s | incluidos=%s | excluidos=%s | ledger=%s",
        result.campaign_path,
        result.included_rows,
        result.excluded_rows,
        result.ledger_path,
    )


if __name__ == "__main__":
    main()
