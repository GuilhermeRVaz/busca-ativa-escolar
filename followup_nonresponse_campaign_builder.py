import argparse
import logging
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from campaign_builder import CAMPAIGN_COLUMNS
from config import Settings, get_settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_INPUT_PATH = Path("relatorios/Recontato_Sem_Resposta.csv")
DEFAULT_LEDGER_PATH = Path("relatorios/Followup_NonResponse_Ledger.xlsx")
FOLLOWUP_TEMPLATE_ID = "followup_nonresponse_v1"


@dataclass(frozen=True)
class FollowupCampaignBuildResult:
    campaign_id: str
    campaign_path: Path
    ledger_path: Path
    included_rows: int


class FollowupNonResponseCampaignBuilder:
    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or get_settings()

    def build_campaign(
        self,
        input_path: Optional[Path] = None,
        output_dir: Optional[Path] = None,
        ledger_path: Optional[Path] = None,
        campaign_date: Optional[datetime] = None,
    ) -> FollowupCampaignBuildResult:
        source_path = Path(input_path or DEFAULT_INPUT_PATH)
        destination_dir = Path(output_dir or "relatorios")
        destination_dir.mkdir(parents=True, exist_ok=True)
        followup_ledger_path = Path(ledger_path or DEFAULT_LEDGER_PATH)
        created_at = campaign_date or datetime.now()

        request_df = self._load_request(source_path)
        campaign_df = self._build_from_requests(request_df, created_at, destination_dir)

        campaign_path = destination_dir / f"{campaign_df.iloc[0]['campaign_id']}.xlsx"
        self._write_campaign_file(campaign_df, campaign_path)

        ledger_df = self._load_or_create_ledger(followup_ledger_path)
        updated_ledger_df = self._append_campaign_to_ledger(ledger_df, campaign_df)
        self._write_ledger_file(updated_ledger_df, followup_ledger_path)

        logger.info(
            "Campanha de recontato %s criada com %s registro(s).",
            campaign_df.iloc[0]["campaign_id"],
            len(campaign_df),
        )

        return FollowupCampaignBuildResult(
            campaign_id=str(campaign_df.iloc[0]["campaign_id"]),
            campaign_path=campaign_path,
            ledger_path=followup_ledger_path,
            included_rows=len(campaign_df),
        )

    def _load_request(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"Lista de alunos sem resposta nao encontrada: {path}")

        if path.suffix.lower() == ".csv":
            request_df = pd.read_csv(path, dtype=str)
        else:
            request_df = pd.read_excel(path, dtype=str)

        required_columns = {"absence_day", "student_name"}
        missing_columns = sorted(required_columns.difference(request_df.columns))
        if missing_columns:
            raise KeyError("Lista sem colunas obrigatorias: " + ", ".join(missing_columns))

        request_df = request_df.copy()
        request_df["absence_day"] = request_df["absence_day"].astype(int)
        request_df["student_name"] = request_df["student_name"].fillna("").astype(str).str.strip()
        request_df["class_name"] = request_df.get("class_name", "").fillna("").astype(str).str.strip()
        request_df["followup_reason"] = request_df.get("followup_reason", "").fillna("").astype(str).str.strip()
        request_df = request_df[request_df["student_name"].ne("")].copy()
        if request_df.empty:
            raise ValueError("A lista de recontato nao possui alunos validos.")
        return request_df

    def _build_from_requests(
        self,
        request_df: pd.DataFrame,
        created_at: datetime,
        output_dir: Path,
    ) -> pd.DataFrame:
        campaign_id = self._build_campaign_id(created_at, output_dir)
        rows: list[dict[str, object]] = []
        missing_students: list[str] = []

        for _, request_row in request_df.iterrows():
            source_row = self._resolve_source_row(
                absence_day=int(request_row["absence_day"]),
                student_name=str(request_row["student_name"]),
                class_name=str(request_row.get("class_name", "")),
            )
            if source_row is None:
                missing_students.append(
                    f"{request_row['student_name']} (dia {request_row['absence_day']})",
                )
                continue

            rows.append(
                self._build_campaign_row(
                    source_row=source_row,
                    request_row=request_row,
                    campaign_id=campaign_id,
                    created_at=created_at,
                ),
            )

        if missing_students:
            raise ValueError(
                "Nao foi possivel localizar contato para: " + "; ".join(missing_students),
            )
        if not rows:
            raise ValueError("Nenhum registro elegivel foi encontrado para a campanha de recontato.")

        campaign_df = pd.DataFrame(rows)
        for column in CAMPAIGN_COLUMNS:
            if column not in campaign_df.columns:
                campaign_df[column] = ""
        return campaign_df[CAMPAIGN_COLUMNS]

    def _resolve_source_row(self, absence_day: int, student_name: str, class_name: str) -> Optional[pd.Series]:
        candidates = sorted(
            Path("relatorios").glob(f"Campanha_Diaria_*_dia_{absence_day:02d}*.xlsx"),
            reverse=True,
        )
        normalized_target = self._normalize_text(student_name)
        normalized_class = self._normalize_text(class_name)

        best_row: Optional[pd.Series] = None
        for path in candidates:
            try:
                df = pd.read_excel(path, sheet_name="Campanha", dtype=str)
            except Exception:
                df = pd.read_excel(path, dtype=str)
            if "student_name" not in df.columns:
                continue

            prepared = df.copy()
            prepared["student_name_norm"] = prepared["student_name"].fillna("").astype(str).map(self._normalize_text)
            matched = prepared.loc[prepared["student_name_norm"].eq(normalized_target)].copy()
            if normalized_class and "class_name" in matched.columns:
                matched["class_name_norm"] = matched["class_name"].fillna("").astype(str).map(self._normalize_text)
                class_filtered = matched.loc[matched["class_name_norm"].str.contains(normalized_class, regex=False)]
                if not class_filtered.empty:
                    matched = class_filtered
            if matched.empty:
                continue

            matched = matched.assign(
                primary_phone=matched.get("phone_sanitized", "").map(self._normalize_phone),
            )
            matched = matched.loc[matched["primary_phone"].ne("")].copy()
            if matched.empty:
                continue

            best_row = matched.iloc[0]
            break

        return best_row

    def _build_campaign_row(
        self,
        source_row: pd.Series,
        request_row: pd.Series,
        campaign_id: str,
        created_at: datetime,
    ) -> dict[str, object]:
        prioritized_contacts = self._prioritize_followup_contacts(source_row)
        class_name = str(source_row.get("class_name", "") or "").strip()
        student_name = str(source_row.get("student_name", "") or "").strip()
        ra_raw = str(source_row.get("ra_raw", "") or "").strip()
        ra_key = str(source_row.get("ra_key", "") or "").strip()
        parent_name = prioritized_contacts[0]["parent_name"]
        phone_sanitized = prioritized_contacts[0]["phone_sanitized"]
        absence_day = str(int(request_row["absence_day"]))
        followup_reason = str(request_row.get("followup_reason", "") or "").strip()

        row = {
            "campaign_row_id": f"{campaign_id}|{ra_key}",
            "campaign_id": campaign_id,
            "data_criacao": created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "status_envio": "pendente",
            "data_envio": "",
            "status_resposta": "sem_resposta",
            "observacao": self._build_observation(absence_day, followup_reason),
            "class_name": class_name,
            "student_name": student_name,
            "ra_raw": ra_raw,
            "ra_key": ra_key,
            "parent_name": parent_name,
            "phone_sanitized": phone_sanitized,
            "absence_days": absence_day,
            "message_template_id": FOLLOWUP_TEMPLATE_ID,
            "whatsapp_message": self._build_followup_message(
                parent_name=parent_name,
                student_name=student_name,
                class_name=class_name,
                absence_day=absence_day,
            ),
            "contact_slot": prioritized_contacts[0]["contact_slot"],
            "parent_name_2": prioritized_contacts[1]["parent_name"],
            "phone_sanitized_2": prioritized_contacts[1]["phone_sanitized"],
            "contact_slot_2": prioritized_contacts[1]["contact_slot"],
            "parent_name_3": prioritized_contacts[2]["parent_name"],
            "phone_sanitized_3": prioritized_contacts[2]["phone_sanitized"],
            "contact_slot_3": prioritized_contacts[2]["contact_slot"],
        }
        return row

    def _build_followup_message(
        self,
        parent_name: str,
        student_name: str,
        class_name: str,
        absence_day: str,
    ) -> str:
        return (
            f"Ola {parent_name}, aqui e da {self.settings.school_name}. "
            f"Entramos em contato anteriormente sobre a ausencia de {student_name}, "
            f"da turma {self._normalize_class_name_short(class_name)}, no dia {absence_day}, "
            "e ainda nao tivemos um retorno efetivo. "
            "Poderia nos informar o motivo da falta, por favor?"
        )

    @staticmethod
    def _build_observation(absence_day: str, followup_reason: str) -> str:
        base = f"Recontato por ausencia do dia {absence_day} sem resposta efetiva."
        if followup_reason:
            return f"{base} Motivo do recontato: {followup_reason}."
        return base

    def _prioritize_followup_contacts(self, source_row: pd.Series) -> list[dict[str, str]]:
        contacts: list[dict[str, str]] = []
        for index in range(1, 4):
            suffix = "" if index == 1 else f"_{index}"
            phone = self._normalize_phone(source_row.get(f"phone_sanitized{suffix}"))
            if not phone:
                continue
            contacts.append(
                {
                    "parent_name": self._clean_parent_name(source_row.get(f"parent_name{suffix}")),
                    "phone_sanitized": phone,
                    "contact_slot": str(
                        source_row.get(f"contact_slot{suffix}", "") or f"responsavel_{index}",
                    ).strip()
                    or f"responsavel_{index}",
                }
            )

        # In recontact campaigns, prefer the second guardian when available.
        if len(contacts) >= 2:
            contacts = [contacts[1], contacts[0], *contacts[2:]]

        while len(contacts) < 3:
            contacts.append(
                {
                    "parent_name": "",
                    "phone_sanitized": "",
                    "contact_slot": "",
                }
            )
        return contacts

    @staticmethod
    def _normalize_text(value: object) -> str:
        text = str(value or "").strip().upper()
        if not text:
            return ""
        normalized = unicodedata.normalize("NFKD", text)
        return "".join(char for char in normalized if not unicodedata.combining(char))

    @staticmethod
    def _normalize_phone(value: object) -> str:
        text = str(value or "").strip()
        if not text or text.lower() == "nan":
            return ""
        if text.endswith(".0") and text.replace(".", "", 1).isdigit():
            text = text[:-2]
        digits = "".join(char for char in text if char.isdigit())
        return digits

    @staticmethod
    def _clean_parent_name(value: object) -> str:
        parent_name = str(value or "").strip()
        if not parent_name or parent_name.lower() == "nan":
            return "Responsavel"
        normalized = FollowupNonResponseCampaignBuilder._normalize_text(parent_name)
        if "MAE" in normalized:
            return "mae"
        if "PAI" in normalized:
            return "pai"
        if "TIA" in normalized:
            return "tia"
        if "AVO" in normalized:
            return "avo"
        return parent_name

    @staticmethod
    def _normalize_class_name_short(value: str) -> str:
        text = str(value or "").strip().upper()
        if not text:
            return "nao informada"
        text = text.replace("INTEGRAL 9H ANUAL", "").strip()
        return text or "nao informada"

    @staticmethod
    def _load_or_create_ledger(path: Path) -> pd.DataFrame:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            empty_df = pd.DataFrame(columns=CAMPAIGN_COLUMNS)
            FollowupNonResponseCampaignBuilder._write_ledger_file(empty_df, path)
            return empty_df
        ledger_df = pd.read_excel(path, sheet_name="Historico")
        for column in CAMPAIGN_COLUMNS:
            if column not in ledger_df.columns:
                ledger_df[column] = ""
        return ledger_df[CAMPAIGN_COLUMNS].copy()

    @staticmethod
    def _append_campaign_to_ledger(ledger_df: pd.DataFrame, campaign_df: pd.DataFrame) -> pd.DataFrame:
        combined = pd.concat([ledger_df, campaign_df], ignore_index=True)
        return combined.drop_duplicates(subset=["campaign_row_id"], keep="first")[CAMPAIGN_COLUMNS].copy()

    @staticmethod
    def _write_campaign_file(df: pd.DataFrame, path: Path) -> None:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Campanha", index=False)

    @staticmethod
    def _write_ledger_file(df: pd.DataFrame, path: Path) -> None:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Historico", index=False)

    @staticmethod
    def _build_campaign_id(created_at: datetime, output_dir: Path) -> str:
        base_id = f"Campanha_Recontato_Sem_Resposta_{created_at:%Y_%m_%d}"
        candidate = base_id
        index = 1
        while (output_dir / f"{candidate}.xlsx").exists():
            candidate = f"{base_id}_{index:02d}"
            index += 1
        return candidate


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gera campanha de recontato para alunos sem resposta efetiva nas campanhas diarias.",
    )
    parser.add_argument(
        "--input",
        help="Arquivo CSV/XLSX com colunas absence_day, student_name, class_name e followup_reason.",
    )
    parser.add_argument(
        "--output-dir",
        help="Pasta de saida da campanha.",
    )
    parser.add_argument(
        "--ledger",
        help="Caminho do ledger da campanha de recontato.",
    )
    args = parser.parse_args()

    builder = FollowupNonResponseCampaignBuilder()
    try:
        result = builder.build_campaign(
            input_path=Path(args.input) if args.input else None,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            ledger_path=Path(args.ledger) if args.ledger else None,
        )
    except Exception as exc:
        logger.exception("Falha ao construir campanha de recontato: %s", exc)
        raise SystemExit(1) from exc

    logger.info(
        "Campanha de recontato finalizada: %s | incluidos=%s | ledger=%s",
        result.campaign_path,
        result.included_rows,
        result.ledger_path,
    )


if __name__ == "__main__":
    main()
