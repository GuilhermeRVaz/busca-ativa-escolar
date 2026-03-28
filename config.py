import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def _resolve_path(raw_value: str, default_name: str) -> Path:
    candidate = (raw_value or default_name).strip()
    path = Path(candidate)
    if not path.is_absolute():
        path = BASE_DIR / path
    return path


@dataclass(frozen=True)
class Settings:
    base_dir: Path
    consolidated_report_path: Path
    ready_to_send_output_path: Path
    campaign_ledger_path: Path
    google_sheet_url: str
    google_sheet_worksheet: str
    google_service_account_file: Path
    default_country_code: str
    default_ddd: str
    school_name: str
    whatsapp_message_template: str
    sender_safety_profile: str
    sender_default_max_messages: int
    sender_default_batch_size: int
    sender_default_message_delay_min_seconds: float
    sender_default_message_delay_max_seconds: float
    sender_default_batch_break_min_seconds: float
    sender_default_batch_break_max_seconds: float


def get_settings() -> Settings:
    return Settings(
        base_dir=BASE_DIR,
        consolidated_report_path=_resolve_path(
            os.getenv("CONSOLIDATED_REPORT_PATH", ""),
            "relatorios/Relatorio_Consolidado_BuscaAtiva.xlsx",
        ),
        ready_to_send_output_path=_resolve_path(
            os.getenv("READY_TO_SEND_OUTPUT_PATH", ""),
            "relatorios/Ready_To_Send_List.xlsx",
        ),
        campaign_ledger_path=_resolve_path(
            os.getenv("CAMPAIGN_LEDGER_PATH", ""),
            "relatorios/Campaign_Ledger.xlsx",
        ),
        google_sheet_url=os.getenv("GOOGLE_SHEET_URL", "").strip(),
        google_sheet_worksheet=os.getenv("GOOGLE_SHEET_WORKSHEET", "Contatos").strip(),
        google_service_account_file=_resolve_path(
            os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", ""),
            "service_account.json",
        ),
        default_country_code=os.getenv("DEFAULT_COUNTRY_CODE", "55").strip(),
        default_ddd=os.getenv("DEFAULT_DDD", "18").strip(),
        school_name=os.getenv("SCHOOL_NAME", "Escola Decia").strip() or "Escola Decia",
        whatsapp_message_template=os.getenv(
            "WHATSAPP_MESSAGE_TEMPLATE",
            (
                "Ola {parent_name}, aqui e da {school_name}. Informamos que o(a) aluno(a) "
                "{student_name} apresentou faltas nos dias: {absence_days}. "
                "Pedimos que informe a justificativa ou entre em contato com a escola."
            ),
        ).strip(),
        sender_safety_profile=os.getenv("SENDER_SAFETY_PROFILE", "conservative").strip().lower() or "conservative",
        sender_default_max_messages=int(os.getenv("SENDER_DEFAULT_MAX_MESSAGES", "8").strip() or "8"),
        sender_default_batch_size=int(os.getenv("SENDER_DEFAULT_BATCH_SIZE", "3").strip() or "3"),
        sender_default_message_delay_min_seconds=float(
            os.getenv("SENDER_DEFAULT_MESSAGE_DELAY_MIN_SECONDS", "60").strip() or "60",
        ),
        sender_default_message_delay_max_seconds=float(
            os.getenv("SENDER_DEFAULT_MESSAGE_DELAY_MAX_SECONDS", "150").strip() or "150",
        ),
        sender_default_batch_break_min_seconds=float(
            os.getenv("SENDER_DEFAULT_BATCH_BREAK_MIN_SECONDS", "600").strip() or "600",
        ),
        sender_default_batch_break_max_seconds=float(
            os.getenv("SENDER_DEFAULT_BATCH_BREAK_MAX_SECONDS", "1200").strip() or "1200",
        ),
    )
