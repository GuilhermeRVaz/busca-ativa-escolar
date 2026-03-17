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
    google_sheet_url: str
    google_sheet_worksheet: str
    google_service_account_file: Path
    default_country_code: str
    default_ddd: str
    whatsapp_message_template: str


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
        google_sheet_url=os.getenv("GOOGLE_SHEET_URL", "").strip(),
        google_sheet_worksheet=os.getenv("GOOGLE_SHEET_WORKSHEET", "Contatos").strip(),
        google_service_account_file=_resolve_path(
            os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", ""),
            "service_account.json",
        ),
        default_country_code=os.getenv("DEFAULT_COUNTRY_CODE", "55").strip(),
        default_ddd=os.getenv("DEFAULT_DDD", "18").strip(),
        whatsapp_message_template=os.getenv(
            "WHATSAPP_MESSAGE_TEMPLATE",
            (
                "Olá {parent_name}, aqui é da escola. Informamos que o(a) aluno(a) "
                "{student_name} apresentou faltas nos dias: {absence_days}. "
                "Pedimos que informe a justificativa ou entre em contato com a escola."
            ),
        ).strip(),
    )
