from pathlib import Path

import pandas as pd

from config import get_settings
from playwright_sender import PlaywrightSender
from playwright_sender import _resolve_safety_profile
from playwright_sender import _resolve_sender_defaults


def _write_ledger(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_excel(path, sheet_name="Historico", index=False)


def test_reconcile_sent_status_with_ledger_skips_already_notified_contact(tmp_path: Path) -> None:
    ledger_path = tmp_path / "Institutional_Campaign_Ledger.xlsx"
    _write_ledger(
        ledger_path,
        [
            {
                "campaign_id": "Campanha_Institucional_anterior",
                "campaign_row_id": "Campanha_Institucional_anterior|114856536-X|5514991180264|responsavel_1",
                "ra_key": "114856536-X",
                "phone_sanitized": "5514991180264",
                "contact_slot": "responsavel_1",
                "status_envio": "enviado",
                "data_envio": "2026-03-26 09:56:51",
                "status_resposta": "sem_resposta",
                "observacao": "Enviado anteriormente.",
            },
        ],
    )
    sender = PlaywrightSender(
        campaign_path=tmp_path / "campanha.xlsx",
        session_dir=tmp_path / "session",
        ledger_path_override=ledger_path,
    )
    campaign_df = pd.DataFrame(
        [
            {
                "campaign_id": "Campanha_Institucional_nova",
                "campaign_row_id": "",
                "ra_key": "114856536-X",
                "student_name": "ANA BEATRIZ SARAIVA DA SILVA",
                "phone_sanitized": "5514991180264",
                "contact_slot": "responsavel_1",
                "status_envio": "pendente",
                "data_envio": "",
                "status_resposta": "",
                "observacao": "Aviso institucional.",
                "whatsapp_message": "Mensagem",
            },
            {
                "campaign_id": "Campanha_Institucional_nova",
                "campaign_row_id": "",
                "ra_key": "114856536-X",
                "student_name": "ANA BEATRIZ SARAIVA DA SILVA",
                "phone_sanitized": "5514999025832",
                "contact_slot": "responsavel_2",
                "status_envio": "pendente",
                "data_envio": "",
                "status_resposta": "",
                "observacao": "Aviso institucional.",
                "whatsapp_message": "Mensagem",
            },
        ],
    )

    reconciled_df, reconciled_count = sender._reconcile_sent_status_with_ledger(campaign_df)
    pending_rows = sender._select_pending_rows(reconciled_df, max_messages=10)

    assert reconciled_count == 1
    assert reconciled_df.loc[0, "status_envio"] == "enviado"
    assert reconciled_df.loc[0, "data_envio"] == "2026-03-26 09:56:51"
    assert "evitar reenvio" in reconciled_df.loc[0, "observacao"].lower()
    assert pending_rows["phone_sanitized"].tolist() == ["5514999025832"]


def test_ensure_campaign_row_id_uses_contact_identity() -> None:
    row = pd.Series(
        {
            "campaign_id": "Campanha_Institucional_nova",
            "campaign_row_id": "",
            "ra_key": "114856536-X",
            "phone_sanitized": "5514991180264",
            "contact_slot": "responsavel_1",
        }
    )

    campaign_row_id = PlaywrightSender._ensure_campaign_row_id(row)

    assert campaign_row_id == "Campanha_Institucional_nova|114856536-X|5514991180264|responsavel_1"


def test_collapse_institutional_rows_groups_old_contact_lines_into_one_student(tmp_path: Path) -> None:
    sender = PlaywrightSender(
        campaign_path=tmp_path / "relatorios" / "campanhas_institucionais" / "campanha.xlsx",
        session_dir=tmp_path / "session",
        ledger_path_override=tmp_path / "ledger.xlsx",
    )
    campaign_df = pd.DataFrame(
        [
            {
                "campaign_id": "Campanha_Institucional_nova",
                "campaign_row_id": "",
                "ra_key": "114856536-X",
                "student_name": "ANA BEATRIZ SARAIVA DA SILVA",
                "parent_name": "mae",
                "phone_sanitized": "5514991180264",
                "contact_slot": "responsavel_1",
                "status_envio": "enviado",
                "data_envio": "2026-03-26 18:24:55",
                "status_resposta": "sem_resposta",
                "observacao": "Linha 1",
                "whatsapp_message": "Mensagem",
            },
            {
                "campaign_id": "Campanha_Institucional_nova",
                "campaign_row_id": "",
                "ra_key": "114856536-X",
                "student_name": "ANA BEATRIZ SARAIVA DA SILVA",
                "parent_name": "padrasto",
                "phone_sanitized": "5514999025832",
                "contact_slot": "responsavel_2",
                "status_envio": "pendente",
                "data_envio": "",
                "status_resposta": "sem_resposta",
                "observacao": "Linha 2",
                "whatsapp_message": "Mensagem",
            },
            {
                "campaign_id": "Campanha_Institucional_nova",
                "campaign_row_id": "",
                "ra_key": "114856536-X",
                "student_name": "ANA BEATRIZ SARAIVA DA SILVA",
                "parent_name": "avo",
                "phone_sanitized": "5514991437554",
                "contact_slot": "responsavel_3",
                "status_envio": "falha",
                "data_envio": "",
                "status_resposta": "sem_resposta",
                "observacao": "Linha 3",
                "whatsapp_message": "Mensagem",
            },
        ],
    )

    collapsed_df = sender._collapse_institutional_student_rows(campaign_df)

    assert len(collapsed_df) == 1
    assert collapsed_df.loc[0, "status_envio"] == "enviado"
    assert collapsed_df.loc[0, "phone_sanitized"] == "5514991180264"
    assert collapsed_df.loc[0, "phone_sanitized_2"] == "5514999025832"
    assert collapsed_df.loc[0, "phone_sanitized_3"] == "5514991437554"


def test_select_pending_rows_can_resume_from_logged_row_index() -> None:
    sender = PlaywrightSender(
        campaign_path=Path("campanha.xlsx"),
        session_dir=Path("session"),
        ledger_path_override=Path("ledger.xlsx"),
    )
    campaign_df = pd.DataFrame(
        [
            {"phone_sanitized": "5514991111111", "whatsapp_message": "A", "status_envio": "pendente", "student_name": "Aluno 10"},
            {"phone_sanitized": "5514992222222", "whatsapp_message": "B", "status_envio": "pendente", "student_name": "Aluno 82"},
            {"phone_sanitized": "5514993333333", "whatsapp_message": "C", "status_envio": "pendente", "student_name": "Aluno 90"},
        ],
        index=[10, 82, 90],
    )

    pending_rows = sender._select_pending_rows(campaign_df, max_messages=10, start_row=82)

    assert pending_rows.index.tolist() == [82, 90]


def test_conservative_profile_uses_config_defaults() -> None:
    settings = get_settings()

    safety_profile = _resolve_safety_profile(settings, None)
    sender_defaults = _resolve_sender_defaults(settings, safety_profile)

    assert safety_profile == "conservative"
    assert sender_defaults["max_messages"] == settings.sender_default_max_messages
    assert sender_defaults["batch_size"] == settings.sender_default_batch_size


def test_custom_profile_preserves_legacy_defaults() -> None:
    settings = get_settings()

    sender_defaults = _resolve_sender_defaults(settings, "custom")

    assert sender_defaults["max_messages"] == 1
    assert sender_defaults["batch_size"] == 1
    assert sender_defaults["message_delay_min_seconds"] == 30.0
