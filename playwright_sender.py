import argparse
import logging
import random
import re
import shutil
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from config import get_settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_CAMPAIGN_PATH = Path("relatorios/Campanha_TESTE.xlsx")
DEFAULT_TEST_SESSION_DIR = Path("user_data/whatsapp_sender_session_test")
DEFAULT_REAL_SESSION_DIR = Path("user_data/whatsapp_sender_session_real")
DEFAULT_DAILY_LEDGER_PATH = Path("relatorios/Daily_Campaign_Ledger.xlsx")
DEFAULT_INSTITUTIONAL_LEDGER_PATH = Path("relatorios/campanhas_institucionais/Institutional_Campaign_Ledger.xlsx")
LEGACY_SENDER_DEFAULTS = {
    "max_messages": 1,
    "batch_size": 1,
    "message_delay_min_seconds": 30.0,
    "message_delay_max_seconds": 90.0,
    "batch_break_min_seconds": 240.0,
    "batch_break_max_seconds": 480.0,
}
REQUIRED_COLUMNS = [
    "campaign_id",
    "status_envio",
    "data_envio",
    "status_resposta",
    "observacao",
    "class_name",
    "ra_key",
    "student_name",
    "parent_name",
    "phone_sanitized",
    "absence_days",
    "message_template_id",
    "whatsapp_message",
    "contact_slot",
]
FALLBACK_CONTACT_COLUMNS = [
    "parent_name_2",
    "phone_sanitized_2",
    "contact_slot_2",
    "parent_name_3",
    "phone_sanitized_3",
    "contact_slot_3",
]


class InvalidNumberError(RuntimeError):
    """Raised when WhatsApp indicates that a phone number is unavailable."""


def _resolve_safety_profile(settings, requested_profile: str | None) -> str:
    profile = (requested_profile or settings.sender_safety_profile or "conservative").strip().lower()
    return profile if profile in {"conservative", "custom"} else "conservative"


def _resolve_sender_defaults(settings, safety_profile: str) -> dict[str, float | int]:
    if safety_profile == "custom":
        return dict(LEGACY_SENDER_DEFAULTS)
    return {
        "max_messages": settings.sender_default_max_messages,
        "batch_size": settings.sender_default_batch_size,
        "message_delay_min_seconds": settings.sender_default_message_delay_min_seconds,
        "message_delay_max_seconds": settings.sender_default_message_delay_max_seconds,
        "batch_break_min_seconds": settings.sender_default_batch_break_min_seconds,
        "batch_break_max_seconds": settings.sender_default_batch_break_max_seconds,
    }


class PlaywrightSender:
    def __init__(
        self,
        campaign_path: Path,
        session_dir: Path,
        ledger_path_override: Path | None = None,
    ) -> None:
        self.campaign_path = campaign_path
        self.session_dir = session_dir
        self.ledger_path_override = ledger_path_override

    def run(
        self,
        dry_run: bool = True,
        max_messages: int = 1,
        start_row: int = 0,
        typing_delay_ms: int = 35,
        batch_size: int = 1,
        message_delay_min_seconds: float = 30.0,
        message_delay_max_seconds: float = 90.0,
        batch_break_min_seconds: float = 240.0,
        batch_break_max_seconds: float = 480.0,
        safety_profile: str = "custom",
    ) -> int:
        campaign_df = self._load_campaign()
        campaign_df, reconciled_count = self._reconcile_sent_status_with_ledger(campaign_df)
        if reconciled_count:
            logger.info(
                "Historico aplicado antes do envio: %s linha(s) ja constavam como enviadas no ledger.",
                reconciled_count,
            )
        pending_rows = self._select_pending_rows(
            campaign_df,
            max_messages=max_messages,
            start_row=start_row,
        )

        if pending_rows.empty:
            logger.info("Nenhuma linha pendente e valida encontrada em %s.", self.campaign_path)
            return 0

        logger.info(
            "Linhas selecionadas para processamento: %s (dry_run=%s).",
            len(pending_rows),
            dry_run,
        )
        logger.info(
            "Configuracao de envio | perfil=%s | max_messages=%s | batch_size=%s | pausa_msg=%.1fs-%.1fs | pausa_lote=%.1fs-%.1fs",
            safety_profile,
            max_messages,
            batch_size,
            message_delay_min_seconds,
            message_delay_max_seconds,
            batch_break_min_seconds,
            batch_break_max_seconds,
        )

        if dry_run:
            for index, row in pending_rows.iterrows():
                logger.info(
                    "DRY-RUN | linha=%s | aluno=%s | telefone=%s",
                    index,
                    row["student_name"],
                    row["phone_sanitized"],
                )
            return len(pending_rows)

        self.session_dir.mkdir(parents=True, exist_ok=True)
        self._backup_campaign()
        if reconciled_count:
            self._persist_campaign_state(campaign_df)

        sent_count = 0
        total_selected = len(pending_rows)
        with sync_playwright() as playwright:
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(self.session_dir),
                headless=False,
                viewport={"width": 1280, "height": 900},
            )
            try:
                page = context.pages[0] if context.pages else context.new_page()

                logger.info("Abrindo WhatsApp Web para reutilizar ou criar sessao.")
                page.goto("https://web.whatsapp.com/", wait_until="domcontentloaded")
                input(
                    "No primeiro uso, escaneie o QR Code. Quando o WhatsApp Web estiver pronto, pressione ENTER...",
                )

                for index, row in pending_rows.iterrows():
                    current_position = sent_count + 1
                    current_batch = ((current_position - 1) // batch_size) + 1
                    logger.info(
                        "Processando envio %s/%s | lote=%s | aluno=%s",
                        current_position,
                        total_selected,
                        current_batch,
                        row["student_name"],
                    )
                    while True:
                        row = campaign_df.loc[index].copy()
                        try:
                            self._send_single_message(
                                page=page,
                                phone=str(row["phone_sanitized"]),
                                message=str(row["whatsapp_message"]),
                                typing_delay_ms=typing_delay_ms,
                            )
                            campaign_df.at[index, "status_envio"] = "enviado"
                            campaign_df.at[index, "data_envio"] = datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S",
                            )
                            campaign_df.at[index, "observacao"] = self._append_observation(
                                campaign_df.at[index, "observacao"],
                                f"Enviado via playwright_sender para {row['contact_slot']} ({row['phone_sanitized']}).",
                            )
                            sent_count += 1
                            self._persist_campaign_state(campaign_df)
                            logger.info(
                                "Envio concluido | linha=%s | aluno=%s | contato=%s",
                                index,
                                row["student_name"],
                                row["contact_slot"],
                            )
                            self._sleep_after_send(
                                sent_count=sent_count,
                                total_selected=total_selected,
                                batch_size=batch_size,
                                message_delay_min_seconds=message_delay_min_seconds,
                                message_delay_max_seconds=message_delay_max_seconds,
                                batch_break_min_seconds=batch_break_min_seconds,
                                batch_break_max_seconds=batch_break_max_seconds,
                            )
                            break
                        except InvalidNumberError as exc:
                            rotated = self._promote_fallback_contact(
                                campaign_df,
                                index,
                                reason=f"Numero invalido ou ausente no WhatsApp: {exc}",
                            )
                            self._persist_campaign_state(campaign_df)
                            logger.warning(
                                "Numero nao encontrado no WhatsApp | linha=%s | aluno=%s | telefone=%s",
                                index,
                                row["student_name"],
                                row["phone_sanitized"],
                            )
                            if rotated:
                                logger.info(
                                    "Tentando contato alternativo para linha=%s | aluno=%s",
                                    index,
                                    row["student_name"],
                                )
                                continue
                            campaign_df.at[index, "status_envio"] = "falha"
                            campaign_df.at[index, "status_resposta"] = "numero_invalido"
                            self._persist_campaign_state(campaign_df)
                            break
                        except Exception as exc:
                            logger.exception(
                                "Falha no envio | linha=%s | aluno=%s | erro=%s",
                                index,
                                row["student_name"],
                                exc,
                            )
                            campaign_df.at[index, "status_envio"] = "falha"
                            campaign_df.at[index, "observacao"] = self._append_observation(
                                campaign_df.at[index, "observacao"],
                                f"Falha no envio sem troca automatica de contato: {exc}",
                            )
                            if "invalido" in str(exc).lower():
                                campaign_df.at[index, "status_resposta"] = "numero_invalido"
                            self._persist_campaign_state(campaign_df)
                            break
            finally:
                try:
                    context.close()
                except Exception as exc:
                    logger.warning("Contexto do navegador ja estava fechado ao encerrar a execucao: %s", exc)

        self._sync_campaign_to_ledger(campaign_df)
        self._write_operational_report(campaign_df)
        return sent_count

    def _load_campaign(self) -> pd.DataFrame:
        if not self.campaign_path.exists():
            raise FileNotFoundError(f"Arquivo de campanha nao encontrado: {self.campaign_path}")

        df = pd.read_excel(self.campaign_path, sheet_name="Campanha")
        missing_columns = sorted(set(REQUIRED_COLUMNS).difference(df.columns))
        if missing_columns:
            raise KeyError(
                "Arquivo de campanha sem colunas obrigatorias: " + ", ".join(missing_columns),
            )
        for column in ["campaign_row_id", *FALLBACK_CONTACT_COLUMNS, "status_envio", "data_envio", "observacao", "status_resposta"]:
            if column not in df.columns:
                df[column] = ""
        for column in ["status_envio", "data_envio", "observacao", "status_resposta"]:
            if column in df.columns:
                df[column] = df[column].astype("object")
        for column in ["phone_sanitized", "phone_sanitized_2", "phone_sanitized_3"]:
            if column in df.columns:
                df[column] = df[column].apply(self._normalize_phone).astype("object")
        for column in ["parent_name", "parent_name_2", "parent_name_3", "contact_slot", "contact_slot_2", "contact_slot_3", "campaign_row_id"]:
            if column in df.columns:
                df[column] = df[column].apply(self._safe_text).astype("object")
        if "campaign_row_id" in df.columns:
            df["campaign_row_id"] = df.apply(self._ensure_campaign_row_id, axis=1).astype("object")
        if self._is_institutional_campaign():
            df = self._collapse_institutional_student_rows(df)
        return df.copy()

    def _reconcile_sent_status_with_ledger(self, campaign_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        if not self._is_institutional_campaign():
            return campaign_df, 0

        settings = get_settings()
        ledger_path = self._resolve_ledger_path(settings)
        if not ledger_path.exists():
            return campaign_df, 0

        ledger_df = pd.read_excel(ledger_path, sheet_name="Historico")
        if ledger_df.empty:
            return campaign_df, 0

        working_campaign = campaign_df.copy()
        working_ledger = ledger_df.copy()
        for df in (working_campaign, working_ledger):
            for column in ["ra_key", "contact_slot", "status_envio", "status_resposta", "data_envio", "observacao", "campaign_id", "campaign_row_id"]:
                if column not in df.columns:
                    df[column] = ""
                df[column] = df[column].apply(self._safe_text).astype("object")
            for column in ["phone_sanitized", "phone_sanitized_2", "phone_sanitized_3"]:
                if column not in df.columns:
                    df[column] = ""
                df[column] = df[column].apply(self._normalize_phone).astype("object")

        working_campaign["campaign_row_id"] = working_campaign.apply(self._ensure_campaign_row_id, axis=1).astype("object")
        working_ledger["campaign_row_id"] = working_ledger.apply(self._ensure_campaign_row_id, axis=1).astype("object")
        working_campaign["_history_key"] = working_campaign.apply(self._build_history_key, axis=1)
        working_ledger["_history_key"] = working_ledger.apply(self._build_history_key, axis=1)

        sent_ledger = working_ledger.loc[
            working_ledger["status_envio"].str.lower().eq("enviado")
            & working_ledger["_history_key"].ne("")
        ].copy()
        if sent_ledger.empty:
            return working_campaign.drop(columns=["_history_key"]), 0

        sent_ledger = sent_ledger.sort_values(["data_envio", "campaign_id"], kind="stable")
        sent_by_key = sent_ledger.drop_duplicates(subset=["_history_key"], keep="last").set_index("_history_key")

        reconciled_count = 0
        for index, row in working_campaign.iterrows():
            current_status = self._safe_text(row.get("status_envio")).lower()
            history_key = self._safe_text(row.get("_history_key"))
            if current_status == "enviado" or not history_key or history_key not in sent_by_key.index:
                continue

            source_row = sent_by_key.loc[history_key]
            working_campaign.at[index, "status_envio"] = "enviado"
            if not self._safe_text(working_campaign.at[index, "data_envio"]):
                working_campaign.at[index, "data_envio"] = self._safe_text(source_row.get("data_envio"))
            source_response = self._safe_text(source_row.get("status_resposta"))
            if source_response and not self._safe_text(working_campaign.at[index, "status_resposta"]):
                working_campaign.at[index, "status_resposta"] = source_response
            working_campaign.at[index, "observacao"] = self._append_observation(
                working_campaign.at[index, "observacao"],
                (
                    "Marcado como enviado com base no ledger institucional "
                    f"({ledger_path.name}) para evitar reenvio ao mesmo responsavel."
                ),
            )
            reconciled_count += 1

        return working_campaign.drop(columns=["_history_key"]), reconciled_count

    def _select_pending_rows(self, df: pd.DataFrame, max_messages: int, start_row: int = 0) -> pd.DataFrame:
        prepared = df.copy()
        prepared["phone_sanitized"] = prepared["phone_sanitized"].apply(self._normalize_phone)
        prepared["whatsapp_message"] = prepared["whatsapp_message"].apply(self._safe_text)
        prepared["status_envio"] = prepared["status_envio"].apply(self._safe_text).str.lower()

        filtered = prepared.loc[
            prepared["phone_sanitized"].ne("")
            & prepared["whatsapp_message"].ne("")
            & prepared["status_envio"].isin({"", "pendente", "falha"})
        ].copy()
        if start_row > 0:
            filtered = filtered.loc[filtered.index >= start_row].copy()
        return filtered.head(max_messages)

    def _promote_fallback_contact(self, campaign_df: pd.DataFrame, index: int, reason: str) -> bool:
        current_slot = self._safe_text(campaign_df.at[index, "contact_slot"])
        current_phone = self._safe_text(campaign_df.at[index, "phone_sanitized"])
        next_phone = self._safe_text(campaign_df.at[index, "phone_sanitized_2"])
        if not next_phone:
            campaign_df.at[index, "observacao"] = self._append_observation(
                campaign_df.at[index, "observacao"],
                f"{reason} | Sem contato alternativo apos {current_slot or 'contato_atual'} ({current_phone}).",
            )
            return False

        next_parent = self._safe_text(campaign_df.at[index, "parent_name_2"]) or "Responsavel"
        next_slot = self._safe_text(campaign_df.at[index, "contact_slot_2"]) or "responsavel_2"
        campaign_df.at[index, "observacao"] = self._append_observation(
            campaign_df.at[index, "observacao"],
            (
                f"{reason} | Fallback acionado: {current_slot or 'contato_atual'} ({current_phone}) "
                f"-> {next_slot} ({next_phone})."
            ),
        )
        campaign_df.at[index, "parent_name"] = next_parent
        campaign_df.at[index, "phone_sanitized"] = next_phone
        campaign_df.at[index, "contact_slot"] = next_slot
        campaign_df.at[index, "parent_name_2"] = self._safe_text(campaign_df.at[index, "parent_name_3"])
        campaign_df.at[index, "phone_sanitized_2"] = self._safe_text(campaign_df.at[index, "phone_sanitized_3"])
        campaign_df.at[index, "contact_slot_2"] = self._safe_text(campaign_df.at[index, "contact_slot_3"])
        campaign_df.at[index, "parent_name_3"] = ""
        campaign_df.at[index, "phone_sanitized_3"] = ""
        campaign_df.at[index, "contact_slot_3"] = ""
        campaign_df.at[index, "status_envio"] = "pendente"
        campaign_df.at[index, "status_resposta"] = campaign_df.at[index, "status_resposta"] or "sem_resposta"
        return True

    def _is_institutional_campaign(self) -> bool:
        return "campanhas_institucionais" in str(self.campaign_path.parent).lower()

    def _collapse_institutional_student_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "ra_key" not in df.columns:
            return df

        working = df.copy().reset_index().rename(columns={"index": "_source_index"})
        ra_keys = working["ra_key"].fillna("").astype(str).str.strip()
        if not ra_keys.duplicated(keep=False).any():
            return working.drop(columns=["_source_index"])

        collapsed_rows: list[pd.Series] = []
        grouped = working.groupby(["campaign_id", "ra_key"], sort=False, dropna=False)
        for _, group in grouped:
            ordered_group = group.sort_values("_source_index", kind="stable")
            sent_group = ordered_group[
                ordered_group["status_envio"].apply(self._safe_text).str.lower().eq("enviado")
            ]
            active_group = ordered_group[
                ordered_group["status_envio"].apply(self._safe_text).str.lower().isin({"", "pendente", "falha"})
            ]
            base_row = (
                sent_group.sort_values(["data_envio", "_source_index"], kind="stable").iloc[0].copy()
                if not sent_group.empty
                else (active_group.iloc[0].copy() if not active_group.empty else ordered_group.iloc[0].copy())
            )

            contacts = self._collect_contacts_from_group(ordered_group)
            preferred_phone = self._normalize_phone(base_row.get("phone_sanitized"))
            if preferred_phone:
                contacts = sorted(contacts, key=lambda item: item[1] != preferred_phone)

            if contacts:
                base_row["parent_name"] = contacts[0][0]
                base_row["phone_sanitized"] = contacts[0][1]
                base_row["contact_slot"] = contacts[0][2]
            else:
                base_row["parent_name"] = self._safe_text(base_row.get("parent_name")) or "Responsavel"
                base_row["phone_sanitized"] = self._normalize_phone(base_row.get("phone_sanitized"))
                base_row["contact_slot"] = self._safe_text(base_row.get("contact_slot")) or "responsavel_1"

            for fallback_index in range(2, 4):
                contact_position = fallback_index - 1
                if contact_position < len(contacts):
                    parent_name, phone, slot = contacts[contact_position]
                else:
                    parent_name, phone, slot = ("", "", "")
                base_row[f"parent_name_{fallback_index}"] = parent_name
                base_row[f"phone_sanitized_{fallback_index}"] = phone
                base_row[f"contact_slot_{fallback_index}"] = slot

            observations: list[str] = []
            for value in ordered_group["observacao"].tolist():
                text = self._safe_text(value)
                if text and text not in observations:
                    observations.append(text)
            base_row["observacao"] = " | ".join(observations)

            if not sent_group.empty:
                sent_row = sent_group.sort_values(["data_envio", "_source_index"], kind="stable").iloc[0]
                base_row["status_envio"] = "enviado"
                base_row["data_envio"] = self._safe_text(sent_row.get("data_envio"))
                base_row["status_resposta"] = (
                    self._safe_text(sent_row.get("status_resposta"))
                    or self._safe_text(base_row.get("status_resposta"))
                )

            base_row["campaign_row_id"] = self._ensure_campaign_row_id(base_row)
            collapsed_rows.append(base_row)

        collapsed_df = pd.DataFrame(collapsed_rows)
        collapsed_df = collapsed_df.sort_values("_source_index", kind="stable").drop(columns=["_source_index"])
        logger.info(
            "Campanha institucional consolidada para %s aluno(s); %s linha(s) por contato foram agrupadas.",
            len(collapsed_df),
            len(df) - len(collapsed_df),
        )
        return collapsed_df[df.columns]

    def _collect_contacts_from_group(self, group: pd.DataFrame) -> list[tuple[str, str, str]]:
        contacts: list[tuple[str, str, str]] = []
        seen_phones: set[str] = set()
        contact_columns = [
            ("parent_name", "phone_sanitized", "contact_slot"),
            ("parent_name_2", "phone_sanitized_2", "contact_slot_2"),
            ("parent_name_3", "phone_sanitized_3", "contact_slot_3"),
        ]
        for _, row in group.iterrows():
            for parent_column, phone_column, slot_column in contact_columns:
                phone = self._normalize_phone(row.get(phone_column))
                if not phone or phone in seen_phones:
                    continue
                parent_name = self._safe_text(row.get(parent_column)) or "Responsavel"
                slot = self._safe_text(row.get(slot_column)) or "responsavel_1"
                contacts.append((parent_name, phone, slot))
                seen_phones.add(phone)
        return contacts

    def _send_single_message(
        self,
        page,
        phone: str,
        message: str,
        typing_delay_ms: int,
    ) -> None:
        url = f"https://web.whatsapp.com/send?phone={phone}"
        logger.info("Abrindo conversa para %s", phone)
        page.goto(url, wait_until="domcontentloaded")

        if self._handle_invalid_number_modal(page):
            raise InvalidNumberError("Numero invalido ou nao localizado pelo WhatsApp.")

        message_box = self._wait_for_message_box(page)
        message_box.click()
        try:
            message_box.press_sequentially(message, delay=typing_delay_ms)
        except PlaywrightTimeoutError:
            logger.warning("Digitacao sequencial excedeu o tempo. Aplicando fallback com insert_text.")
            page.keyboard.insert_text(message)
        time.sleep(random.uniform(1.5, 3.0))
        page.keyboard.press("Enter")
        time.sleep(random.uniform(2.0, 4.0))

    def _sleep_after_send(
        self,
        sent_count: int,
        total_selected: int,
        batch_size: int,
        message_delay_min_seconds: float,
        message_delay_max_seconds: float,
        batch_break_min_seconds: float,
        batch_break_max_seconds: float,
    ) -> None:
        if sent_count >= total_selected:
            return

        if batch_size > 0 and sent_count % batch_size == 0:
            pause = random.uniform(batch_break_min_seconds, batch_break_max_seconds)
            logger.info(
                "Fim do lote atual. Aguardando %.1f segundos antes do proximo lote...",
                pause,
            )
            time.sleep(pause)
            return

        pause = random.uniform(message_delay_min_seconds, message_delay_max_seconds)
        logger.info(
            "Aguardando %.1f segundos antes da proxima mensagem...",
            pause,
        )
        time.sleep(pause)

    @staticmethod
    def _wait_for_message_box(page):
        selectors = [
            "footer div[contenteditable='true']",
            "div[aria-label='Digite uma mensagem']",
            "div[title='Digite uma mensagem']",
        ]
        last_error = None
        for selector in selectors:
            if PlaywrightSender._handle_invalid_number_modal(page):
                raise InvalidNumberError("Numero invalido ou nao localizado pelo WhatsApp.")
            try:
                locator = page.locator(selector).last
                locator.wait_for(state="visible", timeout=25000)
                return locator
            except PlaywrightTimeoutError as exc:
                last_error = exc
        if PlaywrightSender._handle_invalid_number_modal(page):
            raise InvalidNumberError("Numero invalido ou nao localizado pelo WhatsApp.")
        raise RuntimeError("Nao foi possivel localizar a caixa de mensagem.") from last_error

    @staticmethod
    def _has_invalid_number_message(page) -> bool:
        invalid_markers = [
            "numero de telefone compartilhado por url e invalido",
            "phone number shared via url is invalid",
            "não foi encontrado",
            "nao foi encontrado",
        ]
        body_text = page.locator("body").inner_text(timeout=5000).lower()
        return any(marker in body_text for marker in invalid_markers)

    def _save_campaign(self, df: pd.DataFrame) -> None:
        with pd.ExcelWriter(self.campaign_path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Campanha", index=False)

    def _backup_campaign(self) -> None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = self.campaign_path.with_name(
            f"{self.campaign_path.stem}.backup_{timestamp}{self.campaign_path.suffix}",
        )
        shutil.copy2(self.campaign_path, backup_path)
        logger.info("Backup da campanha criado em %s", backup_path)

    def _sync_campaign_to_ledger(self, campaign_df: pd.DataFrame) -> None:
        settings = get_settings()
        ledger_path = self._resolve_ledger_path(settings)
        if not ledger_path.exists():
            logger.warning("Campaign ledger nao encontrado em %s. Sincronizacao ignorada.", ledger_path)
            return

        ledger_df = pd.read_excel(ledger_path, sheet_name="Historico")
        for column in campaign_df.columns:
            if column not in ledger_df.columns:
                ledger_df[column] = ""
        for column in ledger_df.columns:
            if column not in campaign_df.columns:
                campaign_df[column] = ""

        object_columns = {
            "campaign_row_id",
            "campaign_id",
            "status_envio",
            "data_envio",
            "status_resposta",
            "observacao",
            "message_template_id",
            "phone_sanitized",
            "phone_sanitized_2",
            "phone_sanitized_3",
            "ra_key",
            "contact_slot",
            "contact_slot_2",
            "contact_slot_3",
            "parent_name",
            "parent_name_2",
            "parent_name_3",
        }
        for column in object_columns:
            if column in ledger_df.columns:
                if column.startswith("phone_sanitized"):
                    ledger_df[column] = ledger_df[column].apply(self._normalize_phone).astype("object")
                else:
                    ledger_df[column] = ledger_df[column].apply(self._safe_text).astype("object")
            if column in campaign_df.columns:
                if column.startswith("phone_sanitized"):
                    campaign_df[column] = campaign_df[column].apply(self._normalize_phone).astype("object")
                else:
                    campaign_df[column] = campaign_df[column].apply(self._safe_text).astype("object")

        key_columns = self._resolve_merge_key_columns(ledger_df, campaign_df)
        ledger_df["_merge_key"] = ledger_df.apply(
            lambda row: self._build_merge_key(row, key_columns),
            axis=1,
        )
        campaign_df = campaign_df.copy()
        campaign_df["_merge_key"] = campaign_df.apply(
            lambda row: self._build_merge_key(row, key_columns),
            axis=1,
        )

        updates_by_key = {
            row["_merge_key"]: row.to_dict()
            for _, row in campaign_df.iterrows()
            if row["_merge_key"]
        }

        updated_rows = 0
        for index, row in ledger_df.iterrows():
            merge_key = row["_merge_key"]
            if merge_key not in updates_by_key:
                continue
            source = updates_by_key[merge_key]
            for column in campaign_df.columns:
                if column == "_merge_key":
                    continue
                ledger_df.at[index, column] = source.get(column, "")
            updated_rows += 1

        ledger_df = ledger_df.drop(columns=["_merge_key"])
        with pd.ExcelWriter(ledger_path, engine="openpyxl") as writer:
            ledger_df.to_excel(writer, sheet_name="Historico", index=False)
        logger.info("Campaign ledger sincronizado em %s com %s registro(s).", ledger_path, updated_rows)

    def _write_operational_report(self, campaign_df: pd.DataFrame) -> None:
        if campaign_df.empty:
            return

        campaign_id = self._safe_text(campaign_df.iloc[0].get("campaign_id")) or self.campaign_path.stem
        report_path = self.campaign_path.parent / f"Relatorio_Operacional_{campaign_id}.xlsx"

        normalized = campaign_df.copy()
        normalized["status_envio"] = normalized["status_envio"].apply(self._safe_text).str.lower()
        normalized["status_resposta"] = normalized["status_resposta"].apply(self._safe_text).str.lower()

        total_registros = len(normalized)
        total_enviados = int(normalized["status_envio"].eq("enviado").sum())
        total_falhas = int(normalized["status_envio"].eq("falha").sum())
        total_pendentes = int(normalized["status_envio"].isin({"", "pendente"}).sum())
        total_numero_invalido = int(normalized["status_resposta"].eq("numero_invalido").sum())
        total_respondidos = int(normalized["status_resposta"].eq("respondido").sum())
        taxa_envio = round((total_enviados / total_registros) * 100, 2) if total_registros else 0.0

        resumo_df = pd.DataFrame(
            [
                {"indicador": "campaign_id", "valor": campaign_id},
                {"indicador": "total_registros", "valor": total_registros},
                {"indicador": "total_enviados", "valor": total_enviados},
                {"indicador": "total_falhas", "valor": total_falhas},
                {"indicador": "total_pendentes", "valor": total_pendentes},
                {"indicador": "total_numero_invalido", "valor": total_numero_invalido},
                {"indicador": "total_respondidos", "valor": total_respondidos},
                {"indicador": "taxa_envio_percentual", "valor": taxa_envio},
            ]
        )

        por_turma_df = (
            normalized.groupby(["class_name", "status_envio"], dropna=False)
            .size()
            .reset_index(name="quantidade")
            .sort_values(["class_name", "status_envio"])
        )
        por_template_df = (
            normalized.groupby(["message_template_id", "status_envio"], dropna=False)
            .size()
            .reset_index(name="quantidade")
            .sort_values(["message_template_id", "status_envio"])
        )

        detalhes_columns = [
            "campaign_id",
            "class_name",
            "student_name",
            "parent_name",
            "phone_sanitized",
            "absence_days",
            "message_template_id",
            "status_envio",
            "data_envio",
            "status_resposta",
            "observacao",
        ]
        detalhes_df = normalized[detalhes_columns].copy()

        with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
            resumo_df.to_excel(writer, sheet_name="Resumo", index=False)
            por_turma_df.to_excel(writer, sheet_name="Por_Turma", index=False)
            por_template_df.to_excel(writer, sheet_name="Por_Template", index=False)
            detalhes_df.to_excel(writer, sheet_name="Detalhes", index=False)
        logger.info("Relatorio operacional salvo em %s", report_path)

    @staticmethod
    def _normalize_phone(value: object) -> str:
        if pd.isna(value):
            return ""
        if isinstance(value, float) and value.is_integer():
            raw = str(int(value))
        else:
            raw = str(value).strip()
            if raw.endswith(".0"):
                raw = raw[:-2]
        digits = re.sub(r"\D", "", raw)
        return digits if len(digits) in {12, 13} else ""

    @staticmethod
    def _safe_text(value: object) -> str:
        if pd.isna(value):
            return ""
        text = str(value).strip()
        return "" if text.lower() == "nan" else text

    @staticmethod
    def _append_observation(current: object, extra: str) -> str:
        existing = PlaywrightSender._safe_text(current)
        return extra if not existing else f"{existing} | {extra}"

    @staticmethod
    def _build_history_key(row: pd.Series) -> str:
        ra_key = PlaywrightSender._safe_text(row.get("ra_key"))
        phone = PlaywrightSender._normalize_phone(row.get("phone_sanitized"))
        contact_slot = PlaywrightSender._safe_text(row.get("contact_slot"))
        if not ra_key or not phone:
            return ""
        return "|".join([ra_key, phone, contact_slot])

    @staticmethod
    def _ensure_campaign_row_id(row: pd.Series) -> str:
        existing = PlaywrightSender._safe_text(row.get("campaign_row_id"))
        if existing:
            return existing
        campaign_id = PlaywrightSender._safe_text(row.get("campaign_id"))
        history_key = PlaywrightSender._build_history_key(row)
        if campaign_id and history_key:
            return f"{campaign_id}|{history_key}"
        if campaign_id:
            ra_key = PlaywrightSender._safe_text(row.get("ra_key"))
            if ra_key:
                return f"{campaign_id}|{ra_key}"
        return ""

    @staticmethod
    def _build_merge_key(row: pd.Series, key_columns: list[str]) -> str:
        parts = [PlaywrightSender._safe_text(row.get(column)) for column in key_columns]
        return "|".join(parts)

    @staticmethod
    def _resolve_merge_key_columns(ledger_df: pd.DataFrame, campaign_df: pd.DataFrame) -> list[str]:
        if (
            "campaign_row_id" in ledger_df.columns
            and "campaign_row_id" in campaign_df.columns
            and campaign_df["campaign_row_id"].fillna("").astype(str).str.strip().ne("").any()
        ):
            return ["campaign_row_id"]
        return ["campaign_id", "ra_key", "phone_sanitized", "contact_slot"]

    def _resolve_ledger_path(self, settings) -> Path:
        if self.ledger_path_override is not None:
            return self.ledger_path_override
        if "campanhas_institucionais" in str(self.campaign_path.parent).lower():
            return DEFAULT_INSTITUTIONAL_LEDGER_PATH
        if self.campaign_path.stem.lower().startswith("campanha_diaria_"):
            return DEFAULT_DAILY_LEDGER_PATH
        return settings.campaign_ledger_path

    @staticmethod
    def _has_invalid_number_message(page) -> bool:
        invalid_markers = [
            "numero de telefone compartilhado por url e invalido",
            "phone number shared via url is invalid",
            "nao esta no whatsapp",
            "não está no whatsapp",
            "nao está no whatsapp",
            "não esta no whatsapp",
            "nÃ£o foi encontrado",
            "nao foi encontrado",
            "não foi encontrado",
        ]
        body_text = page.locator("body").inner_text(timeout=5000).lower()
        return any(marker in body_text for marker in invalid_markers)

    @staticmethod
    def _handle_invalid_number_modal(page) -> bool:
        try:
            if not PlaywrightSender._has_invalid_number_message(page):
                return False
        except Exception:
            return False

        ok_candidates = [
            page.get_by_role("button", name="OK"),
            page.locator("button").filter(has_text=re.compile(r"^OK$", re.IGNORECASE)),
            page.locator("[role='button']").filter(has_text=re.compile(r"^OK$", re.IGNORECASE)),
        ]
        for locator in ok_candidates:
            try:
                locator.first.click(timeout=2000)
                time.sleep(1.0)
                return True
            except Exception:
                continue
        return True

    def _persist_campaign_state(self, df: pd.DataFrame) -> None:
        try:
            self._save_campaign(df)
        except PermissionError:
            autosave_path = self.campaign_path.with_name(
                f"{self.campaign_path.stem}.runtime_autosave{self.campaign_path.suffix}",
            )
            with pd.ExcelWriter(autosave_path, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name="Campanha", index=False)
            logger.warning(
                "Nao foi possivel salvar a campanha principal porque ela esta aberta no Excel. "
                "Estado salvo em %s. Feche a planilha antes da proxima execucao.",
                autosave_path,
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sender seguro para testar envios no WhatsApp Web com Playwright.",
    )
    parser.add_argument(
        "--campaign",
        default=str(DEFAULT_CAMPAIGN_PATH),
        help="Arquivo da campanha de teste. Padrao: relatorios/Campanha_TESTE.xlsx",
    )
    parser.add_argument(
        "--session-dir",
        help="Diretorio da sessao persistente do WhatsApp Web.",
    )
    parser.add_argument(
        "--ledger-path",
        help="Caminho opcional para sincronizar um ledger especifico.",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="Executa envio real. Sem essa flag, roda em dry-run.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        help="Maximo de mensagens por execucao. Quando omitido, usa o perfil de seguranca ativo.",
    )
    parser.add_argument(
        "--start-row",
        type=int,
        default=0,
        help="Indice minimo da linha da campanha a partir do qual o sender deve retomar. Padrao: 0",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Quantidade de mensagens por lote. Quando omitido, usa o perfil de seguranca ativo.",
    )
    parser.add_argument(
        "--typing-delay-ms",
        type=int,
        default=35,
        help="Delay entre teclas para digitar a mensagem. Padrao: 35ms",
    )
    parser.add_argument(
        "--message-delay-min-seconds",
        type=float,
        help="Pausa minima entre mensagens. Quando omitido, usa o perfil de seguranca ativo.",
    )
    parser.add_argument(
        "--message-delay-max-seconds",
        type=float,
        help="Pausa maxima entre mensagens. Quando omitido, usa o perfil de seguranca ativo.",
    )
    parser.add_argument(
        "--batch-break-min-seconds",
        type=float,
        help="Pausa minima entre lotes. Quando omitido, usa o perfil de seguranca ativo.",
    )
    parser.add_argument(
        "--batch-break-max-seconds",
        type=float,
        help="Pausa maxima entre lotes. Quando omitido, usa o perfil de seguranca ativo.",
    )
    parser.add_argument(
        "--safety-profile",
        choices=["conservative", "custom"],
        help="Perfil de seguranca do sender. Quando omitido, usa SENDER_SAFETY_PROFILE.",
    )
    args = parser.parse_args()

    settings = get_settings()
    safety_profile = _resolve_safety_profile(settings, args.safety_profile)
    sender_defaults = _resolve_sender_defaults(settings, safety_profile)

    max_messages = int(args.max_messages if args.max_messages is not None else sender_defaults["max_messages"])
    batch_size = int(args.batch_size if args.batch_size is not None else sender_defaults["batch_size"])
    message_delay_min_seconds = float(
        args.message_delay_min_seconds
        if args.message_delay_min_seconds is not None
        else sender_defaults["message_delay_min_seconds"]
    )
    message_delay_max_seconds = float(
        args.message_delay_max_seconds
        if args.message_delay_max_seconds is not None
        else sender_defaults["message_delay_max_seconds"]
    )
    batch_break_min_seconds = float(
        args.batch_break_min_seconds
        if args.batch_break_min_seconds is not None
        else sender_defaults["batch_break_min_seconds"]
    )
    batch_break_max_seconds = float(
        args.batch_break_max_seconds
        if args.batch_break_max_seconds is not None
        else sender_defaults["batch_break_max_seconds"]
    )

    if batch_size < 1:
        parser.error("--batch-size deve ser maior ou igual a 1.")
    if max_messages < 1:
        parser.error("--max-messages deve ser maior ou igual a 1.")
    if args.start_row < 0:
        parser.error("--start-row deve ser maior ou igual a 0.")
    if message_delay_min_seconds > message_delay_max_seconds:
        parser.error("message-delay-min-seconds nao pode ser maior que message-delay-max-seconds.")
    if batch_break_min_seconds > batch_break_max_seconds:
        parser.error("batch-break-min-seconds nao pode ser maior que batch-break-max-seconds.")

    campaign_path = Path(args.campaign)
    if args.session_dir:
        session_dir = Path(args.session_dir)
    else:
        session_dir = (
            DEFAULT_TEST_SESSION_DIR
            if "teste" in campaign_path.stem.lower()
            else DEFAULT_REAL_SESSION_DIR
        )

    sender = PlaywrightSender(
        campaign_path=campaign_path,
        session_dir=session_dir,
        ledger_path_override=Path(args.ledger_path) if args.ledger_path else None,
    )
    try:
        processed = sender.run(
            dry_run=not args.send,
            max_messages=max_messages,
            start_row=args.start_row,
            typing_delay_ms=max(0, args.typing_delay_ms),
            batch_size=batch_size,
            message_delay_min_seconds=message_delay_min_seconds,
            message_delay_max_seconds=message_delay_max_seconds,
            batch_break_min_seconds=batch_break_min_seconds,
            batch_break_max_seconds=batch_break_max_seconds,
            safety_profile=safety_profile,
        )
    except Exception as exc:
        logger.exception("Falha no sender: %s", exc)
        raise SystemExit(1) from exc

    logger.info("Execucao finalizada. Registros processados: %s", processed)


if __name__ == "__main__":
    main()
