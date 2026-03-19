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


class PlaywrightSender:
    def __init__(self, campaign_path: Path, session_dir: Path) -> None:
        self.campaign_path = campaign_path
        self.session_dir = session_dir

    def run(
        self,
        dry_run: bool = True,
        max_messages: int = 1,
        typing_delay_ms: int = 35,
        batch_size: int = 1,
        message_delay_min_seconds: float = 30.0,
        message_delay_max_seconds: float = 90.0,
        batch_break_min_seconds: float = 240.0,
        batch_break_max_seconds: float = 480.0,
    ) -> int:
        campaign_df = self._load_campaign()
        pending_rows = self._select_pending_rows(campaign_df, max_messages=max_messages)

        if pending_rows.empty:
            logger.info("Nenhuma linha pendente e valida encontrada em %s.", self.campaign_path)
            return 0

        logger.info(
            "Linhas selecionadas para processamento: %s (dry_run=%s).",
            len(pending_rows),
            dry_run,
        )
        logger.info(
            "Configuracao de envio | batch_size=%s | pausa_msg=%.1fs-%.1fs | pausa_lote=%.1fs-%.1fs",
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
                            "Enviado via playwright_sender.",
                        )
                        sent_count += 1
                        self._save_campaign(campaign_df)
                        logger.info(
                            "Envio concluido | linha=%s | aluno=%s",
                            index,
                            row["student_name"],
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
                    except Exception as exc:
                        campaign_df.at[index, "status_envio"] = "falha"
                        if "invalido" in str(exc).lower():
                            campaign_df.at[index, "status_resposta"] = "numero_invalido"
                        campaign_df.at[index, "observacao"] = self._append_observation(
                            campaign_df.at[index, "observacao"],
                            f"Falha no envio: {exc}",
                        )
                        self._save_campaign(campaign_df)
                        logger.exception(
                            "Falha no envio | linha=%s | aluno=%s | erro=%s",
                            index,
                            row["student_name"],
                            exc,
                        )
                        break
            finally:
                context.close()

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
        for column in ["status_envio", "data_envio", "observacao", "status_resposta"]:
            if column in df.columns:
                df[column] = df[column].astype("object")
        return df.copy()

    def _select_pending_rows(self, df: pd.DataFrame, max_messages: int) -> pd.DataFrame:
        prepared = df.copy()
        prepared["phone_sanitized"] = prepared["phone_sanitized"].apply(self._normalize_phone)
        prepared["whatsapp_message"] = prepared["whatsapp_message"].apply(self._safe_text)
        prepared["status_envio"] = prepared["status_envio"].apply(self._safe_text).str.lower()

        filtered = prepared.loc[
            prepared["phone_sanitized"].ne("")
            & prepared["whatsapp_message"].ne("")
            & prepared["status_envio"].isin({"", "pendente", "falha"})
        ].copy()
        return filtered.head(max_messages)

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

        if self._has_invalid_number_message(page):
            raise ValueError("Numero invalido ou nao localizado pelo WhatsApp.")

        message_box = self._wait_for_message_box(page)
        message_box.click()
        message_box.press_sequentially(message, delay=typing_delay_ms)
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
            try:
                locator = page.locator(selector).last
                locator.wait_for(state="visible", timeout=25000)
                return locator
            except PlaywrightTimeoutError as exc:
                last_error = exc
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
        ledger_path = settings.campaign_ledger_path
        if not ledger_path.exists():
            logger.warning("Campaign ledger nao encontrado em %s. Sincronizacao ignorada.", ledger_path)
            return

        ledger_df = pd.read_excel(ledger_path, sheet_name="Historico")
        for column in [
            "campaign_id",
            "status_envio",
            "data_envio",
            "status_resposta",
            "observacao",
            "message_template_id",
            "phone_sanitized",
            "ra_key",
            "contact_slot",
        ]:
            if column in ledger_df.columns:
                ledger_df[column] = ledger_df[column].astype("object")
        for column in campaign_df.columns:
            if column not in ledger_df.columns:
                ledger_df[column] = ""
        for column in ledger_df.columns:
            if column not in campaign_df.columns:
                campaign_df[column] = ""

        key_columns = ["campaign_id", "ra_key", "phone_sanitized", "contact_slot"]
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
    def _build_merge_key(row: pd.Series, key_columns: list[str]) -> str:
        parts = [PlaywrightSender._safe_text(row.get(column)) for column in key_columns]
        return "|".join(parts)


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
        "--send",
        action="store_true",
        help="Executa envio real. Sem essa flag, roda em dry-run.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=1,
        help="Maximo de mensagens por execucao. Padrao: 1",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        help="Quantidade de mensagens por lote. Padrao: 1",
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
        default=30.0,
        help="Pausa minima entre mensagens. Padrao: 30s",
    )
    parser.add_argument(
        "--message-delay-max-seconds",
        type=float,
        default=90.0,
        help="Pausa maxima entre mensagens. Padrao: 90s",
    )
    parser.add_argument(
        "--batch-break-min-seconds",
        type=float,
        default=240.0,
        help="Pausa minima entre lotes. Padrao: 240s",
    )
    parser.add_argument(
        "--batch-break-max-seconds",
        type=float,
        default=480.0,
        help="Pausa maxima entre lotes. Padrao: 480s",
    )
    args = parser.parse_args()

    if args.batch_size < 1:
        parser.error("--batch-size deve ser maior ou igual a 1.")
    if args.max_messages < 1:
        parser.error("--max-messages deve ser maior ou igual a 1.")
    if args.message_delay_min_seconds > args.message_delay_max_seconds:
        parser.error("message-delay-min-seconds nao pode ser maior que message-delay-max-seconds.")
    if args.batch_break_min_seconds > args.batch_break_max_seconds:
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
    )
    try:
        processed = sender.run(
            dry_run=not args.send,
            max_messages=max(1, args.max_messages),
            typing_delay_ms=max(0, args.typing_delay_ms),
            batch_size=args.batch_size,
            message_delay_min_seconds=args.message_delay_min_seconds,
            message_delay_max_seconds=args.message_delay_max_seconds,
            batch_break_min_seconds=args.batch_break_min_seconds,
            batch_break_max_seconds=args.batch_break_max_seconds,
        )
    except Exception as exc:
        logger.exception("Falha no sender: %s", exc)
        raise SystemExit(1) from exc

    logger.info("Execucao finalizada. Registros processados: %s", processed)


if __name__ == "__main__":
    main()
