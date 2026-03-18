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


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_CAMPAIGN_PATH = Path("relatorios/Campanha_TESTE.xlsx")
DEFAULT_SESSION_DIR = Path("user_data/whatsapp_sender_session_test")
REQUIRED_COLUMNS = [
    "status_envio",
    "data_envio",
    "observacao",
    "student_name",
    "parent_name",
    "phone_sanitized",
    "whatsapp_message",
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
                    except Exception as exc:
                        campaign_df.at[index, "status_envio"] = "falha"
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
        default=str(DEFAULT_SESSION_DIR),
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
        "--typing-delay-ms",
        type=int,
        default=35,
        help="Delay entre teclas para digitar a mensagem. Padrao: 35ms",
    )
    args = parser.parse_args()

    sender = PlaywrightSender(
        campaign_path=Path(args.campaign),
        session_dir=Path(args.session_dir),
    )
    try:
        processed = sender.run(
            dry_run=not args.send,
            max_messages=max(1, args.max_messages),
            typing_delay_ms=max(0, args.typing_delay_ms),
        )
    except Exception as exc:
        logger.exception("Falha no sender: %s", exc)
        raise SystemExit(1) from exc

    logger.info("Execucao finalizada. Registros processados: %s", processed)


if __name__ == "__main__":
    main()
