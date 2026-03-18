import time
import urllib.parse
from pathlib import Path

from playwright.sync_api import sync_playwright


PHONE = "5514981324832"
MESSAGE = "Teste automatico Playwright - ignore"

BASE_DIR = Path(__file__).resolve().parent
USER_DATA_DIR = BASE_DIR / "user_data" / "whatsapp_test_session"


def main() -> None:
    encoded_message = urllib.parse.quote(MESSAGE)
    url = f"https://web.whatsapp.com/send?phone={PHONE}&text={encoded_message}"
    USER_DATA_DIR.mkdir(parents=True, exist_ok=True)

    context = None
    try:
        with sync_playwright() as playwright:
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(USER_DATA_DIR),
                headless=False,
                viewport={"width": 1280, "height": 900},
            )
            page = context.pages[0] if context.pages else context.new_page()

            print("Na primeira execucao, escaneie o QR Code se o WhatsApp pedir.")
            print(f"Sessao persistente em: {USER_DATA_DIR}")
            page.goto(url, wait_until="domcontentloaded")

            input("Pressione ENTER quando a conversa carregar...")
            page.keyboard.press("Enter")

            print("Mensagem enviada.")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Execucao interrompida manualmente. Nenhum problema na sessao.")
    finally:
        if context is not None:
            context.close()


if __name__ == "__main__":
    main()
