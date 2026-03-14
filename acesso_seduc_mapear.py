import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        print("Conectando ao seu Chrome...")
        browser = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        
        # Encontra a aba da Seduc
        page = None
        for aba in browser.contexts[0].pages:
            if "educacao.sp.gov.br" in aba.url:
                page = aba
                break
        if not page:
            page = browser.contexts[0].pages[0]
            
        await page.bring_to_front()
        print("Abrindo o gravador (Codegen)! Faça os cliques no navegador...")
        
        # A mágica que abre a janela de gravação
        await page.pause() 

if __name__ == "__main__":
    asyncio.run(run())