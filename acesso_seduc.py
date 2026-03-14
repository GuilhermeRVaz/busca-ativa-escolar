import asyncio
import os
import glob
import pandas as pd
from playwright.async_api import async_playwright

# --- FUNÇÃO PARA UNIFICAR OS ARQUIVOS (O CONSOLIDADO) ---
def unificar_relatorios():
    path = "relatorios"
    arquivos = glob.glob(os.path.join(path, "Faltas_*.xlsx"))
    
    if not arquivos:
        print("\n⚠️ Nenhum arquivo encontrado para unificar.")
        return

    print("\n📊 Gerando Relatório Consolidado...")
    lista_df = []

    for arquivo in arquivos:
        nome_turma = os.path.basename(arquivo).replace("Faltas_", "").replace(".xlsx", "").replace("_", " ")
        try:
            df = pd.read_excel(arquivo)
            # Só adiciona se o arquivo não estiver vazio
            if not df.empty:
                df.insert(0, 'Turma', nome_turma)
                lista_df.append(df)
        except Exception as e:
            print(f"❌ Erro ao ler {nome_turma}: {e}")

    if lista_df:
        df_final = pd.concat(lista_df, ignore_index=True)
        nome_saida = "Relatorio_Consolidado_BuscaAtiva.xlsx"
        df_final.to_excel(nome_saida, index=False)
        print(f"✨ PERFEITO! Arquivo consolidado criado: {nome_saida}")
    else:
        print("⚠️ Não havia dados válidos para consolidar.")

# --- FUNÇÃO PRINCIPAL DO ROBÔ ---
async def run():
    if not os.path.exists("relatorios"):
        os.makedirs("relatorios")

    async with async_playwright() as p:
        print("Conectando ao Chrome...")
        try:
            browser = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
            page = browser.contexts[0].pages[0]
            await page.bring_to_front()
        except Exception as e:
            print("❌ Erro ao conectar ao Chrome. Verifica se o CMD com a porta 9222 está aberto.")
            return

        print("\nIniciando navegação a partir da Home...")
        
        # Passos iniciais
        await page.get_by_title("Diário de Classe").first.click()
        await asyncio.sleep(1.5)
        await page.get_by_role("link", name="Frequência").first.click()
        await asyncio.sleep(1.5)
        await page.get_by_role("link", name="Consulta de Frequência").click()
        await asyncio.sleep(2)

        turmas = [
            "6° ANO 6A INTEGRAL 9H ANUAL", "6° ANO 6B INTEGRAL 9H ANUAL",
            "7° ANO 7A INTEGRAL 9H ANUAL", "7° ANO 7B INTEGRAL 9H ANUAL",
            "8° ANO 8A INTEGRAL 9H ANUAL", "8° ANO 8B INTEGRAL 9H ANUAL",
            "9° ANO 9A INTEGRAL 9H ANUAL"
        ]

        for turma_nome in turmas:
            print(f"\n--- Processando: {turma_nome} ---")
            try:
                # Selecionar Ensino
                await page.get_by_role("textbox", name="Selecione").first.click()
                await page.get_by_text("ENSINO FUNDAMENTAL DE 9 ANOS", exact=True).click()
                await asyncio.sleep(1)

                # Selecionar Turma
                await page.get_by_role("textbox", name="Selecione").nth(1).click()
                texto_busca = "°" + turma_nome.split("°")[1]
                await page.get_by_text(texto_busca, exact=False).click()
                await asyncio.sleep(1)

                # Abrir página da Turma
                await page.get_by_role("link", name=turma_nome.title(), exact=False).first.click()
                await asyncio.sleep(2)

                # Mês de Março (valor "3") e Tipo Faltas (valor "0")
                await page.locator("#slMes").select_option("3")
                await asyncio.sleep(0.5)
                await page.locator("#slTpConsulta").select_option("0")
                
                # Filtrar
                await page.get_by_role("button", name="Filtrar").click()
                await asyncio.sleep(2)

                # Download Excel
                print("Baixando Excel...")
                async with page.expect_download(timeout=60000) as download_info:
                    await page.get_by_role("button", name="Gerar Excel").click()
                
                download = await download_info.value
                nome_limpo = turma_nome.replace(" ", "_").replace("°", "")
                caminho = f"relatorios/Faltas_{nome_limpo}.xlsx"
                await download.save_as(caminho)
                
                # Fechar popup e voltar
                await page.get_by_role("button", name="OK").click()
                await asyncio.sleep(1)
                await page.get_by_role("button", name=" Voltar").click()
                await asyncio.sleep(2)

            except Exception as e:
                print(f"⚠️ Erro na turma {turma_nome}. A saltar para a próxima...")
                try:
                    await page.get_by_role("button", name=" Voltar").click(timeout=3000)
                except:
                    await page.reload() # Se tudo falhar, dá refresh
                    await asyncio.sleep(3)

        print("\n✅ Todos os downloads concluídos!")
        
        # --- A MÁGICA FINAL: UNIFICAR TUDO ---
        unificar_relatorios()

if __name__ == "__main__":
    asyncio.run(run())