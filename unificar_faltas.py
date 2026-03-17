import pandas as pd
import os
import glob

def unificar_excel():
    path = "relatorios"
    # Busca todos os arquivos que começam com 'Faltas_' e terminam com '.xlsx'
    arquivos = glob.glob(os.path.join(path, "Faltas_*.xlsx"))
    
    if not arquivos:
        print("Nenhum arquivo encontrado na pasta 'relatorios'.")
        return

    lista_df = []

    print(f"Encontrados {len(arquivos)} arquivos. Iniciando unificação...")

    for arquivo in arquivos:
        # Extrai o nome da turma do nome do arquivo para usar como identificador
        nome_turma = os.path.basename(arquivo).replace("Faltas_", "").replace(".xlsx", "").replace("_", " ")
        
        # Lê o Excel
        df = pd.read_excel(arquivo)
        
        # Adiciona uma coluna no começo com o nome da turma
        df.insert(0, 'Turma', nome_turma)
        
        lista_df.append(df)
        print(f"Dados da turma {nome_turma} extraídos.")

    # Junta todos os DataFrames em um só
    df_final = pd.concat(lista_df, ignore_index=True)

    # Salva o resultado final dentro da pasta relatorios/
    os.makedirs(path, exist_ok=True)
    nome_saida = os.path.join(path, "Relatorio_Mestre_Faltas_Marco.xlsx")
    df_final.to_excel(nome_saida, index=False)
    
    print(f"\n✨ SUCESSO! Todos os dados foram unidos em: {nome_saida}")

if __name__ == "__main__":
    unificar_excel()