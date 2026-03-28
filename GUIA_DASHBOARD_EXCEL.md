# Dashboard Excel da Busca Ativa

## O que este arquivo gera

O script `dashboard_builder.py` monta um workbook Excel unico com painel executivo e abas de apoio usando os arquivos ja gerados na pasta `relatorios`.

Arquivo gerado por padrao:

- `relatorios/Dashboard_Busca_Ativa.xlsx`

## Bases utilizadas

O dashboard consome automaticamente:

- `Campanha_*.xlsx`
- `Campanha_Diaria_*.xlsx`
- `Relatorio_Operacional_*.xlsx`
- `Relatorio_de_Retornos_*.xlsx`
- `Campaign_Ledger.xlsx`
- `Daily_Campaign_Ledger.xlsx`

Arquivos de backup, `runtime_autosave` e `TESTE` sao ignorados.

## Como gerar

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe dashboard_builder.py --reports-dir C:/Users/user/buscaativadecia/relatorios
```

Opcionalmente, voce pode informar um caminho de saida:

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe dashboard_builder.py --reports-dir C:/Users/user/buscaativadecia/relatorios --output C:/Users/user/buscaativadecia/relatorios/Dashboard_Busca_Ativa.xlsx
```

## Abas criadas

- `Painel`: KPIs e graficos principais
- `Operacional`: resumo por turma e detalhes operacionais
- `Retornos`: resumo de respondidos, sem retorno e nao recontatar
- `Justificativas`: categorias e detalhes dos textos de resposta
- `Revisao`: pendencias que precisam de conferencia humana
- `Base_Modelo`: instrucoes de uso
- `Base_*`: abas ocultas com os dados consolidados

## Como usar no Excel

1. Rode o fluxo normal de campanhas, envio e relatorios.
2. Gere o dashboard com `dashboard_builder.py`.
3. Abra `Dashboard_Busca_Ativa.xlsx`.
4. Use filtros e tabelas nas abas visiveis para navegar pelos dados.

## Observacoes

- Esta V1 gera o painel pronto por Python, sem VBA.
- O workbook nao depende de colagem manual.
- Para atualizar o painel, basta rodar o script novamente depois de atualizar os relatorios.
