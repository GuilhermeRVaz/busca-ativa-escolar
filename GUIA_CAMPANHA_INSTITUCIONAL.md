# Campanha Institucional Separada

## Objetivo

Esta trilha gera e envia recados gerais sem mexer no fluxo da busca ativa diaria ou mensal.

## Arquivos usados

- Campanhas: `relatorios/campanhas_institucionais/`
- Ledger proprio: `relatorios/campanhas_institucionais/Institutional_Campaign_Ledger.xlsx`
- Relatorio operacional: salvo na mesma pasta da campanha institucional

## Perfil de envio

O sender agora usa por padrao o perfil conservador:

- `max_messages=8`
- `batch_size=3`
- `message_delay=60s a 150s`
- `batch_break=600s a 1200s`

Se quiser outro ritmo, passe os parametros manualmente na linha de comando.

## Gerar a campanha

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe institutional_campaign_builder.py --campaign-name provas_bimestrais_abril
```

## Conferir sem enviar

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe playwright_sender.py --campaign C:/Users/user/buscaativadecia/relatorios/campanhas_institucionais/Campanha_Institucional_provas_bimestrais_abril_2026_03_27.xlsx --session-dir C:/Users/user/buscaativadecia/user_data/whatsapp_test_session --ledger-path C:/Users/user/buscaativadecia/relatorios/campanhas_institucionais/Institutional_Campaign_Ledger.xlsx
```

## Envio real

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe playwright_sender.py --campaign C:/Users/user/buscaativadecia/relatorios/campanhas_institucionais/Campanha_Institucional_provas_bimestrais_abril_2026_03_27.xlsx --session-dir C:/Users/user/buscaativadecia/user_data/whatsapp_test_session --ledger-path C:/Users/user/buscaativadecia/relatorios/campanhas_institucionais/Institutional_Campaign_Ledger.xlsx --send
```

## Retomar de uma linha especifica

Use o mesmo numero exibido no `DRY-RUN` ou no log do sender:

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe playwright_sender.py --campaign C:/Users/user/buscaativadecia/relatorios/campanhas_institucionais/Campanha_Institucional_provas_bimestrais_abril_2026_03_27.xlsx --session-dir C:/Users/user/buscaativadecia/user_data/whatsapp_test_session --ledger-path C:/Users/user/buscaativadecia/relatorios/campanhas_institucionais/Institutional_Campaign_Ledger.xlsx --send --start-row 82
```

## Regras desta trilha

- Usa um envio por aluno e preserva contatos alternativos apenas como fallback tecnico.
- Ordena os disparos em `8 ANO`, `9 ANO`, `7 ANO`, `6 ANO`.
- Nas mensagens, a turma aparece no formato curto, como `8 ANO A`.
- As mensagens se identificam explicitamente como comunicados da Escola Decia.
- As mensagens sao randomizadas de forma estavel por campanha.
