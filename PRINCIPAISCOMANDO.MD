# Principais Comandos

## Atualizar consolidado

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/acesso_seduc.py
```

## Gerar campanha diaria

Ultimo dia disponivel:

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/daily_campaign_builder.py
```

Dia anterior:

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/daily_campaign_builder.py --mode yesterday
```

Dia especifico:

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/daily_campaign_builder.py --day 26
```

## Conferir campanha diaria sem enviar

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/playwright_sender.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_Diaria_2026_03_26_dia_26.xlsx --session-dir C:/Users/user/buscaativadecia/user_data/whatsapp_test_session
```

## Enviar campanha diaria

O sender usa por padrao o perfil conservador:

- `max_messages=8`
- `batch_size=3`
- `message_delay=60s a 150s`
- `batch_break=600s a 1200s`

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/playwright_sender.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_Diaria_2026_03_26_dia_26.xlsx --session-dir C:/Users/user/buscaativadecia/user_data/whatsapp_test_session --send
```

## Retomar campanha a partir de uma linha

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/playwright_sender.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_Diaria_2026_03_26_dia_26.xlsx --session-dir C:/Users/user/buscaativadecia/user_data/whatsapp_test_session --send --start-row 82
```

## Gerar campanha institucional

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/institutional_campaign_builder.py --campaign-name provas_bimestrais_abril
```

## Conferir campanha institucional sem enviar

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/playwright_sender.py --campaign C:/Users/user/buscaativadecia/relatorios/campanhas_institucionais/Campanha_Institucional_provas_bimestrais_abril_2026_03_27.xlsx --session-dir C:/Users/user/buscaativadecia/user_data/whatsapp_test_session --ledger-path C:/Users/user/buscaativadecia/relatorios/campanhas_institucionais/Institutional_Campaign_Ledger.xlsx
```

## Enviar campanha institucional

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/playwright_sender.py --campaign C:/Users/user/buscaativadecia/relatorios/campanhas_institucionais/Campanha_Institucional_provas_bimestrais_abril_2026_03_27.xlsx --session-dir C:/Users/user/buscaativadecia/user_data/whatsapp_test_session --ledger-path C:/Users/user/buscaativadecia/relatorios/campanhas_institucionais/Institutional_Campaign_Ledger.xlsx --send
```

## Processar respostas exportadas

Campanha diaria:

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/whatsapp_export_parser.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_Diaria_2026_03_26_dia_26.xlsx --exports-dir C:/Users/user/buscaativadecia/exports_whatsapp
```

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/response_report_builder.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_Diaria_2026_03_26_dia_26.xlsx
```

Campanha mensal:

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/whatsapp_export_parser.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_2026_03_19.xlsx --exports-dir C:/Users/user/buscaativadecia/exports_whatsapp
```

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe C:/Users/user/buscaativadecia/response_report_builder.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_2026_03_19.xlsx
```
