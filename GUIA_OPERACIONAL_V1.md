# Guia Operacional V1

Este projeto agora tem dois caminhos:

1. `main`
   Fluxo operacional estavel, mantido como referencia.
2. `codex/produto-v1-desktop`
   Fluxo de evolucao do produto, interface e melhoria de relatorios.

## Regra principal

Nao usar `TXT_BRUTO_TOTAL_ULTIMOS_ZIPS.txt` como fluxo oficial.

O fluxo oficial de respostas da V1 e:

1. Rodar a campanha
2. Exportar apenas as conversas daquela campanha
3. Extrair os `.zip`
4. Colocar os arquivos `.txt` em `exports_whatsapp/<campaign_id>/`
5. Rodar `whatsapp_export_parser.py`
6. Rodar `response_report_builder.py`
7. Revisar o relatorio final

## Estrutura esperada

Exemplo de campanha diaria:

- campanha: `relatorios/Campanha_Diaria_2026_03_20_dia_19.xlsx`
- pasta das exportacoes: `exports_whatsapp/Campanha_Diaria_2026_03_20_dia_19/`

Exemplo de conteudo da pasta:

- `exports_whatsapp/Campanha_Diaria_2026_03_20_dia_19/Conversa do WhatsApp com Mae do Aluno.txt`
- `exports_whatsapp/Campanha_Diaria_2026_03_20_dia_19/5514999999999.txt`

## Passo a passo diario

### 1. Gerar ou localizar a campanha

Exemplo:

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe daily_campaign_builder.py --day 19
```

### 1.1 Conferir a campanha sem enviar

O sender usa por padrao o perfil conservador para reduzir risco de bloqueio:

- `max_messages=8`
- `batch_size=3`
- `message_delay=60s a 150s`
- `batch_break=600s a 1200s`

Exemplo:

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe playwright_sender.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_Diaria_2026_03_20_dia_19.xlsx --session-dir C:/Users/user/buscaativadecia/user_data/whatsapp_test_session
```

### 1.2 Enviar de verdade

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe playwright_sender.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_Diaria_2026_03_20_dia_19.xlsx --session-dir C:/Users/user/buscaativadecia/user_data/whatsapp_test_session --send
```

### 1.3 Retomar de uma linha especifica

Use o mesmo numero exibido pelo `DRY-RUN` ou pelo log:

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe playwright_sender.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_Diaria_2026_03_20_dia_19.xlsx --session-dir C:/Users/user/buscaativadecia/user_data/whatsapp_test_session --send --start-row 82
```

### 2. Exportar conversas no WhatsApp

Exporte apenas as conversas dos contatos daquela campanha.

No WhatsApp:

1. Abra a conversa
2. Exporte a conversa
3. Extraia o `.zip`
4. Separe o arquivo `.txt`

### 3. Organizar a pasta da campanha

Coloque os arquivos `.txt` dentro de:

```text
exports_whatsapp/Campanha_Diaria_2026_03_20_dia_19/
```

### 4. Gerar base normalizada das respostas

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe whatsapp_export_parser.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_Diaria_2026_03_20_dia_19.xlsx --exports-dir C:/Users/user/buscaativadecia/exports_whatsapp
```

Saida esperada:

```text
relatorios/WhatsApp_Responses_Normalized_Campanha_Diaria_2026_03_20_dia_19.xlsx
```

### 5. Gerar relatorio final de retornos

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe response_report_builder.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_Diaria_2026_03_20_dia_19.xlsx
```

Saida esperada:

```text
relatorios/Relatorio_de_Retornos_Campanha_Diaria_2026_03_20_dia_19.xlsx
```

## Passo a passo mensal

Mesma logica, trocando o arquivo da campanha.

Exemplo:

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe whatsapp_export_parser.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_2026_03_19.xlsx --exports-dir C:/Users/user/buscaativadecia/exports_whatsapp
```

Depois:

```bash
C:/Users/user/buscaativadecia/venv/Scripts/python.exe response_report_builder.py --campaign C:/Users/user/buscaativadecia/relatorios/Campanha_2026_03_19.xlsx
```

## Se algo nao casar

Se o parser nao conseguir casar uma conversa:

1. confira se o `.txt` esta dentro da pasta correta da campanha
2. confira se a conversa exportada pertence mesmo a um numero daquela campanha
3. confira se o nome do arquivo ajuda no casamento
4. rode primeiro o parser e analise a aba `Files` da base normalizada

## Regra de seguranca

Enquanto o produto novo evolui:

- `main` segue como referencia operacional
- novos experimentos e interface ficam nesta branch
- o fluxo por comando continua sendo a base de fallback
