"C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\Temp\ChromeDebug"

C:/Users/user/buscaativadecia/venv/Scripts/python.exe campaign_builder.py

#Depois conferir a campanha real sem enviar:

C:/Users/user/buscaativadecia/venv/Scripts/python.exe playwright_sender.py --campaign relatorios/Campanha_2026_03_19.xlsx --max-messages 5 --batch-size 3


#E para envio real em lote:
C:/Users/user/buscaativadecia/venv/Scripts/python.exe playwright_sender.py --campaign relatorios/Campanha_2026_03_19.xlsx --send --max-messages 40 --batch-size 30


xemplos:

Último dia disponível no consolidado:

C:/Users/user/buscaativadecia/venv/Scripts/python.exe daily_campaign_builder.py
Dia anterior:

C:/Users/user/buscaativadecia/venv/Scripts/python.exe daily_campaign_builder.py --mode yesterday
Dia específico, por exemplo 18:

C:/Users/user/buscaativadecia/venv/Scripts/python.exe daily_campaign_builder.py --day 18
Isso gera arquivos no formato:

Campanha_Diaria_YYYY_MM_DD_dia_XX.xlsx
E aí o envio seria com o mesmo sender:

C:/Users/user/buscaativadecia/venv/Scripts/python.exe playwright_sender.py --campaign relatorios/Campanha_Diaria_YYYY_MM_DD_dia_XX.xlsx --session-dir user_data/whatsapp_test_session --send --max-messages 5 --batch-size 5
Então, respondendo objetivamente: sim, agora você tem um caminho separado para busca ativa diária sem mexe

