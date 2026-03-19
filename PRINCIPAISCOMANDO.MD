"C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\Temp\ChromeDebug"

C:/Users/user/buscaativadecia/venv/Scripts/python.exe campaign_builder.py

#Depois conferir a campanha real sem enviar:

C:/Users/user/buscaativadecia/venv/Scripts/python.exe playwright_sender.py --campaign relatorios/Campanha_2026_03_19.xlsx --max-messages 5 --batch-size 3


#E para envio real em lote:
C:/Users/user/buscaativadecia/venv/Scripts/python.exe playwright_sender.py --campaign relatorios/Campanha_2026_03_19.xlsx --send --max-messages 40 --batch-size 30


