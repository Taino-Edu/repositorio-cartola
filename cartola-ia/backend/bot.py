import time
import sys
from datetime import datetime
import main  # Importa seu script de coleta (main.py)

# --- CONFIGURAÇÃO ---
INTERVALO_HORAS = 8  # Roda a cada 8 horas (3x ao dia)
INTERVALO_SEGUNDOS = INTERVALO_HORAS * 3600

def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] [BOT] {msg}")

def iniciar_bot():
    print("="*40)
    print(f"   🤖 CARTOLA IA - BOT DE COLETA")
    print(f"   Status: RODANDO")
    print(f"   Intervalo: {INTERVALO_HORAS} horas")
    print("="*40)
    
    # Garante que o banco existe antes de começar
    try:
        log("Verificando banco de dados...")
        main.setup_database()
    except Exception as e:
        log(f"ERRO CRÍTICO no Setup: {e}")
        return

    # Loop Infinito (Daemon)
    while True:
        try:
            log("Iniciando ciclo de coleta...")
            
            # --- CHAMA A INTELIGÊNCIA ---
            main.run_etl()
            # ---------------------------
            
            log("Ciclo finalizado com sucesso.")
            
        except KeyboardInterrupt:
            # Permite parar o bot com Ctrl+C
            print("\n")
            log("Bot parando via comando do usuário...")
            sys.exit(0)
            
        except Exception as e:
            # Se der erro, não para o bot, só loga e tenta na próxima
            log(f"ERRO durante a execução: {e}")
        
        # Espera para a próxima rodada
        log(f"Dormindo por {INTERVALO_HORAS} horas. Não feche essa janela.")
        
        # Countdown visual simples (opcional, para não parecer travado)
        try:
            time.sleep(INTERVALO_SEGUNDOS)
        except KeyboardInterrupt:
            print("\n")
            log("Bot parando via comando do usuário...")
            sys.exit(0)

if __name__ == "__main__":
    iniciar_bot()