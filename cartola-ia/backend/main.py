import sqlite3
import requests
import json
import os
import time
import sys
from datetime import datetime

# --- CONFIGURAÇÃO ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FOLDER = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DB_FOLDER, 'cartola.db')
API_URL = "https://api.cartola.globo.com/atletas/mercado"

# Headers simples (FotMob não bloqueia fácil)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
}

# --- MAPEAMENTO SÉRIE A 2026 (Para Escudos do SofaScore que já funcionam) ---
MAPA_SOFASCORE = {
    262: 5981, 267: 1974, 266: 1961, 263: 1958, # RJ
    264: 1957, 275: 1963, 276: 1981, 277: 1968, 280: 1999, 2305: 21982, # SP
    282: 1977, 283: 1954, # MG
    284: 5926, 285: 1966, # RS
    293: 1967, 294: 1980, # PR
    315: 21845, # SC
    364: 2012,  # PA
    265: 1955, 287: 1995 # BA
}

def get_db_connection():
    if not os.path.exists(DB_FOLDER): os.makedirs(DB_FOLDER)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def setup_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS raw_source (id INTEGER PRIMARY KEY, collected_at DATETIME, payload TEXT)''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS recommendations (
        player_id INTEGER PRIMARY KEY, apelido TEXT, clube_id INTEGER, escudo_url TEXT, foto_url TEXT,
        posicao_id INTEGER, preco REAL, media_num REAL, jogos_num INTEGER, score_mvp REAL, motivo TEXT, updated_at DATETIME
    )''')
    conn.commit()
    conn.close()

# --- NOVO ROBÔ: FOTMOB ---
def buscar_foto_fotmob(nome_jogador):
    try:
        # 1. Pesquisa o nome na API do FotMob
        url_search = f"https://apigw.fotmob.com/searchapi/suggest?term={nome_jogador}"
        r = requests.get(url_search, headers=HEADERS, timeout=2)
        
        if r.status_code == 200:
            data = r.json()
            # Pega a sugestão de jogador (squadMemberSuggest)
            sugestoes = data.get('squadMemberSuggest', [])
            
            if sugestoes:
                # Pega o primeiro resultado (geralmente é o certo pq o nome do Cartola é preciso)
                id_fotmob = sugestoes[0].get('id')
                if id_fotmob:
                    # Monta a URL da foto PNG transparente
                    return f"https://images.fotmob.com/image_resources/playerimages/{id_fotmob}.png"
    except Exception as e:
        pass
    
    return None

def calcular_score(atleta):
    scout = atleta.get('scout', {})
    score_base = (scout.get('G',0)*8 + scout.get('A',0)*5 + scout.get('FD',0)*1.2 + scout.get('FF',0)*0.8 + scout.get('DS',0)*1.5)
    media = atleta.get('media_num', 0)
    jogos = atleta.get('jogos_num', 0)
    fator = 0.5 if jogos < 3 else 1.0
    score_final = (score_base + (media * 2)) * fator
    
    motivo = []
    if media > 5: motivo.append("Boa Média")
    if scout.get('G',0) > 2: motivo.append("Artilheiro")
    
    return score_final, ", ".join(motivo) or "Opção Regular"

def run_etl():
    print(f"[{datetime.now()}] Iniciando Coleta via FOTMOB (Fotos PNG)...")
    try:
        data = requests.get(API_URL).json()
    except:
        print("Erro na API Cartola"); return

    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Vamos focar nos jogadores PROVÁVEIS e NULOS (Status 7) e ordenar por preço
    atletas = [a for a in data.get('atletas', []) if a.get('status_id') == 7]
    atletas = sorted(atletas, key=lambda x: x['preco_num'], reverse=True)
    
    total = len(atletas)
    print(f"Buscando fotos para {total} jogadores titulares...")

    for i, atleta in enumerate(atletas):
        apelido = atleta['apelido']
        
        # Barra de Progresso
        sys.stdout.write(f"\rProcessando {i+1}/{total}: {apelido}                   ")
        sys.stdout.flush()

        # 1. Busca Foto no FotMob
        foto_url = buscar_foto_fotmob(apelido)
        
        # 2. Se falhar, usa a do Cartola (Fallback)
        if not foto_url:
            foto_url = atleta.get('foto', '').replace('FORMATO', '220x220')
        else:
            time.sleep(0.1) # Respeita a API (bem rápido)

        # Escudo SofaScore (já estava funcionando)
        clube_id = atleta['clube_id']
        sofa_team_id = MAPA_SOFASCORE.get(clube_id)
        escudo_url = f"https://img.sofascore.com/api/v1/team/{sofa_team_id}/image" if sofa_team_id else ""
        
        score, reason = calcular_score(atleta)

        cursor.execute('''
        INSERT INTO recommendations (player_id, apelido, clube_id, escudo_url, foto_url, posicao_id, preco, media_num, jogos_num, score_mvp, motivo, updated_at) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?) 
        ON CONFLICT(player_id) DO UPDATE SET 
            score_mvp=excluded.score_mvp, 
            motivo=excluded.motivo, 
            escudo_url=excluded.escudo_url, 
            foto_url=excluded.foto_url, 
            updated_at=excluded.updated_at
        ''', (atleta['atleta_id'], apelido, clube_id, escudo_url, foto_url, atleta['posicao_id'], atleta['preco_num'], atleta['media_num'], atleta['jogos_num'], score, reason, datetime.now()))
        
        conn.commit()

    conn.close()
    print("\n\n✅ Sucesso! Fotos atualizadas via FotMob.")

if __name__ == "__main__": setup_database(); run_etl()