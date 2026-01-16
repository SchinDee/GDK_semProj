# ==============================================================================
# 0. IMPORTY
# ==============================================================================

import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from pathlib import Path
import kagglehub
import time
import re
import sys
import socket

# ==============================================================================
# 1. KONFIGURACE
# ==============================================================================

SCRIPT_DIR = Path(__file__).parent.resolve()
OUTPUT_DIR = SCRIPT_DIR / "output" / "links_batches" / "links_games_ids"


WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "BoardGameGraphBot/1.0 (student project)"

TIMEOUT_SECONDS = 10 
SLEEP_TIME = 0.05
BATCH_SIZE = 500 

# Startovací index (pokud bys chtěl navázat, např. 5000)
START_FROM_INDEX = 0

# Hledáme: Deskové hry (Q131436) nebo Rozšíření (Q10589196)
TARGET_TYPES = [
    "wd:Q131436",    # Board game
    "wd:Q60474521",  # Expansion pack for board game
    "wd:Q19272838",  # Board video game 
    "wd:Q734698",    # Collectible card game 
    "wd:Q142714",    # Card game
    "wd:Q1643932",   # Tabletop role-playing game
    "wd:Q3244175",   # Tabletop game
    "wd:Q1272194",   # Tile-based game
    "wd:Q3177859",   # Dedicated deck card game
    "wd:Q788553",    # German-style board game
    "wd:Q1515156",   # Dice game
]
# ==============================================================================
# 2. POMOCNÉ FUNKCE
# ==============================================================================

def clean_game_name_for_search(name):
    """
    Čistí název hry pro vyhledávání.
    Odstraňuje věci v závorkách, např. 'Catan (5th Edition)' -> 'Catan'.
    """
    if pd.isna(name): return ""
    s = str(name)
    # Odstraníme vše v závorkách (často rok nebo edice)
    s = re.sub(r'\s*\(.*?\)', '', s)
    return s.strip()

def extract_sorted_games(df):
    """
    Vrátí seznam her seřazený podle popularity (users_rated).
    Vrací seznam n-tic: (game_id, name, users_rated)
    """
    print("[INFO] Řadím hry podle popularity (users_rated)...")
    
    # Převedeme users_rated na číslo, kdyby tam byly chyby
    df['users_rated'] = pd.to_numeric(df['users_rated'], errors='coerce').fillna(0)
    
    # Seřadíme sestupně
    df_sorted = df.sort_values(by='users_rated', ascending=False)
    
    games_list = []
    for _, row in df_sorted.iterrows():
        game_id = row['game_id']
        name = row['name']
        rating_count = int(row['users_rated'])
        
        if name and not pd.isna(name):
            games_list.append((game_id, name, rating_count))
            
    print(f"[STATS] TOP 5 nejpopulárnějších her v datasetu:")
    for gid, name, count in games_list[:5]:
        print(f"   - ID {gid}: {name} ({count} hodnocení)")
        
    return games_list

def format_time(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m:02d}m"
    return f"{m}m {s:02d}s"

# ==============================================================================
# 3. SPARQL LOGIKA
# ==============================================================================

def find_wikidata_uri(game_id, game_name): # <--- PŘIDÁN ARGUMENT game_id
    sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
    sparql.addCustomHttpHeader("User-Agent", USER_AGENT)
    sparql.setTimeout(TIMEOUT_SECONDS)
    
    search_name = clean_game_name_for_search(game_name)
    # Escape uvozovek pro SPARQL
    safe_name = search_name.replace('"', '\\"') 
    types_str = " ".join(TARGET_TYPES)
    
    # HYBRIDNÍ DOTAZ:
    # Zkusí najít shodu podle ID. Když nenajde, hledá podle jména.
    # Seřadí výsledky tak, aby ID mělo přednost.
    query = f"""
    SELECT ?item ?priority WHERE {{
      {{
        # --- 1. PRIORITA: Hledání podle BGG ID (P2339) ---
        ?item wdt:P2339 "{game_id}" .
        BIND(1 AS ?priority)
      }}
      UNION
      {{
        # --- 2. PRIORITA: Hledání podle Názvu (Fallback) ---
        VALUES ?type {{ {types_str} }}
        ?item wdt:P31 ?type .
        ?item rdfs:label|skos:altLabel ?label .
        FILTER(LCASE(STR(?label)) = LCASE("{safe_name}"))
        BIND(2 AS ?priority)
      }}
    }}
    ORDER BY ASC(?priority)
    LIMIT 1
    """
    
    try:
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        
        bindings = results["results"]["bindings"]
        if bindings:
            # Našli jsme to!
            found_item = bindings[0]["item"]["value"]
            method = "ID" if bindings[0]["priority"]["value"] == "1" else "NAME"
            return found_item, method # Vracíme i metodu, abychom věděli, jak to našel
            
    except Exception as e:
        print(f" [SPARQL ERROR] {e}")
        pass
    
    return None, None

# ==============================================================================
# 4. HLAVNÍ PROCES
# ==============================================================================

def main():
    print("=== FÁZE 4: Link Discovery (GAMES - RESUMABLE) ===")
    
    try:
        socket.create_connection(("query.wikidata.org", 443), timeout=5)
    except OSError:
        print("[CHYBA] Nelze se připojit k Wikidatům.")
        return

    dataset_path = Path(kagglehub.dataset_download("sujaykapadnis/board-games"))
    csv_file = list(dataset_path.glob("*.csv"))[0]
    df = pd.read_csv(csv_file).fillna("")
    
    # Získání seznamu her
    games_list = extract_sorted_games(df)
    total = len(games_list)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Výstupní složka: {OUTPUT_DIR}")
    
    if START_FROM_INDEX > 0:
        print(f"[RESUME] Přeskakuji prvních {START_FROM_INDEX} her...")

    found_count = 0
    loop_start_time = time.time()
    current_file = None
    batch_index = START_FROM_INDEX // BATCH_SIZE
    
    try:
        for i, (game_id, name, count) in enumerate(games_list):
            
            # --- SKIPPING ---
            if i < START_FROM_INDEX: continue

            # --- FILE ROTATION ---
            if i % BATCH_SIZE == 0 or (i == START_FROM_INDEX and current_file is None):
                if current_file: current_file.close()
                
                batch_index = (i // BATCH_SIZE) + 1
                filename = f"links_games_{batch_index:02d}.ttl"
                file_path = OUTPUT_DIR / filename
                
                mode = "w" if (i == START_FROM_INDEX and i % BATCH_SIZE == 0) else "a"
                if i % BATCH_SIZE == 0: mode = "w"

                current_file = open(file_path, mode, encoding="utf-8")
                
                if current_file.tell() == 0:
                    current_file.write("@prefix owl: <http://www.w3.org/2002/07/owl#> .\n")
                    current_file.write("@prefix game: <http://example.org/game/> .\n\n")
                
                print(f"\n[SYSTEM] Zapisuji do souboru: {filename}")

            # --- STATS ---
            current_idx = i + 1
            percent = (current_idx / total) * 100
            
            eta_str = "Kalibruji..."
            active_time = time.time() - loop_start_time
            processed = current_idx - START_FROM_INDEX
            
            if processed > 20:
                avg = active_time / processed
                rem = total - current_idx
                eta_str = format_time(avg * rem)

            # --- OUTPUT ---
            # Zkrácení názvu pro výpis, aby se vešel do řádku
            display_name = (name[:25] + '..') if len(name) > 25 else name
            
            header = f"[{current_idx}/{total} | {percent:5.1f}% | ETA: {eta_str:<10}]"
            print(f"{header} ID:{game_id:<6} Hledám: {display_name:<30}", end="")
            sys.stdout.flush()
            
            # --- QUERY (OPRAVENO) ---
            t0 = time.time()
            
            # POSÍLÁME game_id I name
            uri, method = find_wikidata_uri(game_id, name) 
            
            dur = time.time() - t0
            
            if uri:
                # Výpis bude hezčí - vidíš, jestli to našel přes ID nebo Jméno
                icon = "🆔" if method == "ID" else "🏷️"
                print(f" -> ✅ {icon} ({dur:.2f}s)")
                
                current_file.write(f"game:{game_id} owl:sameAs <{uri}> .\n")
                current_file.flush()
                found_count += 1
            else:
                print(f" -> ❌ ({dur:.2f}s)")
            
            time.sleep(SLEEP_TIME)

    finally:
        if current_file: current_file.close()

    print(f"\n[SUCCESS] Hotovo! V tomto běhu nalezeno: {found_count}")

if __name__ == "__main__":
    main()