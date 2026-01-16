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
from collections import Counter

# ==============================================================================
# 1. KONFIGURACE
# ==============================================================================

SCRIPT_DIR = Path(__file__).parent.resolve()
OUTPUT_DIR = SCRIPT_DIR / "output" / "links_batches" / "links_agents"

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "BoardGameGraphBot/1.0 (student project)"

TIMEOUT_SECONDS = 30 
SLEEP_TIME = 0.05
BATCH_SIZE = 500 
START_FROM_INDEX = 0

ALLOWED_OCCUPATIONS = [
    "wd:Q3191582",        # Video game artist
    "wd:Q18882335",       # Video game designer
    "wd:Q1544133",        # Board game designer
    "wd:Q3630699",        # Game designer
    # "wd:Q2500638",        # Creator                           |      2,864
    # "wd:Q627325",         # Graphic Designer                  |      8,376
    # "wd:Q5322166",        # Designer (Generic)                |     16,689
    # "wd:Q1925963",        # Graphic Artist                    |     22,420
    # "wd:Q644687",         # Illustrator                       |     37,130
    # "wd:Q483501",         # Artist                            |     81,362
]

# ==============================================================================
# 2. POMOCNÉ FUNKCE
# ==============================================================================

def clean_for_prefix(text):
    if pd.isna(text) or text == "": return ""
    s = str(text)
    s = s.replace(", Inc", " Inc").replace(", Ltd", " Ltd").replace(", LLC", " LLC")
    s = s.replace(" / ", "-").replace("/", "-")
    s = s.replace("&", "and")
    s = re.sub(r'[^\w-]', '_', s)
    s = re.sub(r'_+', '_', s)
    s = s.strip('_')
    return s

def clean_name_for_search(name):
    clean = re.sub(r'\s*\(.*?\)', '', name)
    return clean.strip()

def extract_sorted_agents(df):
    cnt = Counter()
    cols_to_process = ['designer', 'artist']
    
    for col in cols_to_process:
        if col in df.columns:
            for raw_text in df[col].dropna():
                if pd.isna(raw_text) or raw_text == "": continue
                if "Uncredited" in str(raw_text): continue
                
                text = str(raw_text).replace(", Inc", " Inc").replace(", Ltd", " Ltd").replace(", LLC", " LLC")
                items = text.split(',')
                for item in items:
                    name = item.strip()
                    if name and name.lower() != "uncredited":
                        cnt[name] += 1
    
    sorted_pairs = cnt.most_common()
    print(f"[STATS] TOP 5 nejčastějších osob (Designers + Artists):")
    for name, count in sorted_pairs[:5]:
        print(f"   - {name}: {count} výskytů")
    return sorted_pairs

def format_time(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m:02d}m"
    return f"{m}m {s:02d}s"

# ==============================================================================
# 3. SPARQL LOGIKA
# ==============================================================================

def find_wikidata_uri(agent_name):
    sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
    sparql.addCustomHttpHeader("User-Agent", USER_AGENT)
    sparql.setTimeout(TIMEOUT_SECONDS)
    
    search_name = clean_name_for_search(agent_name)
    safe_name = search_name.replace('"', '\\"')
    occupations_str = " ".join(ALLOWED_OCCUPATIONS)
    
    query_template = """
    SELECT ?item ?itemLabel WHERE {{
      VALUES ?occupation {{ {occupations} }}
      ?item wdt:P106 ?occupation .
      ?item rdfs:label ?label .
      FILTER(LCASE(STR(?label)) = LCASE("{name}"))
      ?item wdt:P31 wd:Q5 .
    }}
    LIMIT 1
    """
    
    try:
        sparql.setQuery(query_template.format(occupations=occupations_str, name=safe_name))
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        if results["results"]["bindings"]: return results["results"]["bindings"][0]["item"]["value"]
    except Exception: pass
    
    if "," in search_name:
        parts = search_name.split(",", 1)
        if len(parts) == 2:
            flipped = f"{parts[1].strip()} {parts[0].strip()}"
            safe_flipped = flipped.replace('"', '\\"')
            try:
                sparql.setQuery(query_template.format(occupations=occupations_str, name=safe_flipped))
                results = sparql.query().convert()
                if results["results"]["bindings"]: return results["results"]["bindings"][0]["item"]["value"]
            except Exception: pass

    return None

# ==============================================================================
# 4. HLAVNÍ PROCES (RESUMABLE)
# ==============================================================================

def main():
    print("=== FÁZE 4: Link Discovery (RESUMABLE) ===")
    
    try:
        socket.create_connection(("query.wikidata.org", 443), timeout=5)
    except OSError:
        print("[CHYBA] Nelze se připojit k Wikidatům.")
        return

    dataset_path = Path(kagglehub.dataset_download("sujaykapadnis/board-games"))
    csv_file = list(dataset_path.glob("*.csv"))[0]
    df = pd.read_csv(csv_file).fillna("")
    
    agents_with_counts = extract_sorted_agents(df)
    total = len(agents_with_counts)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Výstupní složka: {OUTPUT_DIR}")
    
    if START_FROM_INDEX > 0:
        print(f"[RESUME] Přeskakuji prvních {START_FROM_INDEX} záznamů...")

    found_count = 0
    loop_start_time = time.time()
    
    current_file = None
    
    # Automatický výpočet čísla souboru podle startovní pozice
    # Např. start=5000, batch=500 -> index=10 -> začne souborem links_11.ttl
    batch_index = START_FROM_INDEX // BATCH_SIZE
    
    try:
        for i, (name, count) in enumerate(agents_with_counts):
            
            # --- SKIPPING LOGIKA ---
            if i < START_FROM_INDEX:
                continue

            # --- OTEVÍRÁNÍ SOUBORU ---
            # Otevřeme nový soubor pokud:
            # a) Jsme přesně na hranici batche (i % 500 == 0)
            # b) NEBO jsme právě začali po přeskočení a soubor ještě není otevřený
            if i % BATCH_SIZE == 0 or (i == START_FROM_INDEX and current_file is None):
                if current_file:
                    current_file.close()
                
                batch_index = (i // BATCH_SIZE) + 1
                filename = f"links_{batch_index:02d}.ttl"
                file_path = OUTPUT_DIR / filename
                
                mode = "w"
                if i != START_FROM_INDEX and (i % BATCH_SIZE != 0):
                     mode = "a" 

                current_file = open(file_path, mode, encoding="utf-8")
                
                # Hlavičku píšeme jen pokud je soubor nový nebo prázdný
                if current_file.tell() == 0:
                    current_file.write("@prefix owl: <http://www.w3.org/2002/07/owl#> .\n")
                    current_file.write("@prefix agent: <http://example.org/agent/> .\n\n")
                
                print(f"\n[SYSTEM] Zapisuji do souboru: {filename}")

            local_slug = clean_for_prefix(name)
            if not local_slug: continue
            
            # --- STATISTIKY ---
            current_idx = i + 1
            percent = (current_idx / total) * 100
            
            eta_str = "Kalibruji..."
            # Resetujeme měření času od bodu startu, aby ETA nebyla zmatená
            active_processing_time = time.time() - loop_start_time
            processed_items_count = current_idx - START_FROM_INDEX
            
            if processed_items_count > 20:
                avg = active_processing_time / processed_items_count
                rem = total - current_idx
                eta_str = format_time(avg * rem)

            header = f"[{current_idx}/{total} | {percent:5.1f}% | ETA: {eta_str:<10}]"
            print(f"{header} ({count}x) Hledám: {name:<25}", end="")
            sys.stdout.flush()
            
            # --- DOTAZ ---
            t0 = time.time()
            uri = find_wikidata_uri(name)
            dur = time.time() - t0
            
            if uri:
                print(f" -> ✅ ({dur:.2f}s)")
                if current_file:
                    current_file.write(f"agent:{local_slug} owl:sameAs <{uri}> .\n")
                    current_file.flush()
                found_count += 1
            else:
                print(f" -> ❌ ({dur:.2f}s)")
            
            time.sleep(SLEEP_TIME)

    finally:
        if current_file:
            current_file.close()

    print(f"\n[SUCCESS] Hotovo! V tomto běhu nalezeno: {found_count}")

if __name__ == "__main__":
    main()