# ==============================================================================
# 0. IMPORTY
# ==============================================================================

import pandas as pd
from urllib.parse import quote
from pathlib import Path
import json
import re
import kagglehub
import html

# ==============================================================================
# 1. KONFIGURACE A CESTY
# ==============================================================================

version = "final"

script_dir = Path(__file__).parent.resolve()
output_file = script_dir / "output" / f"boardgames_{version}.ttl"

# ==============================================================================
# 2. POMOCNÉ FUNKCE PRO ČIŠTĚNÍ DAT
# ==============================================================================

def clean_for_prefix(text):
    """
    Převede vstupní text na formát bezpečný pro Turtle URI.
    
    Změny:
    - Řeší horní indexy (² -> 2, ³ -> 3).
    - Řeší zlomky (½ -> 1_2).
    - Ořezává i pomlčky na začátku/konci.
    """
    if pd.isna(text) or text == "": return ""
    
    s = str(text)
    
    # 1. Specifické firemní přípony
    s = s.replace(", Inc", " Inc").replace(", Ltd", " Ltd").replace(", LLC", " LLC")
    
    # 2. Náhrada znaků
    s = s.replace(" / ", "-").replace("/", "-")
    s = s.replace("&", "and")
    
    # --- OPRAVA INDEXŮ A ZLOMKŮ ---
    s = s.replace("²", "2").replace("³", "3")
    s = s.replace("½", "1_2")  # Např. "War ½" -> "War_1_2"
    
    # 3. Whitelist filtrování
    # \w bere i Azbuku, Čínštinu, Diakritiku...
    s = re.sub(r'[^\w-]', '_', s)
    
    # 4. Redukce vícenásobných podtržítek
    s = re.sub(r'_+', '_', s)
    
    # 5. Ořez (strip) - i pomlčky, aby nevzniklo "-Hra"
    s = s.strip('_').strip('-')
    
    if not s:
        return "unknown"
        
    return s

def clean_html_text(text):
    """
    Původní jednoduchá verze.
    Neřeší 'rozsypaný čaj' (mojibake) ani cizí jazyky.
    """
    if pd.isna(text) or text == "":
        return ""
    
    text = html.unescape(str(text))
    
    return text.strip()

def process_list_to_prefix_format(raw_text):
    """
    Zpracuje textový řetězec obsahující seznam (oddělený čárkami) na seznam bezpečných slugů.
    Používá funkci clean_for_prefix pro každou položku.
    """
    if pd.isna(raw_text) or raw_text == "" or str(raw_text).lower() == "nan":
        return []
    
    text = str(raw_text).replace(", Inc", " Inc").replace(", Ltd", " Ltd").replace(", LLC", " LLC")
    
    items = text.split(',')
    cleaned_slugs = []
    for item in items:
        item = item.strip()
        if not item: continue
        
        safe_slug = clean_for_prefix(item)
        if safe_slug:
            cleaned_slugs.append(safe_slug)
            
    return cleaned_slugs

def clean_literal_list(raw_text):
    """
    Pomocná funkce pro prosté rozdělení řetězce podle čárek bez složité normalizace.
    """
    if pd.isna(raw_text) or raw_text == "": return []
    return [x.strip() for x in str(raw_text).split(',') if x.strip()]

# ==============================================================================
# 3. NAČTENÍ A PŘÍPRAVA DATASETU
# ==============================================================================

print("[INFO] Stahuji dataset z Kaggle…")

dataset_path = Path(
    kagglehub.dataset_download("sujaykapadnis/board-games")
)

print(f"[INFO] Dataset uložen v: {dataset_path}")

csv_files = list(dataset_path.glob("*.csv"))

if not csv_files:
    raise FileNotFoundError("V datasetu nebyl nalezen žádný CSV soubor.")

csv_path = csv_files[0] 
print(f"[INFO] Používám CSV: {csv_path.name}")

try:
    df = pd.read_csv(csv_path).fillna("")
    df['sort_id'] = pd.to_numeric(df['game_id'], errors='coerce')
    df = df.sort_values('sort_id')
    print(f"[INFO] Načteno {len(df)} řádků.")
except Exception as e:
    print(f"[ERROR] {e}")
    exit()

# ==============================================================================
# 4. GENEROVÁNÍ TURTLE (.ttl) SOUBORU
# ==============================================================================

# Definice pořadí predikátů pro konzistentní výstup
PROPERTY_ORDER = [
    "schema:name", "schema:description", "schema:datePublished",
    "bgg:minPlayers", "bgg:maxPlayers", 
    "bgg:minPlaytime", "bgg:maxPlaytime", "bgg:playingTime", "bgg:minAge",
    "schema:author", "schema:contributor", "schema:publisher", 
    "schema:genre", "schema:isPartOf", "schema:partOfSeries", 
    "bgg:hasMechanic", "bgg:hasExpansion",
    "bgg:ratingValue", "bgg:ratingCount"
]

with open(output_file, "w", encoding="utf-8") as f:
    
    # ---------------------------------------------------------
    # A) Zápis hlavičky a prefixů
    # ---------------------------------------------------------
    f.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n")
    f.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n")
    f.write("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n")
    f.write("@prefix schema: <http://schema.org/> .\n")
    f.write("@prefix bgg: <http://example.org/ontology/> .\n\n")
    
    f.write("@prefix game: <http://example.org/game/> .\n")
    f.write("@prefix agent: <http://example.org/agent/> .\n")
    f.write("@prefix category: <http://example.org/category/> .\n")
    f.write("@prefix mechanic: <http://example.org/mechanic/> .\n")
    f.write("@prefix family: <http://example.org/family/> .\n")
    f.write("@prefix comp: <http://example.org/compilation/> .\n")
    f.write("@prefix exp: <http://example.org/expansion/> .\n\n")
    
    count = 0
    total = len(df)
    
    # ---------------------------------------------------------
    # B) Iterace přes řádky datasetu
    # ---------------------------------------------------------
    for _, row in df.iterrows():
        game_id = row['game_id']
        
        subject_uri = f"game:{game_id}"
        
        # Dočasný kontejner pro data aktuální hry (klíč = predikát, hodnota = seznam objektů)
        data_bucket = {}
        def add(predicate, val_str):
            if predicate not in data_bucket: data_bucket[predicate] = []
            data_bucket[predicate].append(val_str)

        # 1. Zpracování literálů (název, popis, rok)
        if row['name']: 
            clean_name = clean_html_text(row['name'])
            add("schema:name", json.dumps(clean_name, ensure_ascii=False))
            
        if row['description']: 
            clean_desc = clean_html_text(row['description'])
            add("schema:description", json.dumps(clean_desc, ensure_ascii=False))
            
        try:
            val = str(int(float(row['year_published'])))
            add("schema:datePublished", f'"{val}"^^xsd:gYear')
        except: pass

        # 2. Zpracování numerických metrik
        for col, pred in [('min_players', 'bgg:minPlayers'), ('max_players', 'bgg:maxPlayers'),
                          ('min_playtime', 'bgg:minPlaytime'), ('max_playtime', 'bgg:maxPlaytime'),
                          ('playing_time', 'bgg:playingTime'), ('min_age', 'bgg:minAge')]:
            try:
                # ZMĚNA: Přidány uvozovky kolem čísla: "{...}"^^xsd:integer
                if float(row[col]) > 0: 
                    add(pred, f'"{int(float(row[col]))}"^^xsd:integer')
            except: pass

        # 3. Zpracování seznamů a vazeb na entity (pomocí prefixů)
        for slug in process_list_to_prefix_format(row['artist']): 
            add("schema:contributor", f"agent:{slug}")
            
        for slug in process_list_to_prefix_format(row['designer']): 
            add("schema:author", f"agent:{slug}")
            
        for slug in process_list_to_prefix_format(row['publisher']): 
            add("schema:publisher", f"agent:{slug}")
            
        for slug in process_list_to_prefix_format(row['category']): 
            add("schema:genre", f"category:{slug}")
            
        for slug in process_list_to_prefix_format(row['mechanic']): 
            add("bgg:hasMechanic", f"mechanic:{slug}")
        
        for slug in process_list_to_prefix_format(row['family']): 
            add("schema:partOfSeries", f"family:{slug}")
            
        for slug in process_list_to_prefix_format(row['compilation']): 
            add("schema:isPartOf", f"comp:{slug}")
        
        for slug in process_list_to_prefix_format(row['expansion']): 
            add("bgg:hasExpansion", f"exp:{slug}")

        # 4. Zpracování hodnocení
        try:
            # ZMĚNA: Přidány uvozovky kolem hodnot
            if row['average_rating']: 
                add("bgg:ratingValue", f'"{float(row["average_rating"])}"^^xsd:decimal')
            if row['users_rated']: 
                add("bgg:ratingCount", f'"{int(float(row["users_rated"]))}"^^xsd:integer')
        except: pass

        # ---------------------------------------------------------
        # C) Zápis subjektu do souboru
        # ---------------------------------------------------------
        if not data_bucket:
            f.write(f"{subject_uri} a schema:Game .\n\n")
        else:
            f.write(f"{subject_uri} a schema:Game ;\n")
        
        valid_keys = [k for k in PROPERTY_ORDER if k in data_bucket]
        
        for i, key in enumerate(valid_keys):
            values_list = data_bucket[key]
            
            # Výpočet odsazení pro formátování více hodnot
            indent_len = 4 + len(key) + 1
            indent_str = "\n" + (" " * indent_len)
            
            values_joined = ("," + indent_str).join(values_list)
            
            terminator = "." if (i == len(valid_keys) - 1) else ";"
            
            f.write(f"    {key} {values_joined} {terminator}\n")
        
        f.write("\n")
        count += 1
        if count % 100 == 0: print(f"Zpracováno {count}/{total}")

print(f"[SUCCESS] Hotovo. Soubor: {output_file}")