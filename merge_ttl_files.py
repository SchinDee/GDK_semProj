from pathlib import Path

# ==============================================================================
# KONFIGURACE
# ==============================================================================

VERSION = "games"
SOURCE_DIR = Path(__file__).parent / "output" / "links_batchES" / f"links_{VERSION}" 
OUTPUT_FILE = Path(__file__).parent / "output" / F"links_{VERSION}_merged_final.ttl"

# ==============================================================================
# MERGE LOGIKA
# ==============================================================================

def main():
    # 1. Najdeme všechny soubory začínající na "links_" a končící ".ttl"
    # Seřadíme je abecedně, aby šly 01, 02, 03 popořadě.
    files = sorted(list(SOURCE_DIR.glob("links_*.ttl")))
    
    if not files:
        print(f"[CHYBA] Ve složce '{SOURCE_DIR}' nebyly nalezeny žádné soubory links_*.ttl")
        return

    print(f"[INFO] Nalezeno {len(files)} souborů ke sloučení.")
    print(f"[INFO] Výstupní soubor: {OUTPUT_FILE}")

    # Vytvoříme/přepíšeme výstupní soubor
    with open(OUTPUT_FILE, "w", encoding="utf-8") as outfile:
        
        prefixes_written = False
        
        prefixes_already_written = False
        
        for file_path in files:
            print(f" -> Zpracovávám: {file_path.name}")
            
            with open(file_path, "r", encoding="utf-8") as infile:
                for line in infile:
                    stripped = line.strip()
                    
                    # 1. Ignorujeme prázdné řádky ze zdrojových souborů
                    if not stripped:
                        continue
                    
                    is_prefix = stripped.startswith("@prefix")
                    
                    if is_prefix:
                        # Prefix zapíšeme jen pokud jsme to ještě neudělali (z 1. souboru)
                        if not prefixes_written:
                            outfile.write(line)
                    else:
                        # 2. DETEKCE PŘECHODU: Pokud to není prefix a ještě jsme neuzavřeli hlavičku
                        if not prefixes_written:
                            outfile.write("\n") # <--- TADY VRACÍME TU MEZERU
                            prefixes_written = True
                        
                        # Zápis dat
                        outfile.write(line)

    print(f"\n[SUCCESS] Hotovo! Vše sloučeno do '{OUTPUT_FILE.name}'")

if __name__ == "__main__":
    main()