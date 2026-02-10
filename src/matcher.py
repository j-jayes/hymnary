import pandas as pd
import re
from pathlib import Path

def normalize_title(title):
    if not isinstance(title, str):
        return ""
    # Remove non-alphanumeric characters and convert to uppercase
    return re.sub(r'[^A-Z0-9]', '', title.upper())

def main():
    # Paths
    raw_mp_path = Path("data/raw/MP HymnTuneNames.csv")
    processed_path = Path("data/processed/summary_filtered.csv")
    output_path = Path("data/processed/organ_book_matches.csv")

    if not raw_mp_path.exists():
        print(f"Error: {raw_mp_path} not found.")
        return
    if not processed_path.exists():
        print(f"Error: {processed_path} not found.")
        return

    # Load Data
    try:
        try:
            df_mp = pd.read_csv(raw_mp_path, encoding='utf-8')
        except UnicodeDecodeError:
            df_mp = pd.read_csv(raw_mp_path, encoding='latin-1')
            
        df_organ = pd.read_csv(processed_path)
    except Exception as e:
        print(f"Error reading CSVs: {e}")
        return

    # Validate Columns
    if "HymnTuneName" not in df_mp.columns:
        print("Error: 'HymnTuneName' column missing in Mission Praise data.")
        return
    
    # Check for 'tune_title' or 'title' in organ data
    title_col = "tune_title"
    if title_col not in df_organ.columns:
        if "title" in df_organ.columns:
            title_col = "title"
        else:
            print("Error: 'tune_title' or 'title' column missing in organ data.")
            return

    # Normalize - Set A (Book)
    book_tunes_raw = df_mp["HymnTuneName"].dropna().astype(str).unique()
    book_tunes_norm = set([normalize_title(t) for t in book_tunes_raw])
    
    # Normalize - Set B (Organ)
    df_organ["normalized_title"] = df_organ[title_col].astype(str).apply(normalize_title)
    
    # Match Logic
    df_organ["in_hymn_book"] = df_organ["normalized_title"].apply(lambda x: x in book_tunes_norm if x else False)

    # Stats for Venn Diagram
    total_book_tunes = len(book_tunes_norm)
    matched_tunes_count = df_organ[df_organ["in_hymn_book"] == True]["tune_slug"].nunique()
    total_organ_tunes = df_organ["tune_slug"].nunique()

    print(f"STATS_START")
    print(f"Total Unique Book Tunes: {total_book_tunes}")
    print(f"Total Unique Organ Tunes: {total_organ_tunes}")
    print(f"Overlap (Matched Tunes): {matched_tunes_count}")
    print(f"STATS_END")

    # Save
    df_organ.to_csv(output_path, index=False)
    print(f"Saved results to {output_path}")

if __name__ == "__main__":
    main()
