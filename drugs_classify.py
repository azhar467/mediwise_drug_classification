import os
import math
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from datetime import datetime
import logging
from colorama import Fore, Style, init
from collections import Counter
import re
import json

# Init colorama
init(autoreset=True)

# ----------------------------
# CONFIG
# ----------------------------
INPUT_FOLDER = "unclassified_drugs"
OUTPUT_EXCEL_FOLDER = "classified_drugs/excel"
OUTPUT_CSV_FOLDER = "classified_drugs/csv"
LOG_FOLDER = "logs"
METADATA_FILE = os.path.join(LOG_FOLDER, "category_counts.json")  # For tracking category changes

INPUT_FILENAME = None  # Optional: specify filename here
NUM_WORKERS = None

COL_COMP1 = "short_composition1"
COL_COMP2 = "short_composition2"

CATEGORY_KEYWORDS = {
    "Antibiotics": [
        "amoxicillin", "amoxycillin", "azithromycin", "ciprofloxacin", "doxycycline",
        "cef", "clavulanic", "levofloxacin", "ceftriaxone", "cefixime", "gentamicin",
        "amikacin", "clindamycin", "linezolid", "meropenem", "piperacillin", "tazobactam",
        "co-trimoxazole", "sulfamethoxazole", "ofloxacin", "ornidazole",
        "moxifloxacin", "roxithromycin", "ampicillin", "norfloxacin", "metronidazole",
        "clarithromycin", "rifaximin"
    ],
    "Antivirals": [
        "oseltamivir", "acyclovir", "favipiravir", "remdesivir",
        "arteether", "artesunate", "artemether"
    ],
    "Antifungals": [
        "fluconazole", "itraconazole", "clotrimazole", "miconazole", "ketoconazole",
        "terbinafine", "luliconazole", "povidone", "iodine", "tacrolimus"
    ],
    "Pain_Inflammation": [
        "ibuprofen", "diclofenac", "naproxen", "aceclofenac", "paracetamol", "mefenamic",
        "ketorolac", "tramadol", "nimesulide", "etoricoxib", "diacerein", "glucosamine"
    ],
    "Fever_Cold_Cough": [
        "paracetamol", "chlorpheniramine", "phenylephrine", "cetirizine", "montelukast",
        "fexofenadine", "pheniramine", "pseudoephedrine", "guaifenesin", "doxylamine",
        "diphenhydramine"
    ],
    "Respiratory_Asthma": [
        "salbutamol", "levosalbutamol", "formoterol", "budesonide", "tiotropium",
        "salmeterol", "acebrophylline"
    ],
    "Cardiac_Hypertension": [
        "amlodipine", "atenolol", "metoprolol", "losartan", "telmisartan", "ramipril",
        "enalapril", "clopidogrel", "aspirin", "bisoprolol", "atorvastatin",
        "rosuvastatin", "olmesartan", "medoxomil", "cilnidipine", "propranolol",
        "minoxidil", "torasemide"
    ],
    "Diabetes": [
        "metformin", "glimepiride", "gliclazide", "sitagliptin", "vildagliptin",
        "dapagliflozin", "insulin", "pioglitazone", "voglibose", "teneligliptin"
    ],
    "Gastrointestinal": [
        "pantoprazole", "omeprazole", "rabeprazole", "ranitidine", "domperidone",
        "ondansetron", "lansoprazole", "loperamide", "ursodeoxycholic", "sucralfate",
        "tricholine", "simethicone", "pyridoxine", "lactulose"
    ],
    "Vitamins_Supplements": [
        "vitamin d", "cholecalciferol", "vitamin b12", "folic", "iron", "ferrous",
        "calcium", "multivitamin", "zinc", "magnesium", "methylcobalamin", "natural",
        "potassium"
    ],
    "Steroids_Hormones": [
        "prednisolone", "dexamethasone", "hydrocortisone", "beclometasone", "testosterone",
        "levothyroxine", "thyroxine", "deflazacort", "progesterone", "nandrolone",
        "decanoate", "micronized", "mometasone", "dutasteride"
    ],
    "Dermatology": [
        "clobetasol", "mupirocin", "hydroquinone", "retinol", "adapalene", "betamethasone",
        "terbinafine topical", "carboxymethylcellulose", "permethrin"
    ],
    "Neurology_Psychiatry": [
        "sertraline", "fluoxetine", "clonazepam", "alprazolam", "gabapentin", "valproate",
        "carbamazepine", "escitalopram", "amitriptyline", "pregabalin", "nortriptyline",
        "piracetam", "olanzapine", "betahistine", "levetiracetam", "risperidone",
        "divalproex", "trihexyphenidyl", "flunarizine", "citicoline", "hydroxyzine",
        "etizolam", "quetiapine", "amisulpride", "chlordiazepoxide", "paroxetine"
    ],
    "Oncology": ["letrozole", "tamoxifen", "imatinib", "gefitinib", "cisplatin", "doxorubicin"],
    "Antiparasitics": ["albendazole"],
    "Enzymes": ["trypsin", "bromelain", "serratiopeptidase"],
    "Appetite_Stimulants": ["cyproheptadine"],
    "Uric_Acid_Control": ["febuxostat"],
    "Muscle_Relaxants": ["thiocolchicoside"],
    "Hemostatics": ["tranexamic"],
    "Erectile_Dysfunction": ["sildenafil"],
    "Hair_Growth": ["minoxidil"],
    "Immunosuppressants": ["tacrolimus"],
    "Tuberculosis": ["isoniazid"]
}

def find_input_file():
    if INPUT_FILENAME:
        path = os.path.join(INPUT_FOLDER, INPUT_FILENAME)
        if os.path.exists(path):
            return path
        raise FileNotFoundError(f"Configured input file not found: {path}")
    for f in os.listdir(INPUT_FOLDER):
        if f.lower().endswith(".xlsx") or f.lower().endswith(".csv"):
            return os.path.join(INPUT_FOLDER, f)
    raise FileNotFoundError(f"No .xlsx or .csv found in input folder '{INPUT_FOLDER}'")

def classify_row_text(combined_text):
    if not isinstance(combined_text, str):
        combined_text = ""
    text = combined_text.lower()
    matched_categories = []
    for cat, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            matched_categories.append(cat)
    return "Others" if not matched_categories else " + ".join(dict.fromkeys(matched_categories))

def classify_df_chunk(df_chunk):
    comp1 = df_chunk.get(COL_COMP1, pd.Series([""] * len(df_chunk)))
    comp2 = df_chunk.get(COL_COMP2, pd.Series([""] * len(df_chunk)))
    combined = comp1.fillna("").astype(str) + " " + comp2.fillna("").astype(str)
    df_chunk = df_chunk.copy()
    df_chunk["Category"] = combined.map(classify_row_text)
    for idx, row in df_chunk.iterrows():
        logging.info(f"Processed ID: {row.get('id', 'N/A')} | Name: {row.get('name', 'N/A')} | Category: {row['Category']}")
        print(Fore.GREEN + f"Processed ID: {row.get('id', 'N/A')} | Name: {row.get('name', 'N/A')} | Category: {row['Category']}")
    return df_chunk

def get_next_version(folder, base_name):
    existing = [f for f in os.listdir(folder) if base_name in f]
    return len(existing) + 1

def extract_others_keywords(df):
    others_df = df[df["Category"] == "Others"]
    text_data = (others_df[COL_COMP1].fillna("").astype(str) + " " + others_df[COL_COMP2].fillna("").astype(str)).str.lower()
    words = re.findall(r'\b[a-zA-Z]{4,}\b', " ".join(text_data))
    return pd.DataFrame(Counter(words).most_common(), columns=["Keyword", "Count"])

def compare_with_previous(current_counts):
    if not os.path.exists(METADATA_FILE):
        return {}
    with open(METADATA_FILE, "r") as f:
        prev_counts = json.load(f)
    changes = {}
    for category, count in current_counts.items():
        prev_count = prev_counts.get(category, 0)
        diff = count - prev_count
        if diff != 0:
            changes[category] = diff
    return changes

def save_current_counts(current_counts):
    os.makedirs(LOG_FOLDER, exist_ok=True)
    with open(METADATA_FILE, "w") as f:
        json.dump(current_counts, f)

def main():
    os.makedirs(OUTPUT_EXCEL_FOLDER, exist_ok=True)
    os.makedirs(OUTPUT_CSV_FOLDER, exist_ok=True)
    os.makedirs(LOG_FOLDER, exist_ok=True)

    start_time = datetime.now()
    version = get_next_version(OUTPUT_CSV_FOLDER, "mediwise_classified")
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")

    log_file = os.path.join(LOG_FOLDER, f"log_v{version}_{timestamp}.txt")
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(message)s")

    print(Fore.CYAN + f"===== Script Started at {start_time} =====")
    logging.info("===== Script Started =====")

    input_path = find_input_file()
    print(Fore.YELLOW + f"Input file: {input_path}")
    logging.info(f"Input file: {input_path}")

    df = pd.read_excel(input_path, engine="openpyxl") if input_path.lower().endswith(".xlsx") else pd.read_csv(input_path)

    cpu = NUM_WORKERS or max(1, multiprocessing.cpu_count() - 1)
    n_chunks = cpu * 4
    chunk_size = math.ceil(len(df) / n_chunks)
    df_chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

    with ProcessPoolExecutor(max_workers=cpu) as executor:
        results = list(executor.map(classify_df_chunk, df_chunks))

    df_out = pd.concat(results, ignore_index=True)

    current_counts = df_out["Category"].value_counts().to_dict()
    changes = compare_with_previous(current_counts)
    save_current_counts(current_counts)

    if changes:
        print(Fore.MAGENTA + "Category count changes since last run:")
        for cat, diff in changes.items():
            change_symbol = "+" if diff > 0 else ""
            print(Fore.MAGENTA + f"{cat}: {change_symbol}{diff}")
            logging.info(f"{cat}: {change_symbol}{diff}")

    csv_out = os.path.join(OUTPUT_CSV_FOLDER, f"mediwise_classified_v{version}_{timestamp}.csv")
    df_out.to_csv(csv_out, index=False)

    excel_out = os.path.join(OUTPUT_EXCEL_FOLDER, f"mediwise_classified_v{version}_{timestamp}.xlsx")
    with pd.ExcelWriter(excel_out, engine="xlsxwriter") as writer:
        df_out.to_excel(writer, sheet_name="Master", index=False)
        for cat in sorted(df_out["Category"].unique()):
            df_cat = df_out[df_out["Category"] == cat]
            df_cat.to_excel(writer, sheet_name=cat[:31], index=False)
            if cat == "Others":
                workbook = writer.book
                worksheet = writer.sheets[cat[:31]]
                yellow_format = workbook.add_format({'bg_color': '#FFFF99'})
                nrows, ncols = df_cat.shape
                worksheet.set_column(0, ncols - 1, None, yellow_format)

    summary_out = os.path.join(OUTPUT_EXCEL_FOLDER, f"classification_summary_v{version}_{timestamp}.xlsx")
    summary = pd.DataFrame(list(current_counts.items()), columns=["Category", "Count"])
    summary["Percentage"] = (summary["Count"] / len(df_out) * 100).round(2)
    others_keywords_df = extract_others_keywords(df_out)

    with pd.ExcelWriter(summary_out, engine="xlsxwriter") as writer:
        summary.to_excel(writer, sheet_name="Summary", index=False)
        others_keywords_df.to_excel(writer, sheet_name="Others_Keywords", index=False)

    end_time = datetime.now()
    print(Fore.CYAN + f"===== Script Finished at {end_time} (Time taken: {end_time - start_time}) =====")
    logging.info(f"Script Finished. Time taken: {end_time - start_time}")

if __name__ == "__main__":
    main()
