# %% [markdown]
# 
# <div style="text-align: center; background-color:#830707ff; font-family:'Times New Roman'; 
#             color: white; padding: 14px; line-height: 1.4; border-radius:20px">
# Pipeline ETL : Conversion, stockage PostgreSQL et modélisation en étoile
# </div>
# 

# %% [markdown]
# <div style="text-align: center; background-color:#257e2aff; font-family:'Times New Roman'; 
#             color: white; padding: 14px; line-height: 1.4; border-radius:20px">
# 1. Conversion du fichier JSON en CSV
# </div>
# 

# %%
import pandas as pd
import json

# === 1. Charger le fichier JSON ===
with open(r"result_for_query_purchase_order3.json", "r", encoding="utf-8") as f:
    data = json.load(f)

rows = data["data"]["biztransactions"]["nodes"]

# === 2. Fonction pour explorer les clés dans l'ordre ===
def explore_value_keys(obj, prefix=""):
    keys = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_prefix = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (str, int, float, bool)) or v is None:
                keys.append(new_prefix)
            else:
                keys.extend(explore_value_keys(v, new_prefix))
    elif isinstance(obj, list):
        for item in obj:
            keys.extend(explore_value_keys(item, prefix))
    return keys

# === 3. Récupérer toutes les colonnes dans l'ordre d'apparition ===
ordered_columns = []
for row in rows:
    for key in explore_value_keys(row):
        if key not in ordered_columns:
            ordered_columns.append(key)

print("Colonnes détectées dans l'ordre :", ordered_columns)

# === 4. Fonction générique pour extraire les valeurs ===
def extract_nested_values(data, path):
    results = []

    def recurse(node, keys):
        if not keys:
            # Si valeur simple → on ajoute directement
            if isinstance(node, (str, int, float, bool)):
                results.append(node)   
            return
        key = keys[0]
        if isinstance(node, dict) and key in node:
            recurse(node[key], keys[1:])
        elif isinstance(node, list):
            for item in node:
                recurse(item, keys)

    recurse(data, path)

    
    # - Si plusieurs valeurs -> on garde une liste
    # - Si une seule valeur -> on garde la valeur directement
    if not results:
        return None
    elif len(results) == 1:
        return results[0]
    else:
        return results   # ← garde une vraie liste Python

# === 5. Construire le tableau avec les colonnes dans l'ordre ===
records = []
for row in rows:
    record = {}
    for col in ordered_columns:
        path = col.split(".")
        record[col] = extract_nested_values(row, path)
    records.append(record)

df = pd.DataFrame(records, columns=ordered_columns)
# === 6. Explosion automatique des colonnes qui contiennent des listes ===
for col in df.columns:
    if df[col].apply(lambda x: isinstance(x, list)).any():
        df = df.explode(col, ignore_index=True)


print(f"✅ Export terminé : {len(ordered_columns)} colonnes exportées dans l'ordre JSON")

# === 6. Export CSV
# les colonnes avec des listes seront écrites comme "[a, b, c]" dans le CSV
df.to_csv(r"purchase_orders_full_flat37 ordered.csv",
          sep=";", index=False, encoding="utf-8")

# %% [markdown]
# <div style="text-align: center; background-color:#257e2aff; font-family:'Times New Roman'; 
#             color: white; padding: 14px; line-height: 1.4; border-radius:20px">
# 2. Anonymisation des données
# </div>

# %%
import pandas as pd
from faker import Faker
import os

# --- Faker en anglais  ---
fake = Faker("en_US")

# --- Fichiers ---
INPUT_CSV  = r"purchase_orders_full_flat37 ordered.csv"
OUTPUT_CSV = r"purchase_orders_full_flat37_anonymized.csv"
MAPPING_CSV = r"anonymization_mapping.csv"
ID_MAPPING_CSV = r"id_mapping.csv"

# --- Colonnes à anonymiser (renommées en X1, X2, ...) ---
COLS = [
    "X1",
    "X2",
    "X3",
    "X4",
    "X5",
    "X6",
]

# --- Colonnes identifiants (renommées en X7, X8, ...) ---
ID_COLS = [
    "X7",
    "X8",
    "X9",
]

# --- Charger mapping existant si présent ---
ANONYMIZED_VALUES = {}
if os.path.exists(MAPPING_CSV):
    old_map = pd.read_csv(MAPPING_CSV, dtype=str)
    for _, row in old_map.iterrows():
        ANONYMIZED_VALUES[row["original"]] = row["anonymized"]

# --- Charger mapping identifiants ---
ID_MAP = {}
ID_COUNTER = {"PO": 0, "GTIN": 0, "HS": 0}
if os.path.exists(ID_MAPPING_CSV):
    idmap_df = pd.read_csv(ID_MAPPING_CSV, dtype=str)
    for _, r in idmap_df.iterrows():
        ID_MAP[r["original"]] = {"fake": r["fake"], "type": r["id_type"]}
        # Mise à jour compteur
        if r["id_type"] in ID_COUNTER:
            try:
                num = int(r["fake"].split("_")[-1])
                ID_COUNTER[r["id_type"]] = max(ID_COUNTER[r["id_type"]], num)
            except:
                pass

# --- Fonctions ---
def fake_value(original, col):
    """Anonymisation des valeurs (entreprises, contacts, etc.)"""
    if pd.isna(original) or str(original).strip() == "":
        return ""   # garder vide si vide à l'origine

    if original not in ANONYMIZED_VALUES:
        if "X2" in col or "X6" in col:
            new_val = fake.company()
        elif "X1" in col or "X5" in col:
            new_val = fake.company() + " Manufacturing"
        elif "X4" in col:
            new_val = fake.name()
        else:
            new_val = fake.word().capitalize()
        ANONYMIZED_VALUES[original] = new_val

    return ANONYMIZED_VALUES[original]


def _fmt(seq, width=6):
    return str(seq).zfill(width)

def anonymize_id_value(original, col):
    """Anonymisation des identifiants (transactions, produits, codes)"""
    if original is None or str(original).strip() in ["", "nan", "NaN", "null", "None"]:
        return ""

    orig = str(original).strip()
    if orig in ID_MAP:
        return ID_MAP[orig]["fake"]

    if col == "X7":
        ID_COUNTER["PO"] += 1
        fakev = f"PO_{_fmt(ID_COUNTER['PO'])}"
        id_type = "PO"
    elif "X8" in col:
        ID_COUNTER["GTIN"] += 1
        fakev = f"FAKE_PRODUIT_{_fmt(ID_COUNTER['GTIN'])}"
        id_type = "GTIN"
    elif "X9" in col:
        ID_COUNTER["HS"] += 1
        fakev = f"FAKE_CODE_{_fmt(ID_COUNTER['HS'])}"
        id_type = "HS"
    else:
        fakev = f"FAKE_{hash(orig) % 1000000}"
        id_type = "OTHER"

    ID_MAP[orig] = {"fake": fakev, "type": id_type}
    return fakev

# --- Traitement par chunks ---
chunksize = 100_000
first = True

for chunk in pd.read_csv(INPUT_CSV, sep=";", dtype=str, chunksize=chunksize, low_memory=False, encoding="utf-8"):
    # anonymiser noms
    for col in COLS:
        if col in chunk.columns:
            chunk[col] = chunk[col].apply(lambda x: fake_value(x, col))

    # anonymiser identifiants
    for col in ID_COLS:
        if col in chunk.columns:
            chunk[col] = chunk[col].apply(lambda x: anonymize_id_value(x, col))

    # écrire fichier
    chunk.to_csv(OUTPUT_CSV, sep=";", index=False, encoding="utf-8", mode="w" if first else "a", header=first)
    first = False

# --- Sauvegarde mapping noms ---
map_df = pd.DataFrame([{"original": k, "anonymized": v} for k, v in ANONYMIZED_VALUES.items()])
map_df.to_csv(MAPPING_CSV, index=False, encoding="utf-8")

# --- Sauvegarde mapping identifiants ---
if ID_MAP:
    rows = [{"original": k, "fake": v["fake"], "id_type": v["type"]} for k, v in ID_MAP.items()]
    idmap_df = pd.DataFrame(rows)
    idmap_df.to_csv(ID_MAPPING_CSV, index=False, encoding="utf-8")

print(f"✅ CSV anonymisé écrit : {OUTPUT_CSV}")
print(f"✅ Mapping noms écrit : {MAPPING_CSV}")
print(f"✅ Mapping IDs écrit : {ID_MAPPING_CSV}")


# %% [markdown]
# <div style="text-align: center; background-color:#257e2aff; font-family:'Times New Roman'; 
#             color: white; padding: 14px; line-height: 1.4; border-radius:20px">
# 3. Échantillonnage des données
# </div>

# %%
import pandas as pd

INPUT_CSV = r"purchase_orders_full_flat37_anonymized.csv"
OUTPUT_CSV = r"purchase_orders_sample_random.csv"

# Nombre de lignes à garder
N = 100000   # tu peux mettre 5000 ou 20000 si tu veux

# Charger seulement N lignes
df = pd.read_csv(INPUT_CSV, sep=";", nrows=N)

# Sauvegarder l'échantillon
df.to_csv(OUTPUT_CSV, sep=";", index=False, encoding="utf-8")

print(f"✅ Échantillon de {N} lignes créé : {OUTPUT_CSV}")



# %% [markdown]
# <div style="text-align: center; background-color:#257e2aff; font-family:'Times New Roman';
#             color: white; padding: 14px; line-height: 1.4; border-radius:20px;">
# 4. Création de la base Projet sur PostgreSQL et chargement des données
# </div>

# %%
import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Charger les variables depuis le fichier .env
load_dotenv()

# === 1. Charger le fichier CSV ===
csv_file = r"purchase_orders_sample_random.csv"
df = pd.read_csv(csv_file, sep=";")

# Colonnes numériques (renommées en X1, X2, ...)
numeric_cols = [
    "X1",
    "X2",
    "X3",
    "X4",
    "X5",
    "X6",
    "X7"
]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# === 2. Supprimer et recréer la base Projet ===
# AUTOCOMMIT pour DROP/CREATE DATABASE
password = os.getenv("POSTGRES_PASSWORD")

admin_engine = create_engine(
    f"postgresql+psycopg2://postgres:{password}@localhost:5432/postgres",
    isolation_level="AUTOCOMMIT"
)

with admin_engine.begin() as conn:
    # Fermer toutes les connexions actives sur Projet
    conn.execute(text("""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = 'Projet' AND pid <> pg_backend_pid();
    """))
    
    # Supprimer puis recréer la base
    conn.execute(text('DROP DATABASE IF EXISTS "Projet";'))
    conn.execute(text("CREATE DATABASE \"Projet\" WITH ENCODING 'UTF8' TEMPLATE template1;"))

print("🗑️ Base 'Projet' supprimée et recréée avec succès")

# === 3. Connexion à la nouvelle base Projet ===
engine = create_engine(
    f"postgresql+psycopg2://postgres:{password}@localhost:5432/Projet"
)

# === 4. Charger le CSV dans une table de staging ===
df.to_sql("staging_orders", engine, if_exists="replace", index=False)

print("✅ Données CSV importées dans PostgreSQL (staging_orders)")


# %% [markdown]
# <div style="text-align: center; background-color:#257e2aff; font-family:'Times New Roman';
#             color: white; padding: 14px; line-height: 1.4; border-radius:20px;">
# 5. Création des tables pour la modélisation en étoile
# </div>

# %%
# === 4. Créer les tables de dimensions et de faits (version anonymisée) ===
schema_sql = """

DROP TABLE IF EXISTS Fact_Orders CASCADE;
DROP SEQUENCE IF EXISTS fact_orders_id_fact_seq CASCADE;
DROP TABLE IF EXISTS Dim_X1 CASCADE;
DROP TABLE IF EXISTS Dim_X2 CASCADE;
DROP TABLE IF EXISTS Dim_X3 CASCADE;
DROP TABLE IF EXISTS Dim_X4 CASCADE;
DROP TABLE IF EXISTS Dim_X5 CASCADE;
DROP TABLE IF EXISTS Dim_X6 CASCADE;
DROP TABLE IF EXISTS Dim_X7 CASCADE;

-- Table X1
CREATE TABLE IF NOT EXISTS Dim_X1 (
    id_x1 SERIAL PRIMARY KEY,
    "X1" TEXT,
    "X2" TEXT,
    "X3" TEXT
);

-- Table X2
CREATE TABLE IF NOT EXISTS Dim_X2 (
    id_x2 SERIAL PRIMARY KEY,
    "X4" TEXT,
    "X5" TEXT,
    "X6" TEXT,
    "X7" TEXT,
    "X8" TEXT,
    "X9" TEXT,
    "X10" TEXT,
    "X11" TEXT,
    "X12" TEXT,
    "X13" TEXT
);

-- Table X3
CREATE TABLE IF NOT EXISTS Dim_X3 (
    id_x3 SERIAL PRIMARY KEY,
    "X14" TEXT,
    "X15" TEXT,
    "X16" TEXT,
    "X17" TEXT
);

-- Table X4
CREATE TABLE IF NOT EXISTS Dim_X4 (
    id_x4 SERIAL PRIMARY KEY,
    "X18" TEXT,
    "X19" TEXT,
    "X20" TEXT
);

-- Table X5
CREATE TABLE IF NOT EXISTS Dim_X5 (
    id_x5 SERIAL PRIMARY KEY,
    "X21" TEXT,
    "X22" TEXT
);

-- Table X6
CREATE TABLE IF NOT EXISTS Dim_X6 (
    id_x6 SERIAL PRIMARY KEY,
    "X23" TEXT
);

-- Table X7
CREATE TABLE IF NOT EXISTS Dim_X7 (
    id_x7 SERIAL PRIMARY KEY,
    "X24" TEXT,
    "X25" TEXT,
    "X26" TEXT
);

-- Table des faits
CREATE TABLE IF NOT EXISTS Fact_Orders (
    id_fact SERIAL PRIMARY KEY,
    "X27" TEXT,
    "X28" TEXT,
    id_x1 INT REFERENCES Dim_X1(id_x1),
    id_x2 INT REFERENCES Dim_X2(id_x2),
    id_x3 INT REFERENCES Dim_X3(id_x3),
    id_x4 INT REFERENCES Dim_X4(id_x4),
    id_x5 INT REFERENCES Dim_X5(id_x5),
    id_x6 INT REFERENCES Dim_X6(id_x6),
    id_x7 INT REFERENCES Dim_X7(id_x7),
    "X29" NUMERIC,
    "X30" NUMERIC,
    "X31" NUMERIC,
    "X32" NUMERIC,
    "X33" NUMERIC,
    "X34" NUMERIC,
    "X35" NUMERIC
);

"""

with engine.connect() as conn:
    conn.execute(text(schema_sql))
    conn.commit()

print("✅ Tables de dimensions et de faits créées dans PostgreSQL")


# %% [markdown]
# <div style="text-align: center; background-color:#257e2aff; font-family:'Times New Roman';
#             color: white; padding: 14px; line-height: 1.4; border-radius:20px;">
# 5. Alimentation des tables avec le fichier de base
# </div>

# %%
## === 5. Alimentation des tables de dimensions (version anonymisée) ===
with engine.begin() as conn:
    # Dim_X1
    conn.execute(text("""
        INSERT INTO Dim_X1 ("X1", "X2", "X3")
        SELECT DISTINCT "X1", "X2", "X3"
        FROM staging_orders
        WHERE "X1" IS NOT NULL;
    """))

    # Dim_X2
    conn.execute(text("""
        INSERT INTO Dim_X2 ("X4", "X5", "X6", "X7", "X8", "X9", "X10", "X11", "X12", "X13")
        SELECT DISTINCT "X4", "X5", "X6", "X7", "X8", "X9", "X10", "X11", "X12", "X13"
        FROM staging_orders;
    """))

    # Dim_X3
    conn.execute(text("""
        INSERT INTO Dim_X3 ("X14", "X15", "X16", "X17")
        SELECT DISTINCT "X14", "X15", "X16", "X17"
        FROM staging_orders;
    """))

    # Dim_X4
    conn.execute(text("""
        INSERT INTO Dim_X4 ("X18", "X19", "X20")
        SELECT DISTINCT "X18", "X19", "X20"
        FROM staging_orders;
    """))

    # Dim_X5
    conn.execute(text("""
        INSERT INTO Dim_X5 ("X21", "X22")
        SELECT DISTINCT "X21", "X22"
        FROM staging_orders;
    """))

    # Dim_X6
    conn.execute(text("""
        INSERT INTO Dim_X6 ("X23")
        SELECT DISTINCT "X23"
        FROM staging_orders;
    """))

    # Dim_X7
    conn.execute(text("""
        INSERT INTO Dim_X7 ("X24", "X25", "X26")
        SELECT DISTINCT "X24", "X25", "X26"
        FROM staging_orders;
    """))

print("✅ Dimensions alimentées avec succès")


# === 6. Alimentation de la table des faits (version anonymisée) ===
with engine.begin() as conn:
    conn.execute(text("""
        INSERT INTO Fact_Orders (
            "X27", "X28",
            id_x1, id_x2, id_x3, id_x4, id_x5, id_x6, id_x7,
            "X29", "X30",
            "X31", "X32",
            "X33", "X34",
            "X35"
        )
        SELECT
            staging_orders."X27",
            staging_orders."X28",
            Dim_X1.id_x1,
            Dim_X2.id_x2,
            Dim_X3.id_x3,
            Dim_X4.id_x4,
            Dim_X5.id_x5,
            Dim_X6.id_x6,
            Dim_X7.id_x7,
            staging_orders."X29",
            staging_orders."X30",
            staging_orders."X31",
            staging_orders."X32",
            staging_orders."X33",
            staging_orders."X34",
            staging_orders."X35"
        FROM staging_orders
        LEFT JOIN Dim_X1 ON staging_orders."X1" = Dim_X1."X1"
        LEFT JOIN Dim_X2 ON staging_orders."X4" = Dim_X2."X4"
        LEFT JOIN Dim_X3 ON staging_orders."X14" = Dim_X3."X14"
        LEFT JOIN Dim_X4 ON staging_orders."X18" = Dim_X4."X18"
        LEFT JOIN Dim_X5 ON staging_orders."X21" = Dim_X5."X21"
        LEFT JOIN Dim_X6 ON staging_orders."X23" = Dim_X6."X23"
        LEFT JOIN Dim_X7 ON staging_orders."X24" = Dim_X7."X24";
    """))

print("✅ Table des faits alimentée avec succès")



