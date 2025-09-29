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
        return results  

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

# --- Faker 
fake = Faker("en_US")

# --- Fichiers ---
INPUT_CSV  = r"purchase_orders_full_flat37 ordered.csv"
OUTPUT_CSV = r"purchase_orders_full_flat37_anonymized.csv"
MAPPING_CSV = r"anonymization_mapping.csv"
ID_MAPPING_CSV = r"id_mapping.csv"

# --- Colonnes à anonymiser (noms) ---
COLS = [
    "attributes.manufacturer",
    "buyer.nodes.name",
    "buyer.nodes.parentCompany.nodes.name",
    "point_of_contact.nodes.name",
    "attributes.sellingManufacturerFacilityName",
    "attributes.trader",
]

# --- Colonnes identifiants ---
ID_COLS = [
    "rowId",
    "produits.nodes.asset.attributes.gtin",
    "produits.nodes.asset.attributes.hsCode",
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
    """Anonymisation des noms (entreprises, contacts, etc.)"""
    if pd.isna(original) or str(original).strip() == "":
        return ""   # on garde vide si vide à l'origine

    if original not in ANONYMIZED_VALUES:
        if "buyer" in col or "trader" in col:
            new_val = fake.company()
        elif "manufacturer" in col or "sellingManufacturerFacilityName" in col:
            new_val = fake.company() + " Manufacturing"
        elif "point_of_contact" in col:
            new_val = fake.name()
        else:
            new_val = fake.word().capitalize()
        ANONYMIZED_VALUES[original] = new_val

    return ANONYMIZED_VALUES[original]


def _fmt(seq, width=6):
    return str(seq).zfill(width)

def anonymize_id_value(original, col):
    """Anonymisation des identifiants (rowId, gtin, hsCode)"""
    if original is None or str(original).strip() in ["", "nan", "NaN", "null", "None"]:
        return ""

    orig = str(original).strip()
    if orig in ID_MAP:
        return ID_MAP[orig]["fake"]

    if col == "rowId":
        ID_COUNTER["PO"] += 1
        fakev = f"PO_{_fmt(ID_COUNTER['PO'])}"
        id_type = "PO"
    elif "gtin" in col.lower():
        ID_COUNTER["GTIN"] += 1
        fakev = f"FAKE_GTIN_{_fmt(ID_COUNTER['GTIN'])}"
        id_type = "GTIN"
    elif "hs" in col.lower():
        ID_COUNTER["HS"] += 1
        fakev = f"FAKE_HS_{_fmt(ID_COUNTER['HS'])}"
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
N = 100000   

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
import os

# Charger les variables depuis le fichier .env
load_dotenv()

# === 1. Charger le fichier CSV ===
csv_file = r"purchase_orders_sample_random.csv"
df = pd.read_csv(csv_file, sep=";")

numeric_cols = [
    "produits.nodes.value",
    "feedstock.nodes.quantity",
    "attributes.millsTracabilityPo",
    "attributes.millsTracabilityPko",
    "attributes.plantationsTracabilityPo",
    "attributes.plantationsTracabilityPko",
    "attributes.volumeContact"
]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# === 2. Supprimer et recréer la base Projet ===
#  AUTOCOMMIT pour DROP/CREATE DATABASE
# Lire le mot de passe dans la variable d’environnement
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
# === 4. Créer les tables de dimensions et de faits ===
# === 4. Créer les tables de dimensions et de faits ===
schema_sql = """

DROP TABLE IF EXISTS Fact_Orders CASCADE;
DROP SEQUENCE IF EXISTS fact_orders_id_fact_seq CASCADE;
DROP TABLE IF EXISTS Dim_Manufacturer CASCADE;
DROP TABLE IF EXISTS Dim_PointOfContact CASCADE;
DROP TABLE IF EXISTS Dim_Trader CASCADE;
DROP TABLE IF EXISTS Dim_Certification CASCADE;
DROP TABLE IF EXISTS Dim_Feedstock CASCADE;
DROP TABLE IF EXISTS Dim_Product CASCADE;
DROP TABLE IF EXISTS Dim_Buyer CASCADE;

-- Table des acheteurs
CREATE TABLE IF NOT EXISTS Dim_Buyer (
    id_buyer SERIAL PRIMARY KEY,
    "buyer.nodes.name" TEXT,
    "buyer.nodes.parentCompany.nodes.name" TEXT,
    "attributes.buyerNameContact" TEXT
);

-- Table des produits
CREATE TABLE IF NOT EXISTS Dim_Product (
    id_product SERIAL PRIMARY KEY,
    "produits.nodes.asset.descriptionShort" TEXT,
    "produits.nodes.asset.attributes.gtin" TEXT,
    "produits.nodes.asset.attributes.hsCode" TEXT,
    "produits.nodes.asset.attributes.descriptionShort" TEXT,
    "produits.nodes.asset.attributes.regulatedProductName" TEXT,
    "produits.nodes.unit" TEXT,
    "produits.nodes.asset.attributes.category" TEXT,
    "produits.nodes.asset.attributes.isComponent" TEXT,
    "produits.nodes.asset.attributes.productLine" TEXT,
    "produits.nodes.asset.attributes.countryOfOrigin" TEXT
);

-- Table des matières premières
CREATE TABLE IF NOT EXISTS Dim_Feedstock (
    id_feedstock SERIAL PRIMARY KEY,
    "attributes.feedstock" TEXT,
    "feedstock.nodes.asset.descriptionShort" TEXT,
    "feedstock.nodes.rawMaterial.name" TEXT,
    "feedstock.nodes.unit" TEXT
);

-- Table des certifications
CREATE TABLE IF NOT EXISTS Dim_Certification (
    id_certification SERIAL PRIMARY KEY,
    "transactionExpectedCertificateNomenclatures.nodes.tradeItem.descriptionShort" TEXT,
    "transactionExpectedCertificateNomenclatures.nodes.type" TEXT,
    "transactionExpectedCertificateNomenclatures.nodes.level" TEXT
);

-- Table des traders
CREATE TABLE IF NOT EXISTS Dim_Trader (
    id_trader SERIAL PRIMARY KEY,
    "attributes.trader" TEXT,
    "attributes.traderContact" TEXT
);

-- Table des points de contact
CREATE TABLE IF NOT EXISTS Dim_PointOfContact (
    id_poc SERIAL PRIMARY KEY,
    "point_of_contact.nodes.name" TEXT
);

-- Table des fabricants
CREATE TABLE IF NOT EXISTS Dim_Manufacturer (
    id_manufacturer SERIAL PRIMARY KEY,
    "attributes.sellingManufacturerFacilityName" TEXT,
    "attributes.manufacturer" TEXT,
    "attributes.manufacturerContact" TEXT
);

-- Table des faits (transactions)
CREATE TABLE IF NOT EXISTS Fact_Orders (
    id_fact SERIAL PRIMARY KEY,
    "rowId" TEXT,
    "type" TEXT,
    id_buyer INT REFERENCES Dim_Buyer(id_buyer),
    id_product INT REFERENCES Dim_Product(id_product),
    id_feedstock INT REFERENCES Dim_Feedstock(id_feedstock),
    id_certification INT REFERENCES Dim_Certification(id_certification),
    id_trader INT REFERENCES Dim_Trader(id_trader),
    id_poc INT REFERENCES Dim_PointOfContact(id_poc),
    id_manufacturer INT REFERENCES Dim_Manufacturer(id_manufacturer),
    "produits.nodes.value" NUMERIC,
    "feedstock.nodes.quantity" NUMERIC,
    "attributes.millsTracabilityPo" NUMERIC,
    "attributes.millsTracabilityPko" NUMERIC,
    "attributes.plantationsTracabilityPo" NUMERIC,
    "attributes.plantationsTracabilityPko" NUMERIC,
    "attributes.volumeContact" NUMERIC
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
## === 5. Alimentation des tables de dimensions ===
with engine.begin() as conn:
    # Buyer
    conn.execute(text("""
        INSERT INTO Dim_Buyer ("buyer.nodes.name", "buyer.nodes.parentCompany.nodes.name", "attributes.buyerNameContact")
        SELECT DISTINCT "buyer.nodes.name", "buyer.nodes.parentCompany.nodes.name", "attributes.buyerNameContact"
        FROM staging_orders
        WHERE "buyer.nodes.name" IS NOT NULL;
    """))

    # Product
    conn.execute(text("""
        INSERT INTO Dim_Product ("produits.nodes.asset.descriptionShort", "produits.nodes.asset.attributes.gtin",
            "produits.nodes.asset.attributes.hsCode", "produits.nodes.asset.attributes.descriptionShort",
            "produits.nodes.asset.attributes.regulatedProductName", "produits.nodes.unit",
            "produits.nodes.asset.attributes.category", "produits.nodes.asset.attributes.isComponent",
            "produits.nodes.asset.attributes.productLine", "produits.nodes.asset.attributes.countryOfOrigin")
        SELECT DISTINCT "produits.nodes.asset.descriptionShort", "produits.nodes.asset.attributes.gtin",
            "produits.nodes.asset.attributes.hsCode", "produits.nodes.asset.attributes.descriptionShort",
            "produits.nodes.asset.attributes.regulatedProductName", "produits.nodes.unit",
            "produits.nodes.asset.attributes.category", "produits.nodes.asset.attributes.isComponent",
            "produits.nodes.asset.attributes.productLine", "produits.nodes.asset.attributes.countryOfOrigin"
        FROM staging_orders;
    """))

    # Feedstock
    conn.execute(text("""
        INSERT INTO Dim_Feedstock ("attributes.feedstock", "feedstock.nodes.asset.descriptionShort",
            "feedstock.nodes.rawMaterial.name", "feedstock.nodes.unit")
        SELECT DISTINCT "attributes.feedstock", "feedstock.nodes.asset.descriptionShort",
            "feedstock.nodes.rawMaterial.name", "feedstock.nodes.unit"
        FROM staging_orders;
    """))

    # Certification
    conn.execute(text("""
        INSERT INTO Dim_Certification ("transactionExpectedCertificateNomenclatures.nodes.tradeItem.descriptionShort",
            "transactionExpectedCertificateNomenclatures.nodes.type",
            "transactionExpectedCertificateNomenclatures.nodes.level")
        SELECT DISTINCT "transactionExpectedCertificateNomenclatures.nodes.tradeItem.descriptionShort",
            "transactionExpectedCertificateNomenclatures.nodes.type",
            "transactionExpectedCertificateNomenclatures.nodes.level"
        FROM staging_orders;
    """))

    # Trader
    conn.execute(text("""
        INSERT INTO Dim_Trader ("attributes.trader", "attributes.traderContact")
        SELECT DISTINCT "attributes.trader", "attributes.traderContact"
        FROM staging_orders;
    """))

    # Point of Contact
    conn.execute(text("""
        INSERT INTO Dim_PointOfContact ("point_of_contact.nodes.name")
        SELECT DISTINCT "point_of_contact.nodes.name"
        FROM staging_orders;
    """))

    # Manufacturer
    conn.execute(text("""
        INSERT INTO Dim_Manufacturer ("attributes.sellingManufacturerFacilityName",
            "attributes.manufacturer", "attributes.manufacturerContact")
        SELECT DISTINCT "attributes.sellingManufacturerFacilityName",
            "attributes.manufacturer", "attributes.manufacturerContact"
        FROM staging_orders;
    """))

print("✅ Dimensions alimentées avec succès")


# === 6. Alimentation de la table des faits ===
with engine.begin() as conn:
    conn.execute(text("""
        INSERT INTO Fact_Orders (
            "rowId", "type",
            id_buyer, id_product, id_feedstock, id_certification, id_trader, id_poc, id_manufacturer,
            "produits.nodes.value", "feedstock.nodes.quantity",
            "attributes.millsTracabilityPo", "attributes.millsTracabilityPko",
            "attributes.plantationsTracabilityPo", "attributes.plantationsTracabilityPko",
            "attributes.volumeContact"
        )
        SELECT
            staging_orders."rowId",
            staging_orders."type",
            Dim_Buyer.id_buyer,
            Dim_Product.id_product,
            Dim_Feedstock.id_feedstock,
            Dim_Certification.id_certification,
            Dim_Trader.id_trader,
            Dim_PointOfContact.id_poc,
            Dim_Manufacturer.id_manufacturer,
            staging_orders."produits.nodes.value",
            staging_orders."feedstock.nodes.quantity",
            staging_orders."attributes.millsTracabilityPo",
            staging_orders."attributes.millsTracabilityPko",
            staging_orders."attributes.plantationsTracabilityPo",
            staging_orders."attributes.plantationsTracabilityPko",
            staging_orders."attributes.volumeContact"
        FROM staging_orders
        LEFT JOIN Dim_Buyer
            ON staging_orders."buyer.nodes.name" = Dim_Buyer."buyer.nodes.name"
        LEFT JOIN Dim_Product
            ON staging_orders."produits.nodes.asset.descriptionShort" = Dim_Product."produits.nodes.asset.descriptionShort"
        LEFT JOIN Dim_Feedstock
            ON staging_orders."attributes.feedstock" = Dim_Feedstock."attributes.feedstock"
        LEFT JOIN Dim_Certification
            ON staging_orders."transactionExpectedCertificateNomenclatures.nodes.tradeItem.descriptionShort" = Dim_Certification."transactionExpectedCertificateNomenclatures.nodes.tradeItem.descriptionShort"
        LEFT JOIN Dim_Trader
            ON staging_orders."attributes.trader" = Dim_Trader."attributes.trader"
        LEFT JOIN Dim_PointOfContact
            ON staging_orders."point_of_contact.nodes.name" = Dim_PointOfContact."point_of_contact.nodes.name"
        LEFT JOIN Dim_Manufacturer
            ON staging_orders."attributes.manufacturer" = Dim_Manufacturer."attributes.manufacturer";
    """))

print("✅ Table des faits alimentée avec succès")


