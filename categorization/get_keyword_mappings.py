import ast
import json
import pandas as pd
from tqdm import tqdm
from categorizer import Categorizer

all_kws = set()

extracted_df = pd.read_csv("../out/extracted.csv")
kws_raw = extracted_df["cordis_keywords"].tolist() + extracted_df["euroscivoc_keywords"].tolist()
kws_raw = [kw_lst for kw_lst in kws_raw if not pd.isna(kw_lst)]
kws = list(set([val.lower().strip().replace("\"", "") for kw_lst in kws_raw for val in ast.literal_eval(kw_lst)]))

kws_string = "count,kw\n"

for kw in tqdm(kws):
    if kw == "":
        continue

    kws_string += f"{kws.count(kw)},\"{kw}\"\n"

with open("kw_categorizations.csv", "w+") as f:
    f.write(kws_string)
print("done writing keywords")

with open("subcategories.json", "r") as f:
    subcategory_dict = json.load(f)
with open("subsubcategories.json", "r") as f:
    subsubcategory_dict = json.load(f)

df = pd.read_csv("kw_categorizations.csv")

c = Categorizer()

tqdm.pandas(desc="Getting categories", leave=True, miniters=10)
df["categories"] = df.progress_apply(lambda row: c.get_categories(row.kw), axis=1)
tqdm.pandas(desc="Getting subcategories", leave=True, miniters=10)
df["subcategories"] = df.progress_apply(lambda row: c.get_subcategory(row.kw, row.categories, subcategory_dict), axis=1)
tqdm.pandas(desc="Getting subsubcategories", leave=True, miniters=10)
df["subsubcategories"] = df.progress_apply(lambda row: c.get_subcategory(row.kw, row.subcategories, subsubcategory_dict), axis=1)
df.to_csv("kw_categorizations.csv", index=False)
