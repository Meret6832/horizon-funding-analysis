import ast
import pandas as pd
from tqdm import tqdm


def get_categories(euroscivoc_kws, cordis_kw):
    euroscivoc_kws = ast.literal_eval(euroscivoc_kws) if pd.notna(euroscivoc_kws) else []
    cordis_kw = ast.literal_eval(cordis_kw) if pd.notna(cordis_kw) else []
    categories = set()
    subcategories = set()
    subsubcategories = set()

    kws = set(euroscivoc_kws).union(cordis_kw)
    for kw in kws:
        kw = kw.lower().strip()
        categories = categories.union(set(categories_dict.get(kw, [])))
        subcategories = subcategories.union(set(subcategories_dict.get(kw, [])))
        subsubcategories = subsubcategories.union(set(subsubcategories_dict.get(kw, [])))

    categories = list(categories) if len(categories) > 0 else None
    subcategories = list(subcategories) if len(subcategories) > 0 else None
    subsubcategories = list(subsubcategories) if len(subsubcategories) > 0 else None
    return (categories, subcategories, subsubcategories)


def process_categories(s: str):
    if pd.isna(s):
        return None

    if not s.startswith("["):
        s = "[\'" + s + "\']"

    return ast.literal_eval(s)

kw_mappings_df = pd.read_csv("kw_categorizations.csv")
kw_mappings_df["categories"] = kw_mappings_df["categories"].map(process_categories)
categories_dict = kw_mappings_df.set_index("kw")["categories"].map(list).to_dict()
kw_mappings_df["subcategories"] = kw_mappings_df["subcategories"].map(process_categories)
subcategories_dict = kw_mappings_df.set_index("kw")["subcategories"].map(list).to_dict()
kw_mappings_df["subsubcategories"] = kw_mappings_df["subsubcategories"].map(process_categories)
subsubcategories_dict = kw_mappings_df.set_index("kw")["subsubcategories"].map(list).to_dict()

extracted_df = pd.read_csv("../out/extracted.csv")
tqdm.pandas(desc="Assign keywords", leave=True, miniters=10)
extracted_df[["categories", "subcategories", "subsubcategories"]] = extracted_df.progress_apply(lambda row: get_categories(row.euroscivoc_keywords, row.cordis_keywords), axis=1).apply(pd.Series)
extracted_df.to_csv("../out/categorized.csv", index=False)
