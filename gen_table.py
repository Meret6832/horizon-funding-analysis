import ast
import json
import numpy as np
import pandas as pd


def has_cat(row, cat):
    if row.categories is None:
        return False
    if cat in row.categories:
        return True

    if row.subcategories is None:
        return False
    if cat in np.array(row.subcategories)[:, 1]:
        return True

    if row.subsubcategories is None:
        return False
    if cat in np.array(row.subsubcategories)[:, 1]:
        return True

    return False


categorized_df = pd.read_csv("../out/categorized.csv")
categorized_df["categories"] = categorized_df.apply(lambda row: ast.literal_eval(row.categories) if pd.notna(row.categories) else None, axis=1)
categorized_df["subcategories"] = categorized_df.apply(lambda row: ast.literal_eval(row.subcategories) if pd.notna(row.subcategories) else None, axis=1)
categorized_df["subsubcategories"] = categorized_df.apply(lambda row: ast.literal_eval(row.subsubcategories) if pd.notna(row.subsubcategories) else None, axis=1)
total_projects = categorized_df.index.nunique()
total_ecmax = categorized_df["ecMaxContribution"].sum()

table_df = pd.DataFrame(columns=["category", "subcategory", "subsubcategory", "number of projects", "% of all projects", "ecMaxContribution", "% of total ecMaxContribution"])

# Add Total row
table_df.loc[len(table_df)] = ["all", None, None, total_projects, 100, total_ecmax, 100]

# Categories
with open("../categorization/categories.json", "r") as f:
    categories = json.load(f)
with open("../categorization/subcategories.json", "r") as f:
    subcategories = json.load(f)
with open("../categorization/subsubcategories.json", "r") as f:
    subsubcategories = json.load(f)

categories_sorted = sorted(categories.keys())
for cat in categories_sorted:
    print(cat)
    this_df = categorized_df[categorized_df.apply(lambda row: has_cat(row, cat), axis=1)]
    table_df.loc[len(table_df)] = [cat.upper(), None, None, len(this_df), round(len(this_df)/total_projects*100, 2), round(this_df["ecMaxContribution"].sum(), 2), round(this_df["ecMaxContribution"].sum()/total_ecmax*100, 2)]

    if cat in subcategories.keys():
        cat_subs = sorted(subcategories[cat].keys())
        for subcat in cat_subs:
            print(subcat)
            this_df = categorized_df[categorized_df.apply(lambda row: has_cat(row, subcat), axis=1)]
            table_df.loc[len(table_df)] = [cat.upper(), subcat.upper(), None, len(this_df), round(len(this_df)/total_projects*100, 2), round(this_df["ecMaxContribution"].sum(), 2), round(this_df["ecMaxContribution"].sum()/total_ecmax*100, 2)]

            if subcat in subsubcategories:
                cat_subsubs = sorted(subsubcategories[subcat].keys())
                for subsubcat in cat_subsubs:
                    print(subsubcat)
                    this_df = categorized_df[categorized_df.apply(lambda row: has_cat(row, subsubcat), axis=1)]
                    table_df.loc[len(table_df)] = [cat.upper(), subcat.upper(), subsubcat.upper(), len(this_df), round(len(this_df)/total_projects*100, 2), round(this_df["ecMaxContribution"].sum(), 2), round(this_df["ecMaxContribution"].sum()/total_ecmax*100, 2)]

table_df.to_csv("overviewCategories.csv", index=False)
