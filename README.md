Code for the data analysis of "The digital innovation we need: Three lessons on EU R&amp;I funding".


# Data Extraction
The datasets used were downloaded on 4 November 2024 from the [open-source European Data portal](https://data.europa.eu/data/datasets/cordis-eu-research-projects-under-horizon-europe-2021-2027?locale=en) and can be found in the [datasets](datasets) folder.

1. [extract_keywords.py](extract_keywords.py): For each project in the [project](datasets/project.csv) dataset, its keywords are scraped from its dedicated CORDIS webpage. The keywords (fields of science) from the [euroSciVoc](datasets/euroSciVoc.csv) are also extracted for each project.
2. [get_most_occurring_keywords.py](get_most_occurring_keywords.py): Sort keywords according to most occuring and save to [kw_counts.csv](out/kw_counts.csv) (according to project count) and [kw_ecmax.csv](out/kw_ecmax.csv).
3. Manually assign the most occuring keywords to categories, subcategories and subsubcategories. The final categorization that was obtained through multiple iterations of analysis can be found in the [categorization](categorization) folder.
4. Categorize the projects:
    1. [get_keyword_mappings.py](categorization/get_keyword_mappings.py): Compute the mapping of keyword -> category for all keywords in the dataset. This mapping is computed with the categorizer defined in [categorizer.py](categorization/categorizer.py). The resulting mappings are written to [kw_categorizations.csv](categorization/kw_categorizations.csv).
    2. [categorize.py](categorization/categorize.py): Assign categories to each project in [extracted.csv](out/extracted.csv) using the mappings defined in [kw_categorizations.csv](categorization/kw_categorizations.csv). The categorized dataset is saved to [categorized.csv](out/categorized.csv).

# Data Analysis
1. The data analysis carried out can be found in [analysis.ipynb](analysis.ipynb).
2. The [overviewCategories](overviewCategories.csv) table was generated with [gen_table.py](gen_table.py).

This code and analysis is based on the code for the "Open Source Intelligence on Budgets for Bits: An Analysis of EU Funding Allocation" project of the Digital Methods Initialitive Summer School 2024 by Anvee Tara, Bastian August, Brogan Latil, Fieke Jansen, Furkan Dabaniyasti, Gizem Brasser, Jasmin Shahbazi, Maxigas, Meret Baumgartner, Niels ten Oever, Sarah Vorndran, Zuza Warso. The report for this project can be found [here](https://www.digitalmethods.net/Dmi/SummerSchool2024BudgetsforBits) and the code can be found [here](https://github.com/Fiekej/infralab_summerschool_2024).
