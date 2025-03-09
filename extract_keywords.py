import argparse
import os
import pandas as pd
import requests

from bs4 import BeautifulSoup
from tqdm import tqdm


def convert_to_float(val: str | int) -> float | str | None:
    if pd.isna(val):
        return None

    try:
        return float(str(val).replace(",", "."))
    except ValueError:
        return val


def get_cluster(topic: str) -> str:
    if topic.startswith("HORIZON"):
        return "HORIZON-" + topic.split("-")[1]
    elif topic.startswith("ERC"):
        return "ERC-" + topic.split("-")[2]
    elif topic.startswith("EURATOM"):
        return "EURATOM-" + "-".join(topic.split("-")[2:-1])

    return topic


def get_euroscivoc_keywords(project_id: str, euroscivoc_df: pd.DataFrame) -> list[str]:
    return euroscivoc_df.loc[euroscivoc_df["projectID"] == project_id]["euroSciVocTitle"].tolist()


def get_cordis_keywords(project_id: str) -> list[str] | str:
    # print("getting", project_id, end=" ")
    url = f"https://cordis.europa.eu/project/id/{project_id}"
    # Try max 3 times
    for _ in range(3):
        try:
            res = requests.get(url, timeout=10)
        except Exception:
            res = requests.Response()
            res.status_code = 408

        if res.ok:
            break

    if not res.ok:
        # print(f"got not ok: {res.status_code}")
        return "ERROR"

    soup = BeautifulSoup(res.text, "html.parser")
    keywords_soup = soup.find("meta", attrs={"name": "keywords"})
    if keywords_soup is None:
        # print("nothing found")
        return "NOT FOUND"

    keywords = set([kw.strip() for kw in keywords_soup["content"].split(",") if (kw != "" and not kw.startswith("HORIZON"))])
    # print("found", keywords)
    return list(keywords)


if __name__ == "__main__":
    # Set up argument parser
    default_project_file = "./data/project.csv"
    default_euroscivoc_file = "./data/euroSciVoc.csv"

    parser = argparse.ArgumentParser()
    parser.add_argument("--clusters", "-c", nargs="?", const="all", default="all", type=str, help="clusters to look at (e.g. 124 for clusters 1, 2 and 4)")
    parser.add_argument("--projectfile", "-pf", nargs='?', const=default_project_file, default=default_project_file, type=str)
    parser.add_argument("--euroscivocfile", "-ef", nargs="?", const=default_euroscivoc_file, default=default_euroscivoc_file, type=str)

    args = parser.parse_args()

    clusters = []
    if args.clusters == "all":
        clusters = "all"
    else:
        for c in args.clusters:
            try:
                c = int(c)
                if c < 1 or c > 6:
                    raise ValueError
                clusters.append(f"CL{c}")
            except ValueError:
                parser.error("--clusters/-c can only contain numbers 1-6")

    project_df = pd.read_csv(args.projectfile)

    # Remove columns which are not needed
    project_df = project_df.drop(columns=[
        "acronym", "status", "title", "startDate", "endDate", "totalCost",
        "legalBasis", "ecSignatureDate", "frameworkProgramme",
        "masterCall", "subCall", "fundingScheme", "nature", "objective",
        "contentUpdateDate", "rcn", "grantDoi"
    ])

    # Convert numbers to floats
    project_df["ecMaxContribution"] = project_df["ecMaxContribution"].apply(lambda val: convert_to_float(val))

    # Filter clusters
    project_df["cluster"] = project_df["topics"].apply(lambda topic: get_cluster(topic))
    if clusters != "all":
        project_df = project_df.loc[project_df.cluster.isin(clusters)]

    # Add euroSciVoc keywords
    print("Getting euroSciVoc keywords...")
    euroscivoc_df = pd.read_csv(args.euroscivocfile)
    euroscivoc_keywords = euroscivoc_df.groupby("projectID")["euroSciVocTitle"].apply(list).reset_index(name="euroscivoc_keywords")
    project_df = project_df.merge(euroscivoc_keywords, how="left", left_on="id", right_on="projectID").drop(columns=["projectID"])

    # Get CORDIS keywords
    tqdm.pandas(desc="Retrieving keywords from CORDIS", leave=True, miniters=1)
    project_df["cordis_keywords"] = project_df.progress_apply(lambda row: get_cordis_keywords(row.id), axis=1)

    out_file = "./out/extracted.csv"
    print(f"Writing to {out_file} ...")
    try:
        os.makedirs("./out", exist_ok=True)
    except FileExistsError:
        pass
    project_df.to_csv(out_file, index=False)
