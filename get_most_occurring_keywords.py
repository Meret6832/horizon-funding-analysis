import argparse
import ast
import csv
import pandas as pd

from tqdm import tqdm


def count_keywords(
    ecmax: float, cluster: str, vals: list[str], freqs: dict, financial: dict
) -> None:
    kw_set = set()
    for val in vals:
        if pd.isna(val) or not val[0] == "[":
            continue

        val = val.lower()
        kws = ast.literal_eval(val)

        for kw in kws:
            kw = kw.lower()
            kw_set.add(kw)

    # Count keywords
    if cluster not in freqs.keys():
        freqs[cluster] = {}
        financial[cluster] = {}

    for kw in kw_set:
        if kw in freqs["all"].keys():
            freqs["all"][kw] += 1
            financial["all"][kw] += ecmax
        else:
            freqs["all"][kw] = 1
            financial["all"][kw] = ecmax
        if kw in freqs[cluster].keys():
            freqs[cluster][kw] += 1
            financial[cluster][kw] += ecmax
        else:
            freqs[cluster][kw] = 1
            financial[cluster][kw] = ecmax


def sort_dict(d: dict) -> dict:
    for key in d.keys():
        d[key] = dict(sorted(d[key].items(), key=lambda x: x[1], reverse=True))

    return dict(sorted(d.items(), key=lambda x: x[0], reverse=True))


def get_row_for_file(kw: str, cluster_row: list[str], d: dict) -> list[str | int]:
    row = [kw]
    for cluster in cluster_row:
        if kw in d[cluster]:
            row.append(d[cluster][kw])
        else:
            row.append(0)

    return row


if __name__ == "__main__":
    # Set up argument parser
    default_extracted_file = "./out/extracted.csv"

    parser = argparse.ArgumentParser()
    parser.add_argument("--extractedfile", "-ef", nargs='?', const=default_extracted_file, default=default_extracted_file, type=str)

    args = parser.parse_args()

    extracted_df = pd.read_csv(args.extractedfile)

    freqs = {"all": {}}
    financial = {"all": {}}

    tqdm.pandas(desc="Analyzing projects", leave=False, miniters=1)

    extracted_df.progress_apply(
        lambda row: count_keywords(row.ecMaxContribution, row.cluster,
                                   [row.cordis_keywords, row.euroscivoc_keywords],
                                    freqs, financial), axis=1
    )

    freqs = sort_dict(freqs)
    financial = sort_dict(financial)

    horizon_cl_clusters = sorted([key for key in freqs.keys() if key.startswith("HORIZON-CL")])
    cluster_headers = horizon_cl_clusters + \
        [key for key in freqs.keys() if key != "all" and key not in horizon_cl_clusters ]
    totalcount = extracted_df.id.count()
    total_counts = [totalcount] + [extracted_df[extracted_df.cluster == cluster].id.count()
                                   for cluster in cluster_headers]
    totalecmax = extracted_df.ecMaxContribution.sum()
    total_financial = [totalecmax] + [extracted_df[extracted_df.cluster == cluster].ecMaxContribution.sum()
                                      for cluster in cluster_headers]
    cluster_headers = ["all"] + cluster_headers

    counts_file = "./out/kw_counts.csv"
    print(f"Writing to {counts_file} ...")
    with open(counts_file, "w+") as f:
        writer = csv.writer(f)
        writer.writerow(["keyword"] + cluster_headers)
        writer.writerow(["Total count"] + total_counts)
        for kw in freqs["all"].keys():
            writer.writerow(get_row_for_file(kw, cluster_headers, freqs))

    financial_file = "./out/kw_ecmax.csv"
    print(f"Writing to {financial_file} ...")
    with open(financial_file, "w+") as f:
        writer = csv.writer(f)
        writer.writerow(["keyword"] + cluster_headers)
        writer.writerow(["Total EC max"] + total_financial)
        for kw in financial["all"].keys():
            writer.writerow(get_row_for_file(kw, cluster_headers, financial))
