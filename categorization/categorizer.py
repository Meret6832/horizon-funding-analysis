import ast
import json
import numpy as np
import pandas as pd
import re
from nltk import pos_tag, word_tokenize
from nltk.corpus import wordnet
from nltk.corpus.reader.wordnet import Synset
from nltk.wsd import lesk
from unidecode import unidecode

SIMILARITY_THRESH = 0.95  # wup similarity must be bigger or equal for a match


class Categorizer:
    def __init__(self) -> None:
        print("Initializing Categorizer...")
        self.kw_cache = {}
        self._init_mappings()
        self.ignore_kws = []
        with open("ignore_kws.txt", "r") as f:
            for line in f.readlines():
                if line.strip() != "":
                    self.ignore_kws.append(line)

        print("* Getting keyword synsets...")
        self._init_kw_synsets()

    def _init_mappings(self) -> None:
        with open("./categories.json") as f:
            self.category_kws = json.load(f)  # {category: [kws]}

        # Add key as keyword.
        for category, kws in self.category_kws.items():
            if category.lower() not in kws:
                self.category_kws[category] = kws + category.lower()

        self.kw_category = {}  # {kw: category}
        self.kw_kw_stripped = {}  # {kw: kw_stripped}
        for category, kws in self.category_kws.items():
            for kw in kws:
                self.kw_category[kw] = category
                kw_stripped = re.sub(r"[^a-zA-Z0-9]", "", unidecode(kw.lower()))
                self.kw_kw_stripped[kw] = kw_stripped
                self.kw_cache[kw_stripped] = category

    def _init_kw_synsets(self) -> dict[str, dict[str, list[Synset]]]:
        """
        Returns a dictionary {
            kw: [[token0_synset0, token0_synset1, ...], [token1_synset0, token1_synset1, ...], ...]
        }
        """
        self.kw_synsets = {}
        for kw in self.kw_category.keys():
            self.kw_synsets[kw] = self._get_synsets(kw)

    def _get_wordnet_pos(self, pos_tag: str) -> str | None:
        if pos_tag.startswith("J"):
            return wordnet.ADJ
        if pos_tag.startswith("V"):
            return wordnet.VERB
        if pos_tag.startswith("N"):
            return wordnet.NOUN
        if pos_tag.startswith("R"):
            return wordnet.ADV

        return None

    def _get_synsets(self, kw: str) -> list[list[Synset]] | None:
        kw = kw.strip()

        # Generate all synsets
        # Try the whole keyword
        all_synsets = wordnet.synsets(kw)
        if len(all_synsets) == 0:  # No match, try decoding.
            all_synsets = wordnet.synsets(unidecode(kw))

        if len(all_synsets) >= 1:  # Found synsets, return them.
            return [[s for s in all_synsets if isinstance(s, Synset)]]

        # If the whole kw is not recognized, try the individual tokens.
        tokens = word_tokenize(kw)
        for token in tokens:
            token_synsets = wordnet.synsets(token)
            if len(token_synsets) > 0:
                all_synsets.append(token_synsets)
            else:
                all_synsets.append(None)

        if len(all_synsets) == 0:  # Could not find any synsets, return None.
            return None

        # Filter synsets according to POS
        selected_synsets = []

        pos_tags = pos_tag(tokens)
        context = [t for t, _ in pos_tags]
        for i in range(len(tokens)):
            token_synsets = all_synsets[i]
            if token_synsets is None:
                continue

            token, pos = pos_tags[i]
            wordnet_pos = self._get_wordnet_pos(pos)
            not_wordnet_tokens = []
            if wordnet_pos is None:  # Not a wordnet POS, skip and add anyways.
                not_wordnet_tokens.append(token_synsets)

            selected_token_synsets = []
            for syn in token_synsets:
                if syn.pos() == wordnet_pos:
                    selected_token_synsets.append(syn)

            if len(selected_token_synsets) == 0:
                continue

            # Select synsets with Word Sense Disambiguation
            if len(selected_token_synsets) > 1:
                selected_synset_no_context = lesk(context, token, pos=wordnet_pos, synsets=selected_token_synsets)  # use whole keyword as context
                selected_synsets_wsd = set()
                if selected_synset_no_context is not None:
                    selected_synsets_wsd.add(selected_synset_no_context)
                if len(selected_synsets_wsd) > 0:
                    selected_token_synsets = list(selected_synsets_wsd)

            selected_token_synsets.append(not_wordnet_tokens)

            if len(selected_token_synsets) == 0:  # could not select any, just keep all.
                selected_synsets.append(all_synsets[i])
            else:
                selected_synsets.append([s for s in selected_token_synsets if isinstance(s, Synset)])

        if len(selected_synsets) > 0:
            return [s for s in selected_synsets if (s is not None and s != [])]

        # Filtering resulted in no synsents being selected, just return all.
        return_synsets = [s for s in all_synsets if (s is not None and s != [])]
        if len(return_synsets) == 0:
            return None

        return return_synsets

    def _synsets_match(self, kw_synsets: list[list[Synset]] | None, compare_synsets: list[list[Synset]] | None) -> float:  # 0 is not a match, 1 is a perfect match.
        """
        Check if compare_synsets is a subset of kw_synsets.
        """
        if compare_synsets is None or kw_synsets is None:
            return 0

        if len(compare_synsets) > len(kw_synsets):  # kw cannot be part of compre_kw
            return 0

        comp_i = 0
        scores = []

        kw_i = 0
        comp_i = 0
        scores = []
        while kw_i < len(kw_synsets) and comp_i < len(compare_synsets):
            kw_token = kw_synsets[kw_i]
            compare_token = compare_synsets[comp_i]

            matching = False
            # Check if matching
            for comp_syn in compare_token:
                for kw_syn in kw_token:
                    sim_score = kw_syn.wup_similarity(comp_syn)
                    if sim_score is not None and sim_score >= SIMILARITY_THRESH:
                        # print(comp_i, kw_syn, comp_syn)
                        matching = True
                        scores.append(sim_score)
                        break

            if matching:
                comp_i += 1
            else:
                comp_i = 0
                scores = []
            kw_i += 1

        if comp_i == len(compare_synsets):
            return np.mean(scores)

        return 0

    def get_categories(self, kw: str) -> list[str]:
        if pd.isna(kw):
            return []

        kw = kw.lower()
        kw_stripped = re.sub(r"[^a-zA-Z0-9]", "", unidecode(kw.lower()))
        if kw_stripped in self.kw_cache.keys():
            return self.kw_cache[kw_stripped]

        kw_synsets = self._get_synsets(kw)

        token_matches = set()
        part_matches = set()
        for compare_kw, compare_synsets in self.kw_synsets.items():
            compare_category = self.kw_category[compare_kw]

            if kw_stripped == self.kw_kw_stripped[compare_kw]:  # Direct match, return category
                self.kw_cache[kw_stripped] = [compare_category]
                return [compare_category]

            if compare_category not in token_matches:
                # Check if compare_kw is one of the tokens in kw
                kw_tokens = [re.sub(r"[^a-zA-Z0-9]", "", unidecode(s.lower())) for s in re.split(r"[\s\-_/#@\.,\(\)\[\]\|\&]", kw)]
                compare_stripped = re.sub(r"[^a-zA-Z0-9]", "", unidecode(compare_kw.lower()))
                found_match = False
                for kw_token in kw_tokens:
                    if kw_token == compare_stripped:  # compare_kw is a match for one of the tokens in kw.
                        token_matches.add(compare_category)
                        # print(f"Word match: {kw_token} ({kw}) -> {compare_kw} -> {compare_category}")
                        found_match = True
                        break

                if not found_match and compare_synsets is not None and kw_synsets is not None:
                    # Only compare synsets if a synset could be found for at least 3/4 of the tokens in compare_kw.
                    if len(compare_synsets) >= (len(word_tokenize(compare_kw)) * 0.75):
                        match_score = self._synsets_match(kw_synsets, compare_synsets)
                        if match_score > 0:
                            token_matches.add(compare_category)
                            # print(f"Syn match {match_score}: {kw} ({len(kw_synsets)}) -> {compare_kw} -> {compare_category}")
                            continue

            if compare_category not in part_matches and compare_kw in kw and len(compare_kw) > 4:
                part_matches.add(compare_category)
                # print(f"Part match: {kw} -> {compare_kw} -> {compare_category}")

        matches = list(token_matches.union(part_matches))
        self.kw_cache[kw_stripped] = matches
        return matches

    def get_subcategory(self, kw: str, categories: str, subcategories_dict: dict) -> list[tuple[str, str]]:
        if pd.isna(kw):
            return []

        categories = str(categories)
        if not categories.startswith("["):
            categories = f"['{categories}']"
        try:
            categories = ast.literal_eval(categories)
        except ValueError:
            return []

        if len(categories) == 0:
            return []
        if type(categories[0]) is (tuple):
            categories = np.array(categories)[:, 1]

        matching_category = False
        for category in categories:
            if category in subcategories_dict.keys():
                matching_category = True
                break

        if not matching_category:
            return []

        kw = kw.lower()
        kw_stripped = re.sub(r"[^a-zA-Z0-9]", "", unidecode(kw.lower()))
        kw_synsets = self._get_synsets(kw)

        token_matches = set()
        part_matches = set()
        for category in categories:
            category = str(category)
            if category not in subcategories_dict.keys():
                continue

            for subcategory, subcat_kws in subcategories_dict[category].items():
                for compare_kw in subcat_kws:
                    compare_synsets = self.kw_synsets.get(compare_kw)

                    try:
                        compare_kw_stripped = self.kw_kw_stripped[compare_kw]
                    except KeyError:
                        compare_kw_stripped = re.sub(r"[^a-zA-Z0-9]", "", unidecode(compare_kw.lower()))
                        self.kw_kw_stripped[compare_kw] = compare_kw_stripped
                    if kw_stripped == compare_kw_stripped:  # Direct match, return category
                        return [(category, subcategory)]

                    if (category, subcategory) not in token_matches:
                        # Check if compare_kw is one of the tokens in kw
                        kw_tokens = [re.sub(r"[^a-zA-Z0-9]", "", unidecode(s.lower())) for s in re.split(r"[\s\-_/#@\.,\(\)\[\]\|\&]", kw)]
                        compare_stripped = re.sub(r"[^a-zA-Z0-9]", "", unidecode(compare_kw.lower()))
                        found_match = False
                        for kw_token in kw_tokens:
                            if kw_token == compare_stripped:  # compare_kw is a match for one of the tokens in kw.
                                token_matches.add((category, subcategory))
                                # print(f"Word match: {kw_token} ({kw}) -> {compare_kw} -> {subcategory}")
                                found_match = True
                                break

                        if not found_match and compare_synsets is not None and kw_synsets is not None:
                            # Only compare synsets if a synset could be found for at least 3/4 of the tokens in compare_kw.
                            if len(compare_synsets) >= (len(word_tokenize(compare_kw)) * 0.75):
                                match_score = self._synsets_match(kw_synsets, compare_synsets)
                                if match_score > 0:
                                    token_matches.add((category, subcategory))
                                    # print(f"Syn match {match_score}: {kw} ({len(kw_synsets)}) -> {compare_kw} -> {subcategory}")
                                    continue

                    if (category, subcategory) not in part_matches and compare_kw in kw and len(compare_kw) > 4:
                        part_matches.add((category, subcategory))
                        # print(f"Part match: {kw} -> {compare_kw} -> {subcategory}")

        matches = list(token_matches.union(part_matches))
        return matches
