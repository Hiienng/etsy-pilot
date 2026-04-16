"""
Tag Optimizer — extract và rank tags dựa trên TF-IDF + frequency.
Etsy cho phép tối đa 13 tags, mỗi tag tối đa 20 ký tự.
"""
from __future__ import annotations

import re
from collections import Counter
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np

ETSY_MAX_TAGS = 13
ETSY_MAX_TAG_LEN = 20


class TagOptimizer:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(
            ngram_range=(1, 2),
            min_df=1,
            max_features=10000,
            stop_words="english",
        )
        self._fitted = False

    def fit(self, tag_lists: list[str]) -> "TagOptimizer":
        """Fit trên corpus tags (mỗi phần tử là chuỗi tags comma-separated)."""
        self.vectorizer.fit(tag_lists)
        self._fitted = True
        return self

    def suggest_tags(self, title: str, current_tags: str | None, top_n: int = ETSY_MAX_TAGS) -> list[str]:
        """Suggest top_n tags cho listing dựa trên title + current tags."""
        if not self._fitted:
            raise RuntimeError("Call fit() trước khi dùng suggest_tags()")

        text = f"{title} {current_tags or ''}"
        vec = self.vectorizer.transform([text])
        feature_names = self.vectorizer.get_feature_names_out()
        scores = vec.toarray()[0]
        top_idx = np.argsort(scores)[::-1]

        tags = []
        for i in top_idx:
            tag = feature_names[i].strip()
            if len(tag) <= ETSY_MAX_TAG_LEN and tag not in tags:
                tags.append(tag)
            if len(tags) >= top_n:
                break
        return tags

    @staticmethod
    def parse_tags(tag_str: str) -> list[str]:
        """Parse comma-separated tags string thành list, filter invalid."""
        raw = [t.strip() for t in tag_str.split(",") if t.strip()]
        return [t for t in raw if 1 <= len(t) <= ETSY_MAX_TAG_LEN]

    @staticmethod
    def format_tags(tags: list[str]) -> str:
        """Format list tags thành comma-separated string cho Etsy."""
        valid = [t.strip() for t in tags if 1 <= len(t.strip()) <= ETSY_MAX_TAG_LEN]
        return ", ".join(valid[:ETSY_MAX_TAGS])

    @staticmethod
    def most_common(tag_lists: list[str], top_n: int = 50) -> list[tuple[str, int]]:
        """Thống kê tags phổ biến nhất từ toàn bộ corpus."""
        all_tags: list[str] = []
        for tags in tag_lists:
            all_tags.extend([t.strip().lower() for t in tags.split(",") if t.strip()])
        return Counter(all_tags).most_common(top_n)
