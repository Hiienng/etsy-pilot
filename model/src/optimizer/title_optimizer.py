"""
Title Optimizer — scikit-learn TF-IDF based keyword scoring.
Dùng để score và rank keywords từ corpus listing để suggest title improvements
trước khi gọi Claude (giảm token cost).
"""
from __future__ import annotations

import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np


class TitleOptimizer:
    def __init__(self, max_title_length: int = 140):
        self.max_title_length = max_title_length
        self.vectorizer = TfidfVectorizer(
            ngram_range=(1, 3),
            min_df=1,
            max_features=5000,
            stop_words="english",
        )
        self._corpus: list[str] = []
        self._fitted = False

    def fit(self, titles: list[str]) -> "TitleOptimizer":
        """Fit TF-IDF trên toàn bộ corpus listing titles."""
        self._corpus = titles
        self.vectorizer.fit(titles)
        self._fitted = True
        return self

    def top_keywords(self, title: str, top_n: int = 10) -> list[tuple[str, float]]:
        """Trả về top keywords và score cho một title."""
        if not self._fitted:
            raise RuntimeError("Call fit() trước khi dùng top_keywords()")
        vec = self.vectorizer.transform([title])
        feature_names = self.vectorizer.get_feature_names_out()
        scores = vec.toarray()[0]
        top_idx = np.argsort(scores)[::-1][:top_n]
        return [(feature_names[i], float(scores[i])) for i in top_idx if scores[i] > 0]

    def similar_titles(self, title: str, top_n: int = 5) -> list[tuple[str, float]]:
        """Tìm các titles tương tự trong corpus để tham khảo."""
        if not self._fitted:
            raise RuntimeError("Call fit() trước khi dùng similar_titles()")
        corpus_vecs = self.vectorizer.transform(self._corpus)
        query_vec = self.vectorizer.transform([title])
        sims = cosine_similarity(query_vec, corpus_vecs)[0]
        top_idx = np.argsort(sims)[::-1][:top_n]
        return [(self._corpus[i], float(sims[i])) for i in top_idx]

    @staticmethod
    def truncate(title: str, max_len: int = 140) -> str:
        """Cắt title về max length, tại word boundary."""
        if len(title) <= max_len:
            return title
        return title[:max_len].rsplit(" ", 1)[0].rstrip(",.;:")
