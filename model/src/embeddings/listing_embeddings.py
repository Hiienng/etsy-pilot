"""
Listing Embeddings — dùng HuggingFace sentence-transformers để embed listings.
Cho phép tìm similar listings và clustering theo semantic similarity.
"""
from __future__ import annotations

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"  # nhỏ, nhanh, phù hợp Etsy titles


class ListingEmbedder:
    def __init__(self, model_name: str = MODEL_NAME):
        self.model = SentenceTransformer(model_name)
        self._embeddings: np.ndarray | None = None
        self._texts: list[str] = []

    def encode(self, texts: list[str], batch_size: int = 64) -> np.ndarray:
        """Encode danh sách texts thành embedding vectors."""
        return self.model.encode(texts, batch_size=batch_size, show_progress_bar=False, normalize_embeddings=True)

    def index(self, texts: list[str]) -> None:
        """Build in-memory index từ corpus listings."""
        self._texts = texts
        self._embeddings = self.encode(texts)

    def find_similar(self, query: str, top_n: int = 10) -> list[tuple[str, float]]:
        """Tìm listings tương tự nhất với query."""
        if self._embeddings is None:
            raise RuntimeError("Call index() trước khi dùng find_similar()")
        q_vec = self.encode([query])
        sims = cosine_similarity(q_vec, self._embeddings)[0]
        top_idx = np.argsort(sims)[::-1][:top_n]
        return [(self._texts[i], float(sims[i])) for i in top_idx]

    def cluster_labels(self, texts: list[str], n_clusters: int = 5) -> list[int]:
        """K-Means clustering listings theo semantic similarity."""
        from sklearn.cluster import KMeans
        vecs = self.encode(texts)
        km = KMeans(n_clusters=n_clusters, random_state=42, n_init="auto")
        return km.fit_predict(vecs).tolist()
