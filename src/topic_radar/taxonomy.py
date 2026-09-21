"""分类词表：题材 -> 分类、受众契合度。

未命中任何关键词的题材不会被强行归类，而是带着 ``uncategorized`` 降级
进入榜单，契合度取配置中的低值。
"""

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Category:
    name: str
    audience_fit: float
    keywords: tuple[str, ...]
    note: str | None = None


@dataclass(frozen=True)
class Classification:
    category: Category | None
    matched_keywords: tuple[str, ...]
    audience_fit: float

    @property
    def categorized(self) -> bool:
        return self.category is not None

    @property
    def category_name(self) -> str | None:
        return self.category.name if self.category else None


class Taxonomy:
    def __init__(self, categories: list[Category], uncategorized_fit: float = 0.3):
        self._categories = tuple(categories)
        self._uncategorized_fit = uncategorized_fit

    @classmethod
    def load(cls, path: str | Path | None, uncategorized_fit: float = 0.3) -> "Taxonomy":
        if path is None:
            return cls([], uncategorized_fit)
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        categories = [
            Category(
                name=str(item["name"]),
                audience_fit=float(item.get("audience_fit", 0.5)),
                keywords=tuple(str(k) for k in item.get("keywords", [])),
                note=item.get("note"),
            )
            for item in raw.get("categories", [])
        ]
        return cls(categories, uncategorized_fit)

    def classify(self, topic: str) -> Classification:
        best: Category | None = None
        best_hits: tuple[str, ...] = ()
        for category in self._categories:
            hits = tuple(k for k in category.keywords if k in topic)
            if len(hits) > len(best_hits):
                best, best_hits = category, hits
        if best is None:
            return Classification(None, (), self._uncategorized_fit)
        return Classification(best, best_hits, best.audience_fit)
