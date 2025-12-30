import difflib
from typing import List, Dict, Optional, Set, Iterable
import pandas as pd

class ModelNormalizer:
    """Normalizes brand and model names against a reference dataset."""

    def __init__(self, reference_data: pd.DataFrame):
        """
        Initialize the normalizer.

        Args:
            reference_data: DataFrame containing 'brand' and 'model' columns.
        """
        self.ref_data = reference_data
        
        # Pre-compute lookups for O(1) access
        # 1. Valid sets for fast existence checks
        self.valid_models: Set[str] = set(self.ref_data['model'].str.lower().unique())
        self.valid_brands: Set[str] = set(self.ref_data['brand'].str.lower().unique())
        
        # 2. Canonical name mappings (lowercase -> original case)
        self.canonical_models: Dict[str, str] = dict(zip(self.ref_data['model'].str.lower(), self.ref_data['model']))
        self.canonical_brands: Dict[str, str] = dict(zip(self.ref_data['brand'].str.lower(), self.ref_data['brand']))
        
        # 3. Brand -> Models mapping for scoped search (O(1) lookup instead of filtering)
        # Group by brand and create a dictionary of sets
        self.brand_to_models: Dict[str, List[str]] = (
            self.ref_data.groupby('brand')['model']
            .apply(lambda x: x.str.lower().tolist())
            .to_dict()
        )

    def normalize(self, brand: str, model: str) -> Dict[str, str]:
        """
        Normalize brand and model names.

        Args:
            brand: Brand name extracted by LLM.
            model: Model name extracted by LLM.

        Returns:
            Dict with 'brand_name' and 'category' (model) normalized.
        """
        normalized_brand = self._normalize_brand(brand)
        normalized_model = self._normalize_model(model, normalized_brand)

        return {
            "brand_name": normalized_brand,
            "category": normalized_model
        }

    def _normalize_brand(self, brand: str) -> str:
        """Fuzzy match brand name."""
        if not brand:
            return "Unknown"
            
        brand_lower = brand.lower()
        
        # Direct match
        if brand_lower in self.valid_brands:
            return self.canonical_brands[brand_lower]
        
        # Fuzzy match
        return self._fuzzy_match(brand_lower, self.valid_brands, self.canonical_brands, cutoff=0.7) or brand

    def _normalize_model(self, model: str, brand: str) -> str:
        """Fuzzy match model name, scoped by brand if possible."""
        if not model or model.lower() == "general":
            return "general"

        model_lower = model.lower()
        
        # Direct match
        if model_lower in self.valid_models:
            return self.canonical_models[model_lower]

        # Determine search scope
        potential_matches = []
        if brand and brand != "Unknown":
            # Use the brand directly if it exists in our map
            if brand in self.brand_to_models:
                potential_matches = self.brand_to_models[brand]
            else:
                # Fallback: try lowercase key if exact match fails
                potential_matches = self.brand_to_models.get(brand.lower(), [])
        
        if not potential_matches:
            potential_matches = list(self.valid_models)

        # Fuzzy match within scope
        match = self._fuzzy_match(model_lower, potential_matches, self.canonical_models, cutoff=0.6)
        return match or model

    def _fuzzy_match(self, text: str, candidates: Iterable[str], canonical_map: Dict[str, str], cutoff: float) -> Optional[str]:
        """Helper for fuzzy matching."""
        matches = difflib.get_close_matches(text, candidates, n=1, cutoff=cutoff)
        if matches:
            return canonical_map[matches[0]]
        return None
