import difflib
from typing import List, Dict, Optional
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
        # Pre-compute valid models lookup for faster access
        self.valid_models = set(self.ref_data['model'].str.lower().unique())
        self.valid_brands = set(self.ref_data['brand'].str.lower().unique())
        
        # Create a mapping from lower case to canonical case
        self.canonical_models = dict(zip(self.ref_data['model'].str.lower(), self.ref_data['model']))
        self.canonical_brands = dict(zip(self.ref_data['brand'].str.lower(), self.ref_data['brand']))

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
        if brand_lower in self.valid_brands:
            return self.canonical_brands[brand_lower]
        
        # Fuzzy match
        matches = difflib.get_close_matches(brand_lower, self.valid_brands, n=1, cutoff=0.7)
        if matches:
            return self.canonical_brands[matches[0]]
        
        return brand  # Return original if no match found (or handle as Unknown)

    def _normalize_model(self, model: str, brand: str) -> str:
        """Fuzzy match model name, scoped by brand if possible."""
        if not model or model.lower() == "general":
            return "general"

        model_lower = model.lower()
        
        # Direct match
        if model_lower in self.valid_models:
            return self.canonical_models[model_lower]

        # Filter reference models by brand if brand is known
        if brand and brand != "Unknown":
            brand_models = self.ref_data[self.ref_data['brand'] == brand]['model'].str.lower().tolist()
            potential_matches = brand_models
        else:
            potential_matches = list(self.valid_models)

        # Fuzzy match
        matches = difflib.get_close_matches(model_lower, potential_matches, n=1, cutoff=0.6)
        if matches:
            return self.canonical_models[matches[0]]

        return model
