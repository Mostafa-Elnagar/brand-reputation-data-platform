from typing import List, Dict, Optional, Any

class PromptBuilder:
    """Builds prompts for Gemini sentiment analysis with context and reference constraints."""

    def __init__(self, reference_brands: List[str]):
        """
        Initialize the prompt builder.
        
        Args:
            reference_brands: List of valid brand names to guide the model.
        """
        self.reference_brands = reference_brands
        self.system_prompt = self._build_system_prompt()

    def _build_system_prompt(self) -> str:
        """Constructs the system prompt with reference data."""
        brands_list = ", ".join(self.reference_brands[:50]) # Limit to top brands to save tokens
        return f"""
You are a sentiment analysis expert specializing in smartphone reviews. 
Your task is to analyze Reddit comments and extract targeted sentiment for specific features.

You must identify:
1. Brand Name: Must be one of the major smartphone brands if applicable (e.g., {brands_list}).
2. Product Name: The specific model name (e.g., "iPhone 15 Pro", "Galaxy S24 Ultra") or "general" if referring to the brand overall.
3. Aspect: The feature discussed (e.g., "camera", "battery", "display", "performance", "price", "software", "build_quality", "design", "other").
4. Sentiment Score: A float between 0.0 (very negative) and 1.0 (very positive).
5. Quote: Extract the exact phrase or sentence from the comment that expresses this sentiment (max 200 characters).

Output Format: JSON array of objects.
Example:
[
  {{
    "brand_name": "Samsung",
    "product_name": "Galaxy S25 FE",
    "aspect": "price",
    "sentiment_score": 0.8,
    "quote": "I was going to get the galaxy s25 fe for $99 but boost's network is terrible"
  }}
]
"""

    def build_analysis_prompt(self, comment_body: str, submission_title: str, submission_selftext: str, parent_body: Optional[str] = None) -> str:
        """
        Builds the user prompt including context.

        Args:
            comment_body: The text of the comment to analyze.
            submission_title: Title of the parent submission.
            submission_selftext: Body of the parent submission.
            parent_body: Text of the parent comment (if this is a reply).

        Returns:
            Formatted prompt string.
        """
        context_parts = [f"Submission Title: {submission_title}"]
        
        if submission_selftext and submission_selftext != "[deleted]" and submission_selftext != "[removed]":
            # Truncate selftext to avoid excessive token usage, keep first 200 chars
            context_parts.append(f"Submission Context: {submission_selftext[:200]}...")
            
        if parent_body and parent_body != "[deleted]" and parent_body != "[removed]":
            context_parts.append(f"Parent Comment: {parent_body}")
            
        context_str = "\n".join(context_parts)

        return f"""
Context:
{context_str}

Target Comment to Analyze:
"{comment_body}"

Analyze the "Target Comment" for sentiment. Use the context to resolve coreferences (e.g., "it", "the phone") but only score the sentiment expressed in the target comment itself.
"""
