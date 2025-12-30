import logging
import time
import threading
import asyncio
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from typing import Optional, List

logger = logging.getLogger(__name__)

class GeminiClient:
    """Client for interacting with Google's Gemini API with multi-key support."""

    def __init__(self, api_keys: List[str], model_name: str = "gemma-3-12b-it"):
        """
        Initialize Gemini client with multiple API keys for load distribution.
        
        Args:
            api_keys: List of Gemini API keys for round-robin rotation
            model_name: Gemini model to use
        """
        if not api_keys:
            raise ValueError("At least one API key must be provided")
        
        self.api_keys = api_keys
        self.model_name = model_name
        self.key_count = len(api_keys)
        
        # Thread-safe key rotation
        self._lock = threading.Lock()
        self._current_key_index = 0
        
        # Relaxed safety settings to avoid over-blocking reasonable content
        self.safety_settings = {
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        }
        
        logger.info(f"GeminiClient initialized with {self.key_count} API keys")

    def _get_next_key(self) -> tuple[str, int]:
        """Get next API key and its index using round-robin rotation (thread-safe)."""
        with self._lock:
            index = self._current_key_index
            key = self.api_keys[index]
            self._current_key_index = (self._current_key_index + 1) % self.key_count
            return key, index

    async def analyze_sentiment_async(self, prompt: str, system_prompt: Optional[str] = None) -> Optional[str]:
        """
        Sends a prompt to Gemini and returns the text response (Asynchronous).
        Rotates through available API keys and retries on transient errors.
        """
        max_retries = 3  # Reverted to 3 as requested
        full_prompt = prompt
        if system_prompt:
            full_prompt = f"{system_prompt}\n\n{prompt}"

        import random
        
        for attempt in range(max_retries):
            key_index = -1
            try:
                # Get next API key
                api_key, key_index = self._get_next_key()
                
                # Configure model
                genai.configure(api_key=api_key)
                model = genai.GenerativeModel(self.model_name)
                
                # Async generation
                # Note: gemma-3-27b-it does not support response_mime_type="application/json"
                response = await model.generate_content_async(
                    full_prompt,
                    safety_settings=self.safety_settings
                )
                return response.text
                
            except Exception as e:
                error_msg = str(e).lower()
                
                # Handle Rate Limits
                if 'rate' in error_msg or 'quota' in error_msg or '429' in error_msg:
                    logger.warning(f"Rate limit hit on key index {key_index}, rotating key...")
                    
                    # Check for explicit retry delay in error message
                    import re
                    match = re.search(r'retry in (\d+(\.\d+)?)s', error_msg)
                    if match:
                        wait_time = float(match.group(1)) + 1 + random.uniform(0, 1) # Add buffer + jitter
                        logger.warning(f"Quota exceeded. Waiting {wait_time:.2f}s as requested by API...")
                        await asyncio.sleep(wait_time)
                        continue 

                # Exponential backoff with jitter: 2, 4, 8, 16... + random
                base_delay = 2
                wait_time = (base_delay * (2 ** attempt)) + random.uniform(0, 1)
                # Cap max wait time to 60s
                wait_time = min(wait_time, 60.0)
                
                logger.warning(f"Gemini Async API call failed (attempt {attempt+1}/{max_retries}) with key index {key_index}: {e}. Retrying in {wait_time:.2f}s...")
                await asyncio.sleep(wait_time)
        
        logger.error("Max retries reached for Gemini Async API call.")
        return None
