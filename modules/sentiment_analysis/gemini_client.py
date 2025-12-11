import logging
import time
import threading
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from typing import Optional, List

logger = logging.getLogger(__name__)

class GeminiClient:
    """Client for interacting with Google's Gemini API with multi-key support."""

    def __init__(self, api_keys: List[str], model_name: str = "gemini-2.5-flash"):
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

    def _get_next_key(self) -> str:
        """Get next API key using round-robin rotation (thread-safe)."""
        with self._lock:
            key = self.api_keys[self._current_key_index]
            self._current_key_index = (self._current_key_index + 1) % self.key_count
            return key

    def analyze_sentiment(self, prompt: str, system_prompt: Optional[str] = None) -> Optional[str]:
        """
        Sends a prompt to Gemini and returns the text response (Synchronous).
        """
        # ... (existing sync implementation wrapped or kept for backward compatibility)
        # For simplicity, we'll keep the sync implementation as is for now
        # or we could make it call the async one via asyncio.run() if we wanted to unify logic
        
        max_retries = 3
        full_prompt = prompt
        if system_prompt:
            full_prompt = f"{system_prompt}\n\n{prompt}"

        for attempt in range(max_retries):
            try:
                api_key = self._get_next_key()
                genai.configure(api_key=api_key)
                model = genai.GenerativeModel(self.model_name)
                
                response = model.generate_content(
                    full_prompt,
                    generation_config={"response_mime_type": "application/json"},
                    safety_settings=self.safety_settings
                )
                return response.text
                
            except Exception as e:
                error_msg = str(e).lower()
                if 'rate' in error_msg or 'quota' in error_msg or '429' in error_msg:
                    logger.warning(f"Rate limit hit on key {self._current_key_index}, rotating...")
                    self._get_next_key()
                
                wait_time = 2 ** attempt
                logger.warning(f"Gemini API call failed (attempt {attempt+1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
        
        logger.error("Max retries reached for Gemini API call.")
        return None

    async def analyze_sentiment_async(self, prompt: str, system_prompt: Optional[str] = None) -> Optional[str]:
        """
        Sends a prompt to Gemini and returns the text response (Asynchronous).
        Rotates through available API keys and retries on transient errors.
        """
        import asyncio
        
        max_retries = 3
        full_prompt = prompt
        if system_prompt:
            full_prompt = f"{system_prompt}\n\n{prompt}"

        for attempt in range(max_retries):
            try:
                # Get next API key
                api_key = self._get_next_key()
                
                # Configure model
                # Note: genai.configure is global, which might be tricky in async if threads share it.
                # However, creating a client instance per key or passing api_key to GenerativeModel (if supported) is safer.
                # The current SDK allows passing api_key to configure.
                # Ideally, we should lock the configuration step or use separate client objects if the SDK supports it.
                # For now, we'll assume standard usage but be aware of potential race conditions if multiple threads configure global state.
                # BUT, since we are using asyncio (single thread), global state changes are sequential between awaits.
                
                genai.configure(api_key=api_key)
                model = genai.GenerativeModel(self.model_name)
                
                # Async generation
                response = await model.generate_content_async(
                    full_prompt,
                    generation_config={"response_mime_type": "application/json"},
                    safety_settings=self.safety_settings
                )
                return response.text
                
            except Exception as e:
                error_msg = str(e).lower()
                
                if 'rate' in error_msg or 'quota' in error_msg or '429' in error_msg:
                    logger.warning(f"Rate limit hit (Async), rotating key...")
                    self._get_next_key()
                
                wait_time = 2 ** attempt
                logger.warning(f"Gemini Async API call failed (attempt {attempt+1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
        
        logger.error("Max retries reached for Gemini Async API call.")
        return None
