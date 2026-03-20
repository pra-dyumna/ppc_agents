"""
Gemini AI client
─────────────────
Wraps google-generativeai with:
- Forced JSON output mode
- Max tokens set high enough to never truncate
- finish_reason check — catches truncation before JSON parse
- Retry on malformed JSON (up to 2 retries)
"""
import json
import logging
import google.generativeai as genai
from config import settings

logger = logging.getLogger(__name__)

genai.configure(api_key=settings.gemini_api_key)

# gemini-1.5-flash supports up to 8192 output tokens.
# Root cause of "Unterminated string" errors: max_output_tokens was 4096,
# Gemini wrote verbose JSON and got cut off mid-string.
# Fix: raise to 8192 (the model's hard ceiling for flash).
_model = genai.GenerativeModel(
    model_name="gemini-2.5-flash",
    generation_config=genai.GenerationConfig(
        response_mime_type="application/json",
        temperature=0.2,
        max_output_tokens=8192,   # raised from 4096 — prevents truncation
    ),
)


def ask_gemini(system_prompt: str, user_prompt: str, retries: int = 2) -> dict:
    """
    Send a prompt to Gemini and return parsed JSON dict.
    Raises RuntimeError if truncated or JSON cannot be parsed after retries.
    """
    full_prompt = f"{system_prompt}\n\n{user_prompt}"

    last_error = None
    for attempt in range(retries + 1):
        try:
            response = _model.generate_content(full_prompt)

            # Check finish_reason BEFORE trying to parse.
            # If MAX_TOKENS → output was cut off → JSON will always be broken.
            # Catch this early so the error message is clear.
            candidate = response.candidates[0] if response.candidates else None
            if candidate:
                finish_reason = candidate.finish_reason.name  # "STOP" | "MAX_TOKENS" | "SAFETY" etc.
                if finish_reason == "MAX_TOKENS":
                    logger.error(
                        "Gemini hit MAX_TOKENS on attempt %d — output truncated. "
                        "Prompt may be too large. Try reducing ai_*_top_n in config.",
                        attempt + 1
                    )
                    raise RuntimeError(
                        "Gemini output was truncated (hit max_output_tokens=8192). "
                        "Reduce the number of rows sent to AI via ai_campaign_top_n / "
                        "ai_search_term_top_n settings in your .env file."
                    )
                elif finish_reason not in ("STOP", "1"):
                    logger.warning("Gemini finish_reason=%s on attempt %d", finish_reason, attempt + 1)

            raw = response.text.strip()

            # Strip accidental markdown fences
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()

            data = json.loads(raw)
            logger.info(
                "Gemini OK (attempt %d) | finish=%s | output_chars=%d",
                attempt + 1,
                finish_reason if candidate else "unknown",
                len(raw),
            )
            return data

        except RuntimeError:
            raise   # don't retry truncation errors — they'll keep failing

        except json.JSONDecodeError as e:
            last_error = e
            logger.warning(
                "Gemini JSON parse failed attempt %d: %s | raw_preview: %s",
                attempt + 1, e, raw[:200] if 'raw' in dir() else "N/A"
            )
            if attempt < retries:
                logger.info("Retrying Gemini call...")
                continue

    raise RuntimeError(f"Gemini returned invalid JSON after {retries + 1} attempts: {last_error}")