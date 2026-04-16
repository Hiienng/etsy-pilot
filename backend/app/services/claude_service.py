import anthropic
from ..core.config import get_settings

settings = get_settings()
client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """You are an expert Etsy SEO specialist.
Optimize listings to maximize visibility and conversion on Etsy.
- Titles: front-load primary keywords, keep under 140 chars, natural language
- Tags: exactly 13 tags, 1-20 chars each, comma-separated, high-search-volume keywords
- Descriptions: engaging, keyword-rich first 160 chars, include personalization details
Always respond in English unless the original content is in another language."""


async def optimize_title(title: str, tags: str | None, description: str | None) -> str:
    prompt = f"""Optimize this Etsy listing title for SEO.

Current title: {title}
Current tags: {tags or "N/A"}
Current description excerpt: {(description or "")[:200]}

Return ONLY the optimized title, no explanation."""

    message = client.messages.create(
        model=settings.CLAUDE_MODEL,
        max_tokens=200,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip()


async def optimize_tags(title: str, tags: str | None, description: str | None) -> str:
    prompt = f"""Generate 13 optimized Etsy tags for this listing.

Title: {title}
Current tags: {tags or "N/A"}
Description excerpt: {(description or "")[:200]}

Return ONLY a comma-separated list of exactly 13 tags, no explanation."""

    message = client.messages.create(
        model=settings.CLAUDE_MODEL,
        max_tokens=300,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip()


async def optimize_description(title: str, tags: str | None, description: str | None, personalization: str | None) -> str:
    prompt = f"""Write an optimized Etsy listing description.

Title: {title}
Tags: {tags or "N/A"}
Current description: {description or "N/A"}
Personalization options: {personalization or "N/A"}

Write a compelling, SEO-optimized description. Start with the strongest keyword phrase. Include personalization details naturally. Keep it under 2000 chars."""

    message = client.messages.create(
        model=settings.CLAUDE_MODEL,
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip()
