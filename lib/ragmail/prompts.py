"""LLM prompt templates for email search and RAG."""

from dataclasses import dataclass
from typing import Sequence

from ragmail.llm.base import Message


@dataclass
class RAGPrompt:
    """Prompt for RAG-based email question answering."""

    system_message: str = """You are an expert email search assistant. Your role is to help users find information in their emails by providing accurate, concise answers based on the retrieved email context.

Guidelines:
- Base your answers ONLY on the provided email context
- Cite specific emails when referencing them (e.g., "In the email from John on Jan 15...")
- If the answer cannot be found in the emails, clearly state "I don't have enough information"
- Be concise but thorough - include relevant details without rambling
- If multiple emails contain relevant information, synthesize them into a coherent answer
- When citing dates, use the format from the emails (e.g., "January 15, 2023")

Your goal is to be helpful and accurate while strictly adhering to the provided context."""

    user_template: str = """## Retrieved Emails

{context}

## User Question

{question}

## Instructions

Please answer the user's question based on the retrieved emails above. Cite specific emails when relevant. If you cannot answer based on the provided emails, say "I don't have enough information to answer this question."

Answer:"""

    def format(self, question: str, emails: Sequence[dict]) -> list[Message]:
        """Format the prompt with context emails.

        Args:
            question: User's question
            emails: Retrieved email records

        Returns:
            List of messages for the LLM
        """
        context = self._format_context(emails)
        user_content = self.user_template.format(
            context=context,
            question=question,
        )

        return [
            Message(role="system", content=self.system_message),
            Message(role="user", content=user_content),
        ]

    def _format_context(self, emails: Sequence[dict]) -> str:
        """Format emails into context string."""
        if not emails:
            return "No relevant emails found."

        formatted = []
        for i, email in enumerate(emails, 1):
            date_str = email.get("date", "Unknown date")
            if hasattr(date_str, "strftime"):
                date_str = date_str.strftime("%B %d, %Y")

            from_str = email.get("from_name") or email.get(
                "from_address", "Unknown sender"
            )
            subject = email.get("subject", "No subject")
            body = email.get("body_plain", "")[:500]  # Truncate for context window

            formatted.append(
                f"### Email {i}\n"
                f"**From:** {from_str}\n"
                f"**Date:** {date_str}\n"
                f"**Subject:** {subject}\n"
                f"**Body:** {body}\n"
            )

        return "\n\n".join(formatted)


@dataclass
class QueryExpansionPrompt:
    """Prompt for expanding and decomposing complex queries."""

    system_message: str = """You are an expert at analyzing search queries and breaking them down into simpler, more effective search components.

Your task is to analyze a user's natural language query about their emails and:
1. Identify the main intent (search, count, aggregation, summary)
2. Extract key entities (names, dates, topics)
3. Break down complex multi-part queries into simpler sub-queries
4. Suggest search terms that will retrieve relevant emails

Guidelines:
- Be precise and extract only what's actually mentioned in the query
- Don't make assumptions beyond what the user asked
- For date references, convert relative terms ("last year", "recently") to specific years when possible
- Identify if the query asks for a count, list, or summary
- Suggest alternative search terms that might capture the same concept"""

    user_template: str = """## User Query

"{query}"

## Analysis

Please analyze this email search query and provide:

1. **Intent**: What type of result is being requested? (search/count/aggregation/summary)
2. **Key Entities**: List any people, dates, or topics mentioned
3. **Sub-queries**: Break complex queries into simpler parts (if applicable)
4. **Search Terms**: Suggest 3-5 effective search terms/queries to find relevant emails

Format your response as JSON:
```json
{{
    "intent": "search|count|aggregation|summary",
    "entities": {{
        "people": ["name1", "name2"],
        "dates": ["2023", "January 2023"],
        "topics": ["topic1", "topic2"]
    }},
    "sub_queries": ["query1", "query2"],
    "search_terms": ["term1", "term2", "term3"]
}}
```

Response:"""

    def format(self, query: str) -> list[Message]:
        """Format the query expansion prompt.

        Args:
            query: User's search query

        Returns:
            List of messages for the LLM
        """
        user_content = self.user_template.format(query=query)

        return [
            Message(role="system", content=self.system_message),
            Message(role="user", content=user_content),
        ]


@dataclass
class QueryPlanPrompt:
    """Prompt for generating structured search plans."""

    system_message: str = """You are a query planner for an email database.

Your task is to convert a natural language query into a structured JSON plan
that clearly separates:
- semantic (vector) search text
- full-text search text
- structured metadata filters

Return JSON only. Do not include code fences or extra text."""

    user_template: str = """Query:
"{query}"

Return JSON with this schema:
{{
  "intent": "search|count|aggregation",
  "vector_query": "semantic query for embeddings",
  "fts_query": "full-text query string",
  "filters": {{
    "from": "sender name or email",
    "to": "recipient name or email",
    "from_domain": "example.com",
    "year": 2026,
    "month": 1,
    "start_date": "YYYY-MM-DD",
    "end_date": "YYYY-MM-DD",
    "has_attachment": true,
    "labels": ["Label A", "Label B"]
  }},
  "aggregation": {{ "field": "from_address|to_address" }},
  "use_vector": true,
  "use_fts": true
}}

Rules:
- Use only fields that are directly supported by the query.
- If a filter is not mentioned, omit it from filters.
- Prefer quoted phrases in fts_query for exact names when helpful.
"""

    def format(self, query: str) -> list[Message]:
        """Format the query planner prompt."""
        user_content = self.user_template.format(query=query)

        return [
            Message(role="system", content=self.system_message),
            Message(role="user", content=user_content),
        ]


@dataclass
class SummarizationPrompt:
    """Prompt for summarizing email threads or conversations."""

    system_message: str = """You are an expert at summarizing email conversations. Your summaries should capture the key points, decisions, and action items while being concise and clear.

Guidelines:
- Start with a one-sentence overview of the conversation
- Identify the main topic(s) discussed
- Note any decisions made or conclusions reached
- List any action items with their owners (if mentioned)
- Mention key dates or deadlines referenced
- Keep the summary to 3-5 bullet points plus the overview
- Use neutral, professional language"""

    user_template: str = """## Emails to Summarize

{emails}

## Instructions

Please provide a concise summary of this email conversation. Focus on key points, decisions, and any action items.

Summary:"""

    def format(self, emails: Sequence[dict]) -> list[Message]:
        """Format the summarization prompt.

        Args:
            emails: Email records to summarize

        Returns:
            List of messages for the LLM
        """
        emails_text = self._format_emails(emails)
        user_content = self.user_template.format(emails=emails_text)

        return [
            Message(role="system", content=self.system_message),
            Message(role="user", content=user_content),
        ]

    def _format_emails(self, emails: Sequence[dict]) -> str:
        """Format emails for summarization."""
        formatted = []
        for i, email in enumerate(emails, 1):
            date_str = email.get("date", "Unknown date")
            if hasattr(date_str, "strftime"):
                date_str = date_str.strftime("%B %d, %Y")

            from_str = email.get("from_name") or email.get(
                "from_address", "Unknown sender"
            )
            subject = email.get("subject", "No subject")
            body = email.get("body_plain", "")[:800]  # Longer for summarization

            formatted.append(
                f"--- Email {i} ---\n"
                f"From: {from_str}\n"
                f"Date: {date_str}\n"
                f"Subject: {subject}\n"
                f"\n{body}\n"
            )

        return "\n".join(formatted)


# Singleton instances for easy use
RAG_PROMPT = RAGPrompt()
QUERY_EXPANSION_PROMPT = QueryExpansionPrompt()
QUERY_PLAN_PROMPT = QueryPlanPrompt()
SUMMARIZATION_PROMPT = SummarizationPrompt()
