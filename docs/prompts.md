# Prompt Documentation

This document describes the LLM prompt templates used in the email search system for RAG (Retrieval-Augmented Generation), query enhancement, query planning, and summarization.

## Table of Contents

- [Overview](#overview)
- [RAGPrompt](#ragprompt)
- [QueryExpansionPrompt](#queryexpansionprompt)
- [QueryPlanPrompt](#queryplanprompt)
- [SummarizationPrompt](#summarizationprompt)
- [Prompt Best Practices](#prompt-best-practices)
- [Customization](#customization)

## Overview

The system uses four main prompt types:

1. **RAGPrompt**: Generates contextual answers based on retrieved emails
2. **QueryExpansionPrompt**: Decomposes complex queries for better retrieval
3. **QueryPlanPrompt**: Produces a structured JSON plan (vector text, FTS text, filters)
4. **SummarizationPrompt**: Summarizes email threads or conversations

All prompts follow best practices:
- Clear system instructions defining the AI's role
- Structured user templates with variable substitution
- Explicit guidelines for output format
- Examples where helpful
- Constraints to prevent hallucination

## RAGPrompt

**Purpose**: Generate contextual answers based on retrieved emails while citing sources.

**Location**: `python/lib/ragmail/prompts.py` - `RAGPrompt` class

### System Message

```
You are an expert email search assistant. Your role is to help users 
find information in their emails by providing accurate, concise answers 
based on the retrieved email context.

Guidelines:
- Base your answers ONLY on the provided email context
- Cite specific emails when referencing them (e.g., "In the email from John on Jan 15...")
- If the answer cannot be found in the emails, clearly state "I don't have enough information"
- Be concise but thorough - include relevant details without rambling
- If multiple emails contain relevant information, synthesize them into a coherent answer
- When citing dates, use the format from the emails (e.g., "January 15, 2023")

Your goal is to be helpful and accurate while strictly adhering to the provided context.
```

**Key Elements**:
- **Role definition**: Expert email search assistant
- **Primary constraint**: Base answers ONLY on provided context
- **Citation requirement**: Must cite specific emails
- **Fallback instruction**: Clear statement for insufficient information
- **Tone guidance**: Concise but thorough

### User Template

```
## Retrieved Emails

{context}

## User Question

{question}

## Instructions

Please answer the user's question based on the retrieved emails above. 
Cite specific emails when relevant. If you cannot answer based on the 
provided emails, say "I don't have enough information to answer this question."

Answer:
```

**Variables**:
- `{context}`: Formatted email records (see Context Formatting below)
- `{question}`: User's original query

### Context Formatting

Emails are formatted as:

```
### Email {n}
**From:** {sender_name_or_email}
**Date:** {formatted_date}
**Subject:** {subject}
**Body:** {truncated_body}
```

**Formatting Details**:
- Body truncated to 500 characters to fit context window
- Dates formatted as "January 15, 2023"
- Sender name preferred over email address
- Sequential numbering (Email 1, Email 2, etc.)

### Example Usage

```python
from ragmail.prompts import RAG_PROMPT

messages = RAG_PROMPT.format(
    question="what did John say about the deadline?",
    emails=[
        {
            "from_name": "John Doe",
            "from_address": "john@example.com",
            "date": "2024-01-15",
            "subject": "Project Update",
            "body_plain": "The deadline has been extended to March..."
        }
    ]
)

# Messages ready for LLM
# [
#   Message(role="system", content=system_message),
#   Message(role="user", content=user_content)
# ]
```

### Example Output

**Input Question**: "what did John say about the deadline?"

**Retrieved Emails**:
1. From: John Doe, Date: Jan 15, Subject: Timeline Update
2. From: John Doe, Date: Jan 20, Subject: Re: Timeline Update

**Expected LLM Response**:
```
John mentioned the project deadline in two emails:

1. In his email from January 15, 2024, John wrote that "the deadline 
   has been extended to March 1st due to testing delays."

2. In his follow-up email on January 20, John confirmed that "the team 
   agreed on the March 1st deadline and management has approved it."

Key point: The deadline was extended from February 15 to March 1, 2024.
```

## QueryExpansionPrompt

**Purpose**: Decompose complex natural language queries into structured components for better retrieval.

**Location**: `python/lib/ragmail/prompts.py` - `QueryExpansionPrompt` class

### System Message

```
You are an expert at analyzing search queries and breaking them down 
into simpler, more effective search components.

Your task is to analyze a user's natural language query about their 
emails and:
1. Identify the main intent (search, count, aggregation, summary)
2. Extract key entities (names, dates, topics)
3. Break down complex multi-part queries into simpler sub-queries
4. Suggest search terms that will retrieve relevant emails

Guidelines:
- Be precise and extract only what's actually mentioned in the query
- Don't make assumptions beyond what the user asked
- For date references, convert relative terms ("last year", "recently") 
  to specific years when possible
- Identify if the query asks for a count, list, or summary
- Suggest alternative search terms that might capture the same concept
```

**Key Elements**:
- **Role definition**: Query analysis expert
- **Four main tasks**: Intent, entities, sub-queries, search terms
- **Precision guideline**: Extract only what's mentioned
- **Date handling**: Convert relative to absolute dates
- **Query type identification**: Distinguish search/count/aggregation/summary

### User Template

```
## User Query

"{query}"

## Analysis

Please analyze this email search query and provide:

1. **Intent**: What type of result is being requested? 
   (search/count/aggregation/summary)
2. **Key Entities**: List any people, dates, or topics mentioned
3. **Sub-queries**: Break complex queries into simpler parts (if applicable)
4. **Search Terms**: Suggest 3-5 effective search terms/queries to find 
   relevant emails

Format your response as JSON:
```json
{
    "intent": "search|count|aggregation|summary",
    "entities": {
        "people": ["name1", "name2"],
        "dates": ["2023", "January 2023"],
        "topics": ["topic1", "topic2"]
    },
    "sub_queries": ["query1", "query2"],
    "search_terms": ["term1", "term2", "term3"]
}
```

Response:
```

## QueryPlanPrompt

**Purpose**: Generate a strict JSON query plan for hybrid search.

**Location**: `python/lib/ragmail/prompts.py` - `QueryPlanPrompt` class

### System Message (Summary)

- Role: query planner for an email database
- Output: JSON only (no code fences)
- Split: semantic vector query, full-text query, and metadata filters

### JSON Schema

```json
{
  "intent": "search|count|aggregation",
  "vector_query": "semantic query for embeddings",
  "fts_query": "full-text query string",
  "filters": {
    "from": "sender name or email",
    "to": "recipient name or email",
    "from_domain": "example.com",
    "year": 2026,
    "month": 1,
    "start_date": "YYYY-MM-DD",
    "end_date": "YYYY-MM-DD",
    "has_attachment": true,
    "labels": ["Label A", "Label B"]
  },
  "aggregation": { "field": "from_address|to_address" },
  "use_vector": true,
  "use_fts": true
}
```

### Example Usage

```python
from ragmail.prompts import QUERY_PLAN_PROMPT

messages = QUERY_PLAN_PROMPT.format(
    query="how many emails from Anthropic in January 2026"
)
```

The query planner returns JSON that is validated and converted into a safe
search plan. No arbitrary code is executed.

**Variables**:
- `{query}`: User's natural language query

**Output Format**: JSON with structured fields

### Example Usage

```python
from ragmail.prompts import QUERY_EXPANSION_PROMPT

messages = QUERY_EXPANSION_PROMPT.format(
    query="emails from John in 2023 about the budget"
)
```

### Example Output

**Input Query**: "emails from John in 2023 about the budget"

**Expected LLM Response**:
```json
{
    "intent": "search",
    "entities": {
        "people": ["John"],
        "dates": ["2023"],
        "topics": ["budget"]
    },
    "sub_queries": [
        "emails from John",
        "emails in 2023",
        "emails about budget"
    ],
    "search_terms": [
        "John budget 2023",
        "John financial planning",
        "budget discussion John"
    ]
}
```

### Intent Types

| Intent | Description | Example Query |
|--------|-------------|---------------|
| `search` | Find specific emails | "emails from John about project" |
| `count` | Count emails matching criteria | "how many emails in 2023" |
| `aggregation` | Aggregate/group results | "who did I email most" |
| `summary` | Summarize content | "summarize my emails with Sarah" |

### Entity Extraction

**People**:
- Names: "John", "Sarah Johnson"
- Email addresses: "john@example.com"
- Roles: "my manager", "the team"

**Dates**:
- Absolute: "2023", "January 2024"
- Relative: "last year" → "2023", "this month" → "February 2024"
- Ranges: "Q1 2024", "between Jan and March"

**Topics**:
- Single words: "budget", "meeting"
- Phrases: "project deadline", "quarterly review"
- Concepts: "remote work policy"

## SummarizationPrompt

**Purpose**: Summarize email threads or conversations into concise, structured summaries.

**Location**: `python/lib/ragmail/prompts.py` - `SummarizationPrompt` class

### System Message

```
You are an expert at summarizing email conversations. Your summaries 
should capture the key points, decisions, and action items while being 
concise and clear.

Guidelines:
- Start with a one-sentence overview of the conversation
- Identify the main topic(s) discussed
- Note any decisions made or conclusions reached
- List any action items with their owners (if mentioned)
- Mention key dates or deadlines referenced
- Keep the summary to 3-5 bullet points plus the overview
- Use neutral, professional language
```

**Key Elements**:
- **Role definition**: Email summarization expert
- **Structure guidance**: Overview + 3-5 bullet points
- **Content requirements**: Topics, decisions, action items, dates
- **Tone**: Neutral and professional

### User Template

```
## Emails to Summarize

{emails}

## Instructions

Please provide a concise summary of this email conversation. Focus on 
key points, decisions, and any action items.

Summary:
```

**Variables**:
- `{emails}`: Formatted email thread (see Email Formatting below)

### Email Formatting

Emails are formatted as:

```
--- Email {n} ---
From: {sender}
Date: {date}
Subject: {subject}

{body}
```

**Formatting Details**:
- Body truncated to 800 characters (longer than RAG for more context)
- Clear visual separation between emails
- Chronological order preserved

### Example Usage

```python
from ragmail.prompts import SUMMARIZATION_PROMPT

messages = SUMMARIZATION_PROMPT.format(emails=[
    {
        "from_name": "Sarah",
        "date": "2024-01-15",
        "subject": "Budget Proposal",
        "body_plain": "Attached is the budget proposal for Q1..."
    },
    {
        "from_name": "You",
        "date": "2024-01-16",
        "subject": "Re: Budget Proposal",
        "body_plain": "Thanks Sarah. A few questions..."
    }
])
```

### Example Output

**Input**: Thread about budget proposal

**Expected LLM Response**:
```
Overview: Sarah submitted a Q1 budget proposal on January 15, and you 
requested clarifications on two line items.

Key Points:
• Sarah proposed a $50K budget increase for marketing initiatives
• The request includes $15K for digital advertising, $20K for events, 
  and $15K for content creation
• You asked for clarification on the events budget and content creation 
  timeline
• Sarah agreed to provide detailed breakdown by January 18

Decisions: None finalized yet - awaiting additional information

Action Items:
• Sarah: Provide detailed events budget breakdown (by Jan 18)
• Sarah: Clarify content creation timeline and deliverables
• You: Review revised proposal once received
```

## Prompt Best Practices

### Design Principles

1. **Clear Role Definition**
   - Always start with "You are an expert..."
   - Define the AI's persona and expertise
   - Set clear boundaries

2. **Explicit Constraints**
   - State what the AI should NOT do
   - Use phrases like "Base your answers ONLY on..."
   - Provide fallback instructions

3. **Structured Output**
   - Use clear section headers
   - Specify format (JSON, markdown, etc.)
   - Include examples

4. **Context Management**
   - Truncate long content to fit context window
   - Use consistent formatting
   - Number items for easy reference

5. **Citation Requirements**
   - Explicitly ask for source citations
   - Provide format for citations
   - Ensure traceability

### Anti-Patterns to Avoid

❌ **Vague Instructions**:
```
Tell me about the emails.
```

✅ **Specific Instructions**:
```
Provide a concise summary focusing on key points, decisions, and action items.
```

❌ **No Constraints**:
```
Answer based on these emails.
```

✅ **Clear Constraints**:
```
Base your answers ONLY on the provided email context. If the answer cannot 
be found, clearly state "I don't have enough information."
```

❌ **Unstructured Output**:
```
What did John say?
```

✅ **Structured Output**:
```
Format your response as JSON with fields: intent, entities, sub_queries, 
search_terms.
```

## Customization

### Modifying Prompts

You can customize prompts by creating a new instance with modified templates:

```python
from ragmail.prompts import RAGPrompt

# Custom RAG prompt with different tone
custom_rag = RAGPrompt(
    system_message="""You are a helpful assistant... [custom instructions]""",
    user_template="""## Context
{context}

## Question
{question}

[custom instructions]

Answer:"""
)

# Use custom prompt
messages = custom_rag.format(question="...", emails=[...])
```

### Adding New Prompts

To add a new prompt type:

1. **Create dataclass**:
```python
@dataclass
class MyCustomPrompt:
    system_message: str = "..."
    user_template: str = "..."
    
    def format(self, **kwargs) -> list[Message]:
        # Formatting logic
        pass
```

2. **Add singleton instance**:
```python
MY_CUSTOM_PROMPT = MyCustomPrompt()
```

3. **Use in code**:
```python
from ragmail.prompts import MY_CUSTOM_PROMPT

messages = MY_CUSTOM_PROMPT.format(...)
```

### Testing Prompts

Always test prompt changes:

```python
def test_custom_prompt():
    prompt = MyCustomPrompt()
    messages = prompt.format(test_data)
    
    # Verify structure
    assert len(messages) == 2
    assert messages[0].role == "system"
    assert messages[1].role == "user"
    
    # Test with LLM (integration test)
    response = llm.complete(messages)
    assert validate_response(response)
```

## Version Control

Prompt changes should be versioned:

```python
@dataclass
class RAGPrompt:
    """Prompt for RAG-based email question answering.
    
    Version: 1.2
    Changes:
    - 1.2: Added date formatting requirement
    - 1.1: Increased body truncation to 500 chars
    - 1.0: Initial version
    """
```

## Performance Considerations

1. **Context Window**: Truncate email bodies appropriately
   - RAG: 500 characters (fit more emails)
   - Summarization: 800 characters (more detail per email)

2. **Token Count**: Monitor prompt sizes
   - System message: ~200 tokens
   - Each email: ~150-300 tokens
   - Keep total under model's context limit

3. **Response Format**: JSON adds tokens but improves reliability
   - Use JSON for structured data
   - Use plain text for open-ended responses

## Related Documentation

- [Architecture Overview](DESIGN.md): How prompts fit into the RAG pipeline
- [Examples](EXAMPLES.md): Real-world query examples and responses
- [API Documentation]: OpenAPI docs for programmatic access
