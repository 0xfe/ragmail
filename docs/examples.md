# Usage Examples

This document provides comprehensive examples of queries and expected responses for the email search tool.

## Table of Contents

- [Basic Search Examples](#basic-search-examples)
- [RAG (AI-Powered) Examples](#rag-ai-powered-examples)
- [Complex Query Examples](#complex-query-examples)
- [Aggregation Queries](#aggregation-queries)
- [Date-Based Queries](#date-based-queries)
- [Sender/Recipient Queries](#senderrecipient-queries)
- [Advanced Usage Patterns](#advanced-usage-patterns)
- [Query Planning](#query-planning)

## Basic Search Examples

### Simple Keyword Search

```bash
ragmail search "meeting"
```

**Expected Output:**
```
Found 15 emails (showing top 10):

1. Team Meeting Notes - Jan 15, 2024
   From: Sarah Johnson <sarah@company.com>
   Subject: Weekly Team Meeting - Action Items
   Preview: Here are the notes from today's meeting...

2. Re: Meeting Tomorrow - Jan 14, 2024
   From: John Doe <john@example.com>
   Subject: Re: Meeting Tomorrow
   Preview: Looking forward to our meeting tomorrow...

...
```

### Semantic Search (Concept-Based)

```bash
ragmail search "project deadline concerns"
```

**Expected Output:**
```
Found 8 emails (showing top 10):

1. Timeline Update - Jan 10, 2024
   From: Project Manager <pm@company.com>
   Subject: Q1 Project Timeline - Delay Notification
   Preview: I wanted to inform everyone that we're pushing...

2. Budget Review - Jan 8, 2024
   From: Finance Team <finance@company.com>
   Subject: Budget Constraints Impact on Schedule
   Preview: Due to the recent budget cuts, we need to reconsider...

...
```

**Note**: Semantic search finds emails about deadline concerns even if they don't use those exact words.

## RAG (AI-Powered) Examples

### Basic RAG Query

```bash
export EMAIL_SEARCH_OPENAI_API_KEY="your-key"
ragmail search "what did John say about the project deadline?" --rag
```

**Expected Output:**
```
═══════════════════════════════════════════════════════════════
AI ANSWER
═══════════════════════════════════════════════════════════════

Based on your emails, John mentioned the project deadline in three 
emails:

1. In his email from January 10, 2024, John wrote: "We're still on 
   track for the February 15 deadline, but I'm concerned about the 
   testing phase."

2. On January 20, John followed up: "After discussing with the team, 
   we agreed to extend the deadline to March 1st to ensure quality."

3. In his most recent email from January 25, John confirmed: "The new 
   deadline of March 1st has been approved by management."

Key points:
- Original deadline was February 15, 2024
- Extended to March 1, 2024 due to testing concerns
- Extension has management approval

═══════════════════════════════════════════════════════════════
RETRIEVED EMAILS
═══════════════════════════════════════════════════════════════

1. Project Timeline Update (Jan 10, 2024)
   From: John Doe <john@example.com>
   
2. Re: Project Schedule (Jan 20, 2024)
   From: John Doe <john@example.com>
   
3. Deadline Confirmation (Jan 25, 2024)
   From: John Doe <john@example.com>

...
```

### Complex RAG Query

```bash
ragmail search "summarize my discussions with Sarah about the budget" --rag
```

**Expected Output:**
```
═══════════════════════════════════════════════════════════════
AI ANSWER
═══════════════════════════════════════════════════════════════

Here's a summary of your budget discussions with Sarah Johnson:

**Overview**: You and Sarah exchanged 5 emails between January 5-18, 
2024, primarily discussing the Q1 2024 budget allocation for the 
marketing team.

**Key Points:**

1. **Initial Request** (Jan 5): Sarah requested a 20% increase in 
   marketing budget to support the new product launch campaign.

2. **Concerns Raised** (Jan 8): You expressed concerns about the 
   overall departmental budget constraints and asked for a breakdown 
   of how the additional funds would be used.

3. **Detailed Proposal** (Jan 12): Sarah provided a detailed breakdown 
   showing $15K for digital ads, $10K for events, and $5K for content 
   creation.

4. **Negotiation** (Jan 15): You proposed approving 75% of the request 
   ($22.5K instead of $30K), prioritizing digital ads and events.

5. **Final Agreement** (Jan 18): Sarah accepted the modified budget 
   with the understanding that content creation would use existing 
   resources.

**Outcome**: Approved $22.5K additional budget for Q1 marketing, with 
focus on digital advertising and events.

═══════════════════════════════════════════════════════════════
RETRIEVED EMAILS
═══════════════════════════════════════════════════════════════

1. Q1 Budget Request (Jan 5, 2024) - Sarah Johnson
2. Re: Budget Request (Jan 8, 2024) - You
3. Budget Breakdown Details (Jan 12, 2024) - Sarah Johnson
4. Modified Budget Proposal (Jan 15, 2024) - You
5. Budget Approval Confirmation (Jan 18, 2024) - Sarah Johnson
```

### RAG Query with No Results

```bash
ragmail search "what did Alice say about the merger?" --rag
```

**Expected Output:**
```
═══════════════════════════════════════════════════════════════
AI ANSWER
═══════════════════════════════════════════════════════════════

I don't have enough information to answer this question.

After searching your emails, I found no emails from Alice or any 
mentions of a merger. The retrieved emails discuss other topics 
(e.g., project timelines, budget meetings) but don't contain 
information about Alice or a merger.

═══════════════════════════════════════════════════════════════
RETRIEVED EMAILS
═══════════════════════════════════════════════════════════════

No relevant emails found.
```

## Complex Query Examples

### Multi-Part Queries

```bash
ragmail search "emails from John in 2023 about the budget"
```

**Expected Output:**
```
Found 12 emails from John in 2023 matching "budget":

1. Budget Review Q3 (Aug 15, 2023)
   From: John Doe <john@example.com>
   Subject: Q3 Budget Review - Marketing Team
   
2. Re: Budget Cuts (Sep 20, 2023)
   From: John Doe <john@example.com>
   Subject: Re: Budget Cuts Impact on Projects

...
```

**How it works**: The query parser extracts:
- Sender: "John"
- Date: "2023"
- Topic: "budget"

Then performs filtered semantic search.

### Query with Multiple Constraints

```bash
ragmail search "emails from Sarah to the team in January about deadlines"
```

**Expected Output:**
```
Found 8 emails from Sarah to team@company.com in January:

1. Project Deadlines Update (Jan 5, 2024)
   From: Sarah Johnson <sarah@company.com>
   To: team@company.com
   
2. Sprint Deadline Reminder (Jan 12, 2024)
   From: Sarah Johnson <sarah@company.com>
   To: team@company.com

...
```

### Negation Queries

```bash
ragmail search "emails about meetings but not about cancelled"
```

## Aggregation Queries

### Count Queries

```bash
ragmail search "how many emails in 2023"
```

**Expected Output:**
```
COUNT: 1,247 emails in 2023

Breakdown by month:
- January: 98
- February: 103
- March: 112
- April: 89
- May: 95
- June: 134
- July: 101
- August: 87
- September: 118
- October: 105
- November: 99
- December: 106
```

### Most Frequent Contacts

```bash
ragmail search "who did I email most in 2023"
```

**Expected Output:**
```
TOP CONTACTS IN 2023 (by email frequency):

1. Sarah Johnson <sarah@company.com>
   Sent: 47 emails | Received: 52 emails | Total: 99

2. Team <team@company.com>
   Sent: 38 emails | Received: 41 emails | Total: 79

3. John Doe <john@example.com>
   Sent: 31 emails | Received: 28 emails | Total: 59

4. Project Managers <pm@company.com>
   Sent: 22 emails | Received: 35 emails | Total: 57

5. Marketing Team <marketing@company.com>
   Sent: 18 emails | Received: 29 emails | Total: 47
```

### Email Volume Analysis

```bash
ragmail search "show email volume by month"
```

## Date-Based Queries

### Specific Date Ranges

```bash
ragmail search "emails from March 2024"
ragmail search "emails in Q1 2024"
ragmail search "emails last week"
```

### Relative Dates

```bash
ragmail search "emails from yesterday"
ragmail search "emails this month"
ragmail search "emails from last year"
```

### Date Range with Topic

```bash
ragmail search "emails about the conference between Jan 1 and March 31"
```

## Sender/Recipient Queries

### Find by Sender

```bash
ragmail search "emails from john@example.com"
ragmail search "emails from John"
ragmail search "emails sent by Sarah"
```

### Find by Recipient

```bash
ragmail search "emails to team@company.com"
ragmail search "emails sent to the marketing team"
```

### Find Conversations

```bash
ragmail search "emails between me and John"
ragmail search "conversations with Sarah about the project"
```

## Advanced Usage Patterns

### Combining Search with RAG

```bash
# First, see what emails exist
ragmail search "emails from John about budget"

# Then, get AI summary
ragmail search "what was John's main concern about the budget?" --rag
```

### Pipeline Examples

```bash
# Search and export to file
ragmail search "tax documents 2023" > tax_emails.txt

# Search with JSON output for processing
ragmail search "invoices" --format json | jq '.[] | {subject: .subject, date: .date}'

# Count and analyze
ragmail search "how many emails from each sender in 2023"
```

### Batch Processing

```bash
# Process multiple queries from file
while read query; do
    echo "=== $query ==="
    ragmail search "$query" --rag
done < queries.txt
```

### API Usage Examples

```bash
# Basic search via API
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"query": "meeting tomorrow", "limit": 5}'

# RAG search via API
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "what did John say about the project?",
    "use_rag": true,
    "limit": 10
  }'

# Get email by ID
curl http://localhost:8000/emails/abc123

# Find similar emails
curl http://localhost:8000/emails/abc123/similar?limit=5
```

## Query Enhancement Examples

### Before and After Query Expansion

**Original Query:**
```bash
ragmail search "John project stuff"
```

**Enhanced Query (internally expanded):**
```
Original: "John project stuff"
Expanded to:
- Intent: search
- Entities: {people: ["John"], topics: ["project"]}
- Sub-queries: [
    "emails from John",
    "emails about project",
    "John project updates"
  ]
```

### Complex Query Decomposition

**User Query:**
```bash
ragmail search "who did I email most in 2023 about the budget"
```

**Internal Processing:**
```
Query Analysis:
- Intent: aggregation
- Entities: {
    dates: ["2023"],
    topics: ["budget"]
  }
- Sub-queries: [
    "emails in 2023",
    "emails about budget",
    "frequent contacts 2023"
  ]
```

## Tips for Effective Queries

### 1. Be Specific for RAG

❌ Vague: `ragmail search "tell me about work" --rag`
✅ Specific: `ragmail search "what did the team decide about remote work policy?" --rag`

### 2. Use Natural Language

❌ Robotic: `ragmail search "sender:john date:2023 subject:budget"`
✅ Natural: `ragmail search "emails from John in 2023 about the budget"`

### 3. Combine Search and RAG

```bash
# First find relevant emails
ragmail search "emails from Sarah about the conference"

# Then ask specific questions
ragmail search "when and where is the conference?" --rag
```

### 4. Use Quotes for Exact Phrases

```bash
ragmail search '"quarterly review"'
ragmail search '"action items" from Sarah'
```

### 5. Filter by Date When Possible

```bash
# Faster and more accurate
ragmail search "emails from 2024 about budget"

# Instead of
ragmail search "emails about budget"
```

## Example Workflows

### Workflow 1: Finding a Specific Decision

```bash
# Step 1: Find relevant emails
ragmail search "decision about remote work policy"

# Step 2: Get AI summary
ragmail search "what was the final decision on remote work policy?" --rag

# Step 3: Get specific details
ragmail search "when does the remote work policy take effect?" --rag
```

### Workflow 2: Preparing for a Meeting

```bash
# Step 1: Find recent emails about the topic
ragmail search "emails from this week about project X"

# Step 2: Get caught up with RAG
ragmail search "summarize the latest updates on project X" --rag

# Step 3: Check action items
ragmail search "what are my action items from project X discussions?" --rag
```

### Workflow 3: Year-End Review

```bash
# Step 1: Get email volume
ragmail search "how many emails in 2023"

# Step 2: See top contacts
ragmail search "who did I email most in 2023"

# Step 3: Review key projects
ragmail search "summarize emails about major projects in 2023" --rag
```

## Common Query Patterns

| Pattern | Example | Use Case |
|---------|---------|----------|
| Who + When | "who did I email most in 2023" | Contact analysis |
| What + Who | "what did John say about X" | Information extraction |
| Count + When | "how many emails last month" | Volume analysis |
| Find + Constraints | "emails from X in Y about Z" | Filtered search |
| Summarize + Context | "summarize my discussions with X" | Conversation review |
| Decision + Topic | "what was decided about X" | Decision tracking |
| Action Items | "what are my action items" | Task extraction |

## Error Examples and Solutions

### No Results Found

```bash
ragmail search "emails from Alice about quantum computing"
# Output: No emails found matching your query.
```

**Solution**: Try broader search
```bash
ragmail search "emails about quantum"
ragmail search "emails from Alice"
```

### Too Many Results

```bash
ragmail search "emails about work"
# Output: Found 2,847 emails...
```

**Solution**: Add constraints
```bash
ragmail search "emails about work from 2024"
ragmail search "emails from John about work"
```

### RAG Without API Key

```bash
ragmail search "what did John say?" --rag
# Output: Error: OpenAI API key not set
```

**Solution**: Set API key
```bash
export EMAIL_SEARCH_OPENAI_API_KEY="sk-..."
```

## Query Planning

The planner generates a structured JSON plan that separates vector search,
full-text search, and metadata filters. It is enabled with `--plan` (and is
also used when `--rag` is set).

```bash
ragmail search --plan "how many emails from Anthropic in January 2026"
```

```bash
ragmail search --plan "emails to legal@company.com with attachments in 2025"
```

Notes:
- Full-text search covers subject, body, sender, recipients, and labels.
- Metadata filters (year/month/date ranges/attachment) are applied as SQL filters.
