"""Generate realistic synthetic MBOX files for testing.

This module creates fake but realistic email data for testing the email search
system without using real personal emails. Uses LLM to generate varied personas
and realistic email threads.

Usage:
    python generate_emails.py --persona tech_professional --count 50 --output 2024_test.mbox
    python generate_emails.py --persona small_business --count 100 --output business_emails.mbox
"""

import argparse
import json
import mailbox
import os
import random
import re
import time
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Any

# Try to import OpenAI, but allow the module to load without it for imports
try:
    from openai import OpenAI

    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False


# Email personas with distinct communication patterns
PERSONAS = {
    "tech_professional": {
        "name": "Alex Chen",
        "email": "alex.chen@techcorp.com",
        "company": "TechCorp Inc.",
        "role": "Senior Software Engineer",
        "style": "concise, technical, uses jargon, bullet points, code snippets",
        "topics": [
            "Code reviews and pull requests",
            "Sprint planning and retrospectives",
            "Bug fixes and feature implementations",
            "Technical architecture discussions",
            "Deployment and CI/CD issues",
            "Team standup updates",
            "1-on-1 meetings with manager",
        ],
        "contacts": [
            ("Sarah Johnson", "sarah.j@techcorp.com", "Product Manager"),
            ("Mike Ross", "mike.ross@techcorp.com", "Engineering Manager"),
            ("DevOps Team", "devops@techcorp.com", "Infrastructure"),
            ("GitHub", "noreply@github.com", "Automated"),
            ("Jira", "jira@techcorp.atlassian.net", "Automated"),
        ],
    },
    "small_business": {
        "name": "Maria Garcia",
        "email": "maria@garciadesigns.com",
        "company": "Garcia Design Studio",
        "role": "Owner & Creative Director",
        "style": "friendly, professional, detail-oriented, uses emojis occasionally",
        "topics": [
            "Client project proposals and quotes",
            "Invoice payments and reminders",
            "Vendor communications and orders",
            "Meeting scheduling with clients",
            "Project feedback and revisions",
            "Marketing and social media planning",
            "Tax and accounting matters",
        ],
        "contacts": [
            ("John Smith", "john@smithrealty.com", "Client"),
            ("Lisa Wong", "lisa@printshop.com", "Vendor"),
            ("QuickBooks", "notifications@quickbooks.com", "Automated"),
            ("David Park", "david@parklaw.com", "Accountant"),
            ("Jennifer Adams", "jennifer@adamsmarketing.com", "Marketing Consultant"),
        ],
    },
    "student": {
        "name": "Jordan Taylor",
        "email": "jtaylor@university.edu",
        "company": "State University",
        "role": "Graduate Student",
        "style": "casual, sometimes formal, asks questions, apologetic about deadlines",
        "topics": [
            "Course registration and add/drop",
            "Assignment submissions and extensions",
            "Study group coordination",
            "Research assistant work",
            "Professor office hours",
            "Financial aid and scholarships",
            "Campus event announcements",
        ],
        "contacts": [
            ("Dr. Emily Brown", "e.brown@university.edu", "Professor"),
            ("Student Services", "services@university.edu", "Administration"),
            ("Group Project Team", "project-team@groups.university.edu", "Peers"),
            ("Financial Aid", "finaid@university.edu", "Administration"),
            ("Career Center", "careers@university.edu", "Services"),
        ],
    },
    "parent": {
        "name": "Chris Williams",
        "email": "chris.williams@email.com",
        "company": "Personal",
        "role": "Parent of 2",
        "style": "warm, slightly hurried, scheduling-focused, uses abbreviations",
        "topics": [
            "School newsletters and announcements",
            "PTO meeting schedules",
            "Sports practice and game schedules",
            "Doctor and dentist appointments",
            "Birthday party invitations",
            "Carpool coordination",
            "Teacher communications",
        ],
        "contacts": [
            ("Oakwood Elementary", "office@oakwoodelem.edu", "School"),
            ("Sarah Miller", "sarah.m@email.com", "Other Parent"),
            ("Dr. Patel", "appointments@patelpediatrics.com", "Doctor"),
            ("Soccer League", "info@youthsoccer.org", "Sports"),
            ("Room Parent", "room15@oakwoodelem.edu", "School"),
        ],
    },
}


def create_openai_client() -> OpenAI:
    """Create OpenAI client with API key from environment."""
    if not HAS_OPENAI:
        raise ImportError("OpenAI package not installed. Run: pip install openai")

    api_key = os.environ.get("EMAIL_SEARCH_OPENAI_API_KEY") or os.environ.get(
        "OPENAI_API_KEY"
    )
    if not api_key:
        raise ValueError(
            "OpenAI API key required. Set EMAIL_SEARCH_OPENAI_API_KEY or OPENAI_API_KEY environment variable."
        )
    return OpenAI(api_key=api_key)


def generate_persona_details(persona_type: str) -> dict[str, Any]:
    """Generate detailed persona information using LLM.

    Args:
        persona_type: Key from PERSONAS dict

    Returns:
        Persona details dictionary
    """
    client = create_openai_client()
    base_persona = PERSONAS[persona_type]

    prompt = f"""Generate a detailed persona profile for testing an email search system.

Base Information:
- Name: {base_persona["name"]}
- Email: {base_persona["email"]}
- Company: {base_persona["company"]}
- Role: {base_persona["role"]}

Please expand this into a detailed profile including:
1. 5-7 specific work projects/initiatives they're involved in
2. 3-4 recurring meeting types they attend
3. Communication patterns (response time, formal vs casual)
4. 5-8 specific people they regularly email (names, roles, relationship)
5. Typical daily/weekly email volume
6. Any stressors or recurring issues

Return as JSON with these keys: projects, meetings, communication_style, regular_contacts, email_volume, stressors
"""

    response = client.chat.completions.create(
        model="gpt-5.2",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.7,
    )

    details = json.loads(response.choices[0].message.content)
    details.update(base_persona)  # Merge with base info
    return details


def generate_email_batch(
    persona: dict[str, Any],
    start_date: datetime,
    end_date: datetime,
    count: int,
    threads: bool = True,
) -> list[dict[str, Any]]:
    """Generate a batch of emails for a persona.

    Args:
        persona: Persona details dictionary
        start_date: Start of date range
        end_date: End of date range
        count: Number of emails to generate
        threads: Whether to create email threads

    Returns:
        List of email dictionaries
    """
    client = create_openai_client()

    # Convert contacts to string format for prompt
    contacts_str = "\n".join(
        [
            f"- {name} ({email}) - {role}"
            for name, email, role in persona.get("contacts", [])
        ]
    )

    topics_str = "\n".join([f"- {t}" for t in persona.get("topics", [])])
    projects_str = "\n".join(
        [f"- {p}" for p in persona.get("projects", persona.get("topics", []))[:5]]
    )

    prompt = f"""Generate {count} realistic emails for this persona.

PERSONA:
Name: {persona["name"]}
Email: {persona["email"]}
Role: {persona["role"]} at {persona["company"]}
Style: {persona["style"]}

PROJECTS/INITIATIVES:
{projects_str}

REGULAR CONTACTS:
{contacts_str}

TOPICS:
{topics_str}

DATE RANGE: {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}

Generate a mix of:
- Sent emails (from this persona to contacts)
- Received emails (from contacts to this persona)
- Some emails should be replies forming threads
- Automated notifications (calendar invites, system alerts)
- Include realistic timestamps spread across the date range

Each email should have:
- message_id: unique ID
- thread_id: ID for grouping related emails
- in_reply_to: message ID being replied to (if reply)
- date: ISO format timestamp
- from_name, from_email
- to_emails: list
- subject
- body: full email body (realistic length and content)
- is_reply: boolean

Return as JSON array of email objects."""

    response = client.chat.completions.create(
        model="gpt-5.2",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.8,
        max_tokens=4000,
    )

    result = json.loads(response.choices[0].message.content)
    emails = result.get("emails", [])

    # Validate and add missing fields
    for i, email in enumerate(emails):
        if "message_id" not in email:
            email["message_id"] = (
                f"<gen-{persona['name'].lower().replace(' ', '-')}-{i}-{int(time.time())}@generated.com>"
            )
        if "thread_id" not in email:
            email["thread_id"] = email["message_id"]
        if "date" not in email:
            # Random date in range
            delta = end_date - start_date
            random_days = random.randint(0, delta.days)
            email_date = start_date + timedelta(
                days=random_days, hours=random.randint(8, 18)
            )
            email["date"] = email_date.isoformat()

    return emails


def create_mbox_message(email_data: dict[str, Any]) -> EmailMessage:
    """Convert email dictionary to MBOX format message.

    Args:
        email_data: Email dictionary from generation

    Returns:
        EmailMessage object ready for MBOX
    """
    msg = EmailMessage()

    # Headers
    msg["Message-ID"] = email_data.get("message_id", "")
    if email_data.get("in_reply_to"):
        msg["In-Reply-To"] = email_data["in_reply_to"]
    if email_data.get("references"):
        msg["References"] = email_data["references"]

    msg["From"] = (
        f"{email_data.get('from_name', '')} <{email_data.get('from_email', '')}>"
    )
    msg["To"] = ", ".join(email_data.get("to_emails", []))
    if email_data.get("cc_emails"):
        msg["Cc"] = ", ".join(email_data["cc_emails"])

    msg["Subject"] = email_data.get("subject", "No Subject")
    msg["Date"] = email_data.get("date", datetime.now().isoformat())

    # Body
    body = email_data.get("body", "")
    msg.set_content(body)

    return msg


def generate_mbox_file(
    persona_type: str,
    output_path: Path,
    email_count: int = 50,
    start_date: str | None = None,
    end_date: str | None = None,
    expand_persona: bool = True,
) -> None:
    """Generate an MBOX file with synthetic emails.

    Args:
        persona_type: Type of persona to generate
        output_path: Where to save the MBOX file
        email_count: Number of emails to generate
        start_date: Start date (YYYY-MM-DD), defaults to 1 year ago
        end_date: End date (YYYY-MM-DD), defaults to today
        expand_persona: Whether to use LLM to expand persona details
    """
    print(f"Generating {email_count} emails for persona: {persona_type}")
    print(f"Output: {output_path}")

    # Parse dates
    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end_dt = datetime.now()

    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start_dt = end_dt - timedelta(days=365)

    # Get base persona
    persona = PERSONAS.get(persona_type)
    if not persona:
        raise ValueError(
            f"Unknown persona: {persona_type}. Choose from: {list(PERSONAS.keys())}"
        )

    # Expand persona with LLM if requested
    if expand_persona:
        print("Expanding persona details with LLM...")
        try:
            persona = generate_persona_details(persona_type)
            print(
                f"Generated expanded persona with {len(persona.get('projects', []))} projects"
            )
        except Exception as e:
            print(f"Warning: Could not expand persona: {e}. Using base persona.")

    # Generate emails
    print("Generating email content...")
    try:
        emails = generate_email_batch(persona, start_dt, end_dt, email_count)
    except Exception as e:
        print(f"Error generating emails: {e}")
        print("Falling back to basic email generation...")
        emails = generate_basic_emails(persona, start_dt, end_dt, email_count)

    print(f"Generated {len(emails)} emails")

    # Write to MBOX
    output_path.parent.mkdir(parents=True, exist_ok=True)
    mbox = mailbox.mbox(str(output_path))
    mbox.lock()

    try:
        for email_data in emails:
            msg = create_mbox_message(email_data)
            mbox.add(msg)
    finally:
        mbox.unlock()
        mbox.close()

    print(f"✓ Saved to {output_path}")


def generate_basic_emails(
    persona: dict[str, Any],
    start_date: datetime,
    end_date: datetime,
    count: int,
) -> list[dict[str, Any]]:
    """Fallback function to generate basic emails without LLM.

    Used when OpenAI API is not available or fails.
    """
    emails = []
    contacts = persona.get("contacts", [])
    topics = persona.get("topics", ["General discussion"])

    for i in range(count):
        # Random date
        delta = end_date - start_date
        random_days = random.randint(0, delta.days)
        email_date = start_date + timedelta(
            days=random_days, hours=random.randint(8, 18)
        )
        if email_date > end_date:
            email_date = end_date

        # Random direction (sent or received)
        is_sent = random.choice([True, False])

        if is_sent:
            from_name = persona["name"]
            from_email = persona["email"]
            to_contact = (
                random.choice(contacts)
                if contacts
                else ("Contact", "contact@example.com", "Contact")
            )
            to_emails = [to_contact[1]]
        else:
            from_contact = (
                random.choice(contacts)
                if contacts
                else ("Contact", "contact@example.com", "Contact")
            )
            from_name = from_contact[0]
            from_email = from_contact[1]
            to_emails = [persona["email"]]

        topic = random.choice(topics)
        subject = f"Re: {topic}" if random.random() > 0.7 else topic

        # Generate basic body
        if is_sent:
            body = f"Hi {from_contact[0] if not is_sent else to_contact[0]},\n\nJust wanted to follow up on {topic.lower()}. Let me know if you have any questions.\n\nThanks,\n{persona['name']}"
        else:
            body = f"Hi {persona['name']},\n\nQuick note about {topic.lower()}. Can we discuss this soon?\n\nBest,\n{from_name}"

        email = {
            "message_id": f"<basic-{i}-{int(time.time())}@generated.com>",
            "thread_id": f"<basic-{i}-{int(time.time())}@generated.com>",
            "date": email_date.isoformat(),
            "from_name": from_name,
            "from_email": from_email,
            "to_emails": to_emails,
            "subject": subject,
            "body": body,
            "is_reply": "Re:" in subject,
        }
        emails.append(email)

    return emails


def main():
    parser = argparse.ArgumentParser(
        description="Generate realistic synthetic MBOX files for testing"
    )
    parser.add_argument(
        "--persona",
        choices=list(PERSONAS.keys()),
        default="tech_professional",
        help="Type of persona to generate emails for",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=50,
        help="Number of emails to generate",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("test_emails.mbox"),
        help="Output MBOX file path",
    )
    parser.add_argument(
        "--start-date",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--no-expand",
        action="store_true",
        help="Skip LLM persona expansion (use basic persona)",
    )
    parser.add_argument(
        "--list-personas",
        action="store_true",
        help="List available personas and exit",
    )

    args = parser.parse_args()

    if args.list_personas:
        print("Available personas:")
        for key, persona in PERSONAS.items():
            print(f"\n  {key}:")
            print(f"    Name: {persona['name']}")
            print(f"    Role: {persona['role']}")
            print(f"    Style: {persona['style']}")
        return

    generate_mbox_file(
        persona_type=args.persona,
        output_path=args.output,
        email_count=args.count,
        start_date=args.start_date,
        end_date=args.end_date,
        expand_persona=not args.no_expand,
    )


if __name__ == "__main__":
    main()
