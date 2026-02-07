"""Tests for email text processing utilities."""

from ragmail.ingest.text_processing import clean_body_for_embedding, chunk_text


def test_clean_body_for_embedding_removes_reply_chain():
    body = (
        "Hi team,\n\nLet's meet tomorrow.\n\n"
        "On Tue, Jan 2, 2024 at 9:00 AM Jane wrote:\n"
        "> Previous message\n> More quoted text\n"
    )
    cleaned = clean_body_for_embedding(body)

    assert "Let's meet tomorrow" in cleaned
    assert "Previous message" not in cleaned
    assert "Jane wrote" not in cleaned


def test_clean_body_for_embedding_removes_signature_and_footer():
    body = (
        "Quick update on the plan.\n\n"
        "--\nJohn Doe\n"
        "Sent from my iPhone\n\n"
        "To unsubscribe click here\n"
    )
    cleaned = clean_body_for_embedding(body)

    assert "Quick update" in cleaned
    assert "John Doe" not in cleaned
    assert "unsubscribe" not in cleaned.lower()


def test_chunk_text_splits_and_overlaps():
    text = " ".join(["word"] * 400)
    chunks = chunk_text(text, max_chars=200, overlap=50)

    assert len(chunks) > 1
    assert all(len(chunk) <= 200 + 10 for chunk in chunks)
    assert chunks[0].split()[0] == "word"
