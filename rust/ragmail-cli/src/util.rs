pub(crate) fn is_interrupted_error(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("interrupted")
}
