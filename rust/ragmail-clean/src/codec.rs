pub(crate) fn decode_rfc2047_q(input: &[u8]) -> Vec<u8> {
    let mut output: Vec<u8> = Vec::with_capacity(input.len());
    let mut i = 0_usize;
    while i < input.len() {
        if input[i] == b'_' {
            output.push(b' ');
            i += 1;
            continue;
        }
        if input[i] == b'=' && i + 2 < input.len() {
            if let (Some(hi), Some(lo)) = (hex_value(input[i + 1]), hex_value(input[i + 2])) {
                output.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        output.push(input[i]);
        i += 1;
    }
    output
}

pub(crate) fn decode_with_charset(bytes: &[u8], charset: &str) -> String {
    let charset = charset.trim().to_ascii_lowercase();
    if charset == "iso-8859-1" || charset == "latin1" || charset == "latin-1" {
        return bytes.iter().map(|byte| *byte as char).collect::<String>();
    }
    String::from_utf8_lossy(bytes).to_string()
}

pub(crate) fn decode_quoted_printable(input: &[u8]) -> Vec<u8> {
    let mut output: Vec<u8> = Vec::with_capacity(input.len());
    let mut i = 0_usize;
    while i < input.len() {
        if input[i] == b'=' {
            if i + 1 < input.len() && input[i + 1] == b'\n' {
                i += 2;
                continue;
            }
            if i + 2 < input.len() && input[i + 1] == b'\r' && input[i + 2] == b'\n' {
                i += 3;
                continue;
            }
            if i + 2 < input.len() {
                if let (Some(hi), Some(lo)) = (hex_value(input[i + 1]), hex_value(input[i + 2])) {
                    output.push((hi << 4) | lo);
                    i += 3;
                    continue;
                }
            }
        }
        output.push(input[i]);
        i += 1;
    }
    output
}

pub(crate) fn decode_base64(input: &[u8]) -> Vec<u8> {
    let mut clean: Vec<u8> = vec![];
    for byte in input {
        if byte.is_ascii_whitespace() {
            continue;
        }
        clean.push(*byte);
    }

    let mut output: Vec<u8> = vec![];
    let mut chunk: [u8; 4] = [0; 4];
    let mut used = 0_usize;
    for byte in clean {
        let value = if byte == b'=' {
            Some(64_u8)
        } else {
            base64_value(byte)
        };
        let Some(value) = value else {
            continue;
        };
        chunk[used] = value;
        used += 1;
        if used == 4 {
            output.push((chunk[0] << 2) | (chunk[1] >> 4));
            if chunk[2] != 64 {
                output.push((chunk[1] << 4) | (chunk[2] >> 2));
            }
            if chunk[3] != 64 {
                output.push((chunk[2] << 6) | chunk[3]);
            }
            used = 0;
        }
    }
    output
}

fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

pub(crate) fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
