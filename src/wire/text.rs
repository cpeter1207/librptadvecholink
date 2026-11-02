//! EchoLib/ASL data grammar, deliberately separate from human-text authorization.
use super::{MAX_CHAT_BYTES, MAX_DATAGRAM, WireError, WirePacket, copy_output};

pub(super) fn parse(bytes: &[u8]) -> Result<WirePacket<'_>, WireError> {
    let Some(data) = bytes.strip_prefix(b"oNDATA") else {
        return Ok(WirePacket::Ignored);
    };
    let data = data.strip_suffix(&[0]).unwrap_or(data);
    if data.contains(&0) {
        return Err(WireError::Malformed);
    }
    if let Some(information) = data.strip_prefix(b"\r") {
        return Ok(WirePacket::StationInfo(information));
    }
    let text = std::str::from_utf8(data).map_err(|_| WireError::Malformed)?;
    let text = text.strip_suffix("\r\n").ok_or(WireError::Malformed)?;
    let (sender, body) = text.split_once('>').ok_or(WireError::Malformed)?;
    validate_chat(sender, body)?;
    Ok(WirePacket::Chat { sender, body })
}

fn validate_chat(sender: &str, body: &str) -> Result<(), WireError> {
    if body.len() > MAX_CHAT_BYTES {
        return Err(WireError::TooLarge);
    }
    if sender.is_empty()
        || sender.len() > 64
        || !sender
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-/*_".contains(&byte))
        || body.is_empty()
        || body.chars().any(char::is_control)
    {
        return Err(WireError::Malformed);
    }
    Ok(())
}

/// Serialize human chat with the EchoLib CR/LF and NUL terminators. The caller
/// supplies the preserved represented sender; admission authority stays with the QSO.
/// Invalid input or insufficient capacity leaves output unchanged.
pub fn write_chat(sender: &str, body: &str, output: &mut [u8]) -> Result<usize, WireError> {
    validate_chat(sender, body)?;
    copy_output(format!("oNDATA{sender}>{body}\r\n\0").as_bytes(), output)
}

/// Serialize station information, never human chat. Normalize line endings to
/// EchoLib's CR form and include its terminating NUL for both ASL and EchoLib readers.
/// Byte bounds are checked before allocation; no identity or text is truncated.
pub fn write_station_info(body: &str, output: &mut [u8]) -> Result<usize, WireError> {
    if body.len() > MAX_DATAGRAM - 8 {
        return Err(WireError::TooLarge);
    }
    if body.contains('\0') {
        return Err(WireError::Malformed);
    }
    let body = body.replace("\r\n", "\n").replace('\n', "\r");
    copy_output(format!("oNDATA\r{body}\0").as_bytes(), output)
}
