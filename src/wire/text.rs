//! EchoLib/ASL data grammar, deliberately separate from human-text authorization.
use super::{MAX_CHAT_BYTES, WireError, WirePacket};

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
    Ok(WirePacket::Chat { sender, body })
}
