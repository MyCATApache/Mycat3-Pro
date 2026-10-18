use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("unexpected end of packet: need {need} bytes, got {got}")]
    UnexpectedEof { need: usize, got: usize },
    #[error("invalid length-encoded integer prefix: 0x{0:02x}")]
    InvalidLenencInt(u8),
    #[error("invalid utf8 in packet payload")]
    InvalidUtf8,
    #[error("packet too large: {0} bytes")]
    PacketTooLarge(usize),
    #[error("malformed packet: {0}")]
    Malformed(&'static str),
    #[error("unsupported auth plugin: {0}")]
    UnsupportedAuthPlugin(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type ProtocolResult<T> = Result<T, ProtocolError>;

impl ProtocolError {
    pub fn need_got(need: usize, got: usize) -> Self {
        ProtocolError::UnexpectedEof { need, got }
    }
}
