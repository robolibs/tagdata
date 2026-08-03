use super::{CodecError, KeyCodec, ValueCodec};

#[derive(Clone, Copy, Debug, Default)]
pub struct U64Codec;

impl KeyCodec<u64> for U64Codec {
    const ORDER_PRESERVING: bool = true;

    fn encode_key(&self, key: &u64) -> Result<Vec<u8>, CodecError> {
        Ok(key.to_be_bytes().to_vec())
    }

    fn decode_key(&self, bytes: &[u8]) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(fixed(bytes, "u64 key")?))
    }
}

impl ValueCodec<u64> for U64Codec {
    fn encode_value(&self, value: &u64) -> Result<Vec<u8>, CodecError> {
        Ok(value.to_be_bytes().to_vec())
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(fixed(bytes, "u64 value")?))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct I64Codec;

impl KeyCodec<i64> for I64Codec {
    const ORDER_PRESERVING: bool = true;

    fn encode_key(&self, key: &i64) -> Result<Vec<u8>, CodecError> {
        Ok(((*key as u64) ^ (1_u64 << 63)).to_be_bytes().to_vec())
    }

    fn decode_key(&self, bytes: &[u8]) -> Result<i64, CodecError> {
        Ok((u64::from_be_bytes(fixed(bytes, "i64 key")?) ^ (1_u64 << 63)) as i64)
    }
}

impl ValueCodec<i64> for I64Codec {
    fn encode_value(&self, value: &i64) -> Result<Vec<u8>, CodecError> {
        Ok(value.to_be_bytes().to_vec())
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<i64, CodecError> {
        Ok(i64::from_be_bytes(fixed(bytes, "i64 value")?))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct StringCodec;

impl KeyCodec<String> for StringCodec {
    const ORDER_PRESERVING: bool = true;

    fn encode_key(&self, key: &String) -> Result<Vec<u8>, CodecError> {
        Ok(key.as_bytes().to_vec())
    }

    fn decode_key(&self, bytes: &[u8]) -> Result<String, CodecError> {
        String::from_utf8(bytes.to_vec()).map_err(|error| CodecError::Decode(error.to_string()))
    }
}

impl ValueCodec<String> for StringCodec {
    fn encode_value(&self, value: &String) -> Result<Vec<u8>, CodecError> {
        Ok(value.as_bytes().to_vec())
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<String, CodecError> {
        String::from_utf8(bytes.to_vec()).map_err(|error| CodecError::Decode(error.to_string()))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BytesCodec;

impl KeyCodec<Vec<u8>> for BytesCodec {
    const ORDER_PRESERVING: bool = true;

    fn encode_key(&self, key: &Vec<u8>) -> Result<Vec<u8>, CodecError> {
        Ok(key.clone())
    }

    fn decode_key(&self, bytes: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(bytes.to_vec())
    }
}

impl ValueCodec<Vec<u8>> for BytesCodec {
    fn encode_value(&self, value: &Vec<u8>) -> Result<Vec<u8>, CodecError> {
        Ok(value.clone())
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<Vec<u8>, CodecError> {
        Ok(bytes.to_vec())
    }
}

/// Ordering-preserving codec for a pair of UTF-8 strings.
#[derive(Clone, Copy, Debug, Default)]
pub struct StringPairCodec;

impl KeyCodec<(String, String)> for StringPairCodec {
    const ORDER_PRESERVING: bool = true;

    fn encode_key(&self, key: &(String, String)) -> Result<Vec<u8>, CodecError> {
        let mut bytes = escape(&key.0);
        bytes.extend_from_slice(&escape(&key.1));
        Ok(bytes)
    }

    fn decode_key(&self, bytes: &[u8]) -> Result<(String, String), CodecError> {
        let (first, used) = unescape(bytes)?;
        let (second, tail) = unescape(&bytes[used..])?;
        if used + tail != bytes.len() {
            return Err(CodecError::Decode("trailing compound key bytes".into()));
        }
        Ok((first, second))
    }
}

fn fixed<const N: usize>(bytes: &[u8], name: &str) -> Result<[u8; N], CodecError> {
    bytes
        .try_into()
        .map_err(|_| CodecError::Decode(format!("{name} must contain {N} bytes")))
}

fn escape(value: &str) -> Vec<u8> {
    let mut result = Vec::with_capacity(value.len() + 2);
    for byte in value.bytes() {
        if byte == 0 {
            result.extend_from_slice(&[0, 255]);
        } else {
            result.push(byte);
        }
    }
    result.extend_from_slice(&[0, 0]);
    result
}

fn unescape(bytes: &[u8]) -> Result<(String, usize), CodecError> {
    let mut result = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != 0 {
            result.push(bytes[index]);
            index += 1;
        } else if bytes.get(index + 1) == Some(&255) {
            result.push(0);
            index += 2;
        } else if bytes.get(index + 1) == Some(&0) {
            return String::from_utf8(result)
                .map(|value| (value, index + 2))
                .map_err(|error| CodecError::Decode(error.to_string()));
        } else {
            return Err(CodecError::Decode("invalid compound key escape".into()));
        }
    }
    Err(CodecError::Decode("unterminated compound key".into()))
}

#[cfg(feature = "serde-codec")]
#[derive(Clone, Copy, Debug, Default)]
pub struct MessagePackCodec;

#[cfg(feature = "serde-codec")]
impl<T> ValueCodec<T> for MessagePackCodec
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    fn encode_value(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        rmp_serde::to_vec(value).map_err(|error| CodecError::Encode(error.to_string()))
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<T, CodecError> {
        rmp_serde::from_slice(bytes).map_err(|error| CodecError::Decode(error.to_string()))
    }
}
