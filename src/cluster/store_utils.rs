use bebytes::BeBytes;

#[must_use]
pub fn bytes_to_str(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap_or("")
}

#[must_use]
pub fn serialize<T: BeBytes>(item: &T) -> Vec<u8> {
    item.to_be_bytes()
}

#[must_use]
pub fn deserialize<T: BeBytes>(bytes: &[u8]) -> Option<T> {
    T::try_from_be_bytes(bytes).ok().map(|(v, _)| v)
}
