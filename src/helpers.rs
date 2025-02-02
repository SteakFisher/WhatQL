pub fn decode_sqlite_varint(bytes: &[u8]) -> (u64, usize) {
    let mut result = 0;

    for (index, &byte) in bytes.iter().enumerate() {
        result = (result << 7) | (byte & 0x7F) as u64;

        if (byte & 0x80) == 0 {
            return (result, index + 1);
        }
    }

    panic!("Invalid VARINT");
}