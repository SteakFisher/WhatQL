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

#[derive(Debug)]
pub enum SqliteValue {
    Null,
    Integer(i64),
    Float(f64),
    Blob(Vec<u8>),
    Text(String),
}

pub struct ParseResult {
    pub value: SqliteValue,
    pub bytes_consumed: usize,
}

pub fn parse_value(serial_type: u64, bytes: &[u8]) -> Result<ParseResult, std::io::Error> {
    match serial_type {
        0 => Ok(ParseResult {
            value: SqliteValue::Null,
            bytes_consumed: 0,
        }),
        1 => {
            if bytes.len() < 1 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "insufficient bytes"));
            }
            Ok(ParseResult {
                value: SqliteValue::Integer(bytes[0] as i8 as i64),
                bytes_consumed: 1,
            })
        }
        2 => {
            if bytes.len() < 2 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "insufficient bytes"));
            }
            Ok(ParseResult {
                value: SqliteValue::Integer(i16::from_be_bytes(bytes[..2].try_into().unwrap()) as i64),
                bytes_consumed: 2,
            })
        }
        3 => {
            if bytes.len() < 3 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "insufficient bytes"));
            }
            // For 24-bit integers, we need to handle sign extension manually
            let mut int_bytes = [0u8; 4];
            int_bytes[1..4].copy_from_slice(&bytes[..3]);
            if (bytes[0] & 0x80) != 0 {
                int_bytes[0] = 0xFF;
            }
            Ok(ParseResult {
                value: SqliteValue::Integer(i32::from_be_bytes(int_bytes) as i64),
                bytes_consumed: 3,
            })
        }
        4 => {
            if bytes.len() < 4 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "insufficient bytes"));
            }
            Ok(ParseResult {
                value: SqliteValue::Integer(i32::from_be_bytes(bytes[..4].try_into().unwrap()) as i64),
                bytes_consumed: 4,
            })
        }
        5 => {
            if bytes.len() < 6 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "insufficient bytes"));
            }
            let mut int_bytes = [0u8; 8];
            int_bytes[2..8].copy_from_slice(&bytes[..6]);
            if (bytes[0] & 0x80) != 0 {
                int_bytes[0] = 0xFF;
                int_bytes[1] = 0xFF;
            }
            Ok(ParseResult {
                value: SqliteValue::Integer(i64::from_be_bytes(int_bytes)),
                bytes_consumed: 6,
            })
        }
        6 => {
            if bytes.len() < 8 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "insufficient bytes"));
            }
            Ok(ParseResult {
                value: SqliteValue::Integer(i64::from_be_bytes(bytes[..8].try_into().unwrap())),
                bytes_consumed: 8,
            })
        }
        7 => {
            if bytes.len() < 8 {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "insufficient bytes"));
            }
            Ok(ParseResult {
                value: SqliteValue::Float(f64::from_be_bytes(bytes[..8].try_into().unwrap())),
                bytes_consumed: 8,
            })
        }
        8 => Ok(ParseResult {
            value: SqliteValue::Integer(0),
            bytes_consumed: 0,
        }),
        9 => Ok(ParseResult {
            value: SqliteValue::Integer(1),
            bytes_consumed: 0,
        }),
        n if n >= 12 => {
            let length = if n % 2 == 0 {
                (n - 12) / 2
            } else {
                (n - 13) / 2
            } as usize;

            if bytes.len() < length {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "insufficient bytes"));
            }

            if n % 2 == 0 {
                // BLOB
                Ok(ParseResult {
                    value: SqliteValue::Blob(bytes[..length].to_vec()),
                    bytes_consumed: length,
                })
            } else {
                // Text
                match String::from_utf8(bytes[..length].to_vec()) {
                    Ok(s) => Ok(ParseResult {
                        value: SqliteValue::Text(s),
                        bytes_consumed: length,
                    }),
                    Err(_) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid UTF-8")),
                }
            }
        },
        10 | 11 => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "reserved serial type")),
        _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid serial type")),
    }
}