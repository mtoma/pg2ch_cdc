//! pgoutput binary protocol parser.
//!
//! Decodes the binary messages produced by PostgreSQL's pgoutput logical
//! decoding plugin: Relation, Insert, Update, Delete, Begin, Commit.
//!
//! Reference: https://www.postgresql.org/docs/current/protocol-logical-replication.html

use tracing::{debug, warn};

// ── Types ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub flags: u8,      // 1 = part of key
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
pub enum TupleData {
    Null,
    Unchanged,
    Text(String),
    Binary(Vec<u8>),
}

#[derive(Debug)]
pub enum PgoutputMessage {
    Relation(RelationInfo),
    Insert {
        rel_id: u32,
        values: Vec<TupleData>,
    },
    Update {
        rel_id: u32,
        old_values: Option<Vec<TupleData>>,
        new_values: Vec<TupleData>,
    },
    Delete {
        rel_id: u32,
        key_or_old: u8,
        values: Vec<TupleData>,
    },
    Begin {
        final_lsn: u64,
        timestamp: u64,
        xid: u32,
    },
    Commit,
}

// ── Parser ──────────────────────────────────────────────────────────────

pub fn decode_pgoutput(payload: &[u8]) -> Option<PgoutputMessage> {
    if payload.is_empty() {
        return None;
    }

    match payload[0] {
        b'R' => parse_relation(&payload[1..]),
        b'I' => parse_insert(&payload[1..]),
        b'U' => parse_update(&payload[1..]),
        b'D' => parse_delete(&payload[1..]),
        b'B' => parse_begin(&payload[1..]),
        b'C' => Some(PgoutputMessage::Commit),
        other => {
            debug!("Skipping pgoutput message type '{}' (0x{:02x})", other as char, other);
            None
        }
    }
}

fn parse_relation(data: &[u8]) -> Option<PgoutputMessage> {
    let mut pos = 0;

    let rel_id = read_u32(data, &mut pos)?;

    // Namespace (null-terminated)
    let namespace = read_cstring(data, &mut pos)?;

    // Relation name (null-terminated)
    let name = read_cstring(data, &mut pos)?;

    // Replica identity setting (1 byte)
    let _replica_identity = *data.get(pos)?;
    pos += 1;

    // Number of columns
    let n_cols = read_u16(data, &mut pos)?;

    let mut columns = Vec::with_capacity(n_cols as usize);
    for _ in 0..n_cols {
        let flags = *data.get(pos)?;
        pos += 1;

        let col_name = read_cstring(data, &mut pos)?;
        let type_oid = read_u32(data, &mut pos)?;
        let type_modifier = read_i32(data, &mut pos)?;

        columns.push(ColumnInfo {
            name: col_name,
            flags,
            type_oid,
            type_modifier,
        });
    }

    Some(PgoutputMessage::Relation(RelationInfo {
        id: rel_id,
        namespace,
        name,
        columns,
    }))
}

fn parse_insert(data: &[u8]) -> Option<PgoutputMessage> {
    let mut pos = 0;
    let rel_id = read_u32(data, &mut pos)?;

    let flag = *data.get(pos)?;
    pos += 1;

    if flag != b'N' {
        warn!("Unexpected INSERT flag: {}", flag as char);
        return None;
    }

    let values = read_tuple_data(data, &mut pos)?;

    Some(PgoutputMessage::Insert { rel_id, values })
}

fn parse_update(data: &[u8]) -> Option<PgoutputMessage> {
    let mut pos = 0;
    let rel_id = read_u32(data, &mut pos)?;

    let flag = *data.get(pos)?;
    pos += 1;

    let old_values = if flag == b'K' || flag == b'O' {
        // Old key or old tuple present — read and skip
        let old = read_tuple_data(data, &mut pos)?;
        let new_flag = *data.get(pos)?;
        pos += 1;
        if new_flag != b'N' {
            warn!("Unexpected UPDATE new flag: {}", new_flag as char);
            return None;
        }
        Some(old)
    } else if flag != b'N' {
        warn!("Unexpected UPDATE flag: {}", flag as char);
        return None;
    } else {
        None
    };

    let new_values = read_tuple_data(data, &mut pos)?;

    Some(PgoutputMessage::Update {
        rel_id,
        old_values,
        new_values,
    })
}

fn parse_delete(data: &[u8]) -> Option<PgoutputMessage> {
    let mut pos = 0;
    let rel_id = read_u32(data, &mut pos)?;

    let key_or_old = *data.get(pos)?;
    pos += 1;

    if key_or_old != b'K' && key_or_old != b'O' {
        warn!("Unexpected DELETE flag: {}", key_or_old as char);
        return None;
    }

    let values = read_tuple_data(data, &mut pos)?;

    Some(PgoutputMessage::Delete {
        rel_id,
        key_or_old,
        values,
    })
}

fn parse_begin(data: &[u8]) -> Option<PgoutputMessage> {
    let mut pos = 0;
    let final_lsn = read_u64(data, &mut pos)?;
    let timestamp = read_u64(data, &mut pos)?;
    let xid = read_u32(data, &mut pos)?;

    Some(PgoutputMessage::Begin {
        final_lsn,
        timestamp,
        xid,
    })
}

// ── Tuple data decoder ──────────────────────────────────────────────────

fn read_tuple_data(data: &[u8], pos: &mut usize) -> Option<Vec<TupleData>> {
    let n_cols = read_u16(data, pos)?;
    let mut values = Vec::with_capacity(n_cols as usize);

    for _ in 0..n_cols {
        let col_type = *data.get(*pos)?;
        *pos += 1;

        match col_type {
            b'n' => {
                values.push(TupleData::Null);
            }
            b'u' => {
                values.push(TupleData::Unchanged);
            }
            b't' => {
                let len = read_u32(data, pos)? as usize;
                if *pos + len > data.len() {
                    warn!("Tuple text data overflows buffer");
                    return None;
                }
                let text = String::from_utf8_lossy(&data[*pos..*pos + len]).into_owned();
                *pos += len;
                values.push(TupleData::Text(text));
            }
            b'b' => {
                let len = read_u32(data, pos)? as usize;
                if *pos + len > data.len() {
                    warn!("Tuple binary data overflows buffer");
                    return None;
                }
                let bin = data[*pos..*pos + len].to_vec();
                *pos += len;
                values.push(TupleData::Binary(bin));
            }
            _ => {
                warn!("Unknown tuple data type: 0x{:02x}", col_type);
                return None;
            }
        }
    }

    Some(values)
}

// ── Primitive readers ───────────────────────────────────────────────────

fn read_u16(data: &[u8], pos: &mut usize) -> Option<u16> {
    if *pos + 2 > data.len() {
        return None;
    }
    let val = u16::from_be_bytes(data[*pos..*pos + 2].try_into().ok()?);
    *pos += 2;
    Some(val)
}

fn read_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
    if *pos + 4 > data.len() {
        return None;
    }
    let val = u32::from_be_bytes(data[*pos..*pos + 4].try_into().ok()?);
    *pos += 4;
    Some(val)
}

fn read_i32(data: &[u8], pos: &mut usize) -> Option<i32> {
    if *pos + 4 > data.len() {
        return None;
    }
    let val = i32::from_be_bytes(data[*pos..*pos + 4].try_into().ok()?);
    *pos += 4;
    Some(val)
}

fn read_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
    if *pos + 8 > data.len() {
        return None;
    }
    let val = u64::from_be_bytes(data[*pos..*pos + 8].try_into().ok()?);
    *pos += 8;
    Some(val)
}

fn read_cstring(data: &[u8], pos: &mut usize) -> Option<String> {
    let start = *pos;
    while *pos < data.len() && data[*pos] != 0 {
        *pos += 1;
    }
    if *pos >= data.len() {
        return None;
    }
    let s = String::from_utf8_lossy(&data[start..*pos]).into_owned();
    *pos += 1; // skip null terminator
    Some(s)
}
