#[derive(Debug)]
pub struct  FieldInfo {
    pub size: usize,
    pub offset: usize
}

impl FieldInfo {
    pub fn new(size: usize, offset: usize) -> FieldInfo {
        FieldInfo {
            size,
            offset
        }
    }
}

const UNKNOWN_FIELD_TYPE: &'static str = "Unknown field type";

#[inline(always)]
pub fn field_type_from_string(field_type: &str) -> Result<FieldType, &'static str> {
    match field_type {
        "Byte" => Ok(FieldType::Byte),
        "Bool" => Ok(FieldType::Bool),
        "Uint8" => Ok(FieldType::Uint8),
        "Uint16" => Ok(FieldType::Uint16),
        "Uint32" => Ok(FieldType::Uint32),
        "Uint64" => Ok(FieldType::Uint64),
        "Uint128" => Ok(FieldType::Uint128),
        "Int8" => Ok(FieldType::Int8),
        "Int16" => Ok(FieldType::Int16),
        "Int32" => Ok(FieldType::Int32),
        "Int64" => Ok(FieldType::Int64),
        "Int128" => Ok(FieldType::Int128),
        "Float32" => Ok(FieldType::Float32),
        "Float64" => Ok(FieldType::Float64),
        "String" => Ok(FieldType::String),
        "ByteSlice" => Ok(FieldType::ByteSlice),
        _ => Err(UNKNOWN_FIELD_TYPE),
    }
}

#[repr(u8)]
pub enum FieldType {
    Byte,
    Bool,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Uint128,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Float32,
    Float64,
    String,
    ByteSlice,
}

#[inline(always)]
/// Get number of unsized fields only for unsized fields. For other fields it can get any number. Read below.
///
/// Returns size of field.
///
/// Return value is more than 16 only for unsized fields. It is 17 + number_of_unsized_fields before.
/// So for calculate size of unsized field, we can iterate only size - 17 elements and add the size of them.
pub fn get_size(field_type: FieldType, number_of_unsized_fields: usize) -> usize {
    match field_type {
        FieldType::Byte => 1,
        FieldType::Bool => 1,
        FieldType::Uint8 => 1,
        FieldType::Uint16 => 2,
        FieldType::Uint32 => 4,
        FieldType::Uint64 => 8,
        FieldType::Uint128 => 16,
        FieldType::Int8 => 1,
        FieldType::Int16 => 2,
        FieldType::Int32 => 4,
        FieldType::Int64 => 8,
        FieldType::Int128 => 16,
        FieldType::Float32 => 4,
        FieldType::Float64 => 8,
        FieldType::String => 17 + number_of_unsized_fields,
        FieldType::ByteSlice => 17 + number_of_unsized_fields,
    }
}