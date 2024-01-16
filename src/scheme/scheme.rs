use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use crate::bin_types::BinValue;
use crate::scheme::field_info::{field_type_from_string, FieldInfo, get_size};
use crate::utils::fastbytes::uint::{u16, u64};
use crate::writers::get_size_for_value_len;

pub type Scheme = Box<[FieldInfo]>;

#[derive(Serialize, Deserialize, Debug)]
pub struct SchemeJSON {
    pub sized_fields: Map<String, Value>,
    pub unsized_fields: Map<String, Value>,
}

/// Get JSON scheme with 2 fields: sized_fields and unsized_fields.
///
/// sized_fields and unsized_fields are maps with key = name and value = type.
pub fn scheme_from_bytes(data: &[u8]) -> Result<Scheme, &'static str> {
    let scheme_json: SchemeJSON = match serde_json::from_slice(data) {
        Ok(scheme_json) => scheme_json,
        Err(_) => return Err("Scheme is not valid JSON"),
    };
    let mut scheme = Vec::with_capacity(scheme_json.sized_fields.len() + scheme_json.unsized_fields.len());

    let mut cur_offset = 0;
    let mut number_of_unsized_fields = 0;
    for (_key, field_type) in scheme_json.sized_fields {
        if !field_type.is_string() {
            return Err("Fields type must be a string");
        }

        let res = field_type_from_string(field_type.as_str().unwrap());
        if res.is_err() {
            return Err(res.err().unwrap());
        }

        let size = get_size(res.unwrap(), number_of_unsized_fields);
        scheme.push(FieldInfo::new(size, cur_offset));
        cur_offset += size;
    }
    let offset_to_unsized_fields = cur_offset;

    for (_key, field_type) in scheme_json.unsized_fields {
        if !field_type.is_string() {
            return Err("Fields type must be a string");
        }

        let res = field_type_from_string(field_type.as_str().unwrap());
        if res.is_err() {
            return Err(res.err().unwrap());
        }

        let size = get_size(res.unwrap(), number_of_unsized_fields);
        number_of_unsized_fields += 1;
        scheme.push(FieldInfo::new(size, offset_to_unsized_fields));
    }

    return Ok(scheme.into_boxed_slice());
}

pub fn empty_scheme() -> Scheme {
    vec![].into_boxed_slice()
}

#[inline(always)]
pub fn get_field(value: &BinValue, scheme: &Scheme, number: usize) -> Vec<u8> {
    let info = &scheme[number];
    let size = info.size;
    let mut offset = info.offset;
    let ptr;
    {
        let len = value.len();
        let size = get_size_for_value_len(len);
        ptr = unsafe { value.ptr.add(size) };
    }
    // get sized
    if size < 17 {
        let mut buf = vec![0u8; size + 2];
        buf[0] = size as u8;
        buf[1] = 0;
        unsafe {
            std::ptr::copy_nonoverlapping(ptr.add(offset), buf.as_mut_ptr().add(2), size);
        }
        return buf;
    }

    let necessary_to_read_len = size - 17;
    let mut len_of_field;
    let mut buf_for_field_len = [0;4];
    // Here we read a few fields to find an offset to needed field.
    for _ in 0..necessary_to_read_len {
        unsafe {
            std::ptr::copy_nonoverlapping(ptr.add(offset), buf_for_field_len.as_mut_ptr(), 2);
        }
        if buf_for_field_len[1] < 255 || buf_for_field_len[0] < 255 {
            len_of_field = buf_for_field_len[0] as usize | (buf_for_field_len[1] as usize) << 8;
            offset += 2;
        } else {
            offset += 2;
            unsafe {
                std::ptr::copy_nonoverlapping(ptr.add(offset), buf_for_field_len.as_mut_ptr(), 4);
                len_of_field = buf_for_field_len[0] as usize | (buf_for_field_len[1] as usize) << 8 | (buf_for_field_len[2] as usize) << 16 | (buf_for_field_len[3] as usize) << 24;
            }
            offset += 4;
        }

        offset += len_of_field;
    }

    let size_of_field_len;
    unsafe {
        std::ptr::copy_nonoverlapping(ptr.add(offset), buf_for_field_len.as_mut_ptr(), 2);
    }
    if buf_for_field_len[1] < 255 || buf_for_field_len[0] < 255 {
        len_of_field = buf_for_field_len[0] as usize | (buf_for_field_len[1] as usize) << 8;
        size_of_field_len = 2;
    } else {
        unsafe {
            std::ptr::copy_nonoverlapping(ptr.add(offset + 2), buf_for_field_len.as_mut_ptr(), 4);
        }
        len_of_field = buf_for_field_len[0] as usize | (buf_for_field_len[1] as usize) << 8 | (buf_for_field_len[2] as usize) << 16 | (buf_for_field_len[3] as usize) << 24;
        size_of_field_len = 6;
    }

    let real_len = len_of_field + size_of_field_len;
    let mut buf = vec![0u8;real_len];
    unsafe {
        std::ptr::copy_nonoverlapping(ptr.add(offset), buf.as_mut_ptr(), real_len);
    }

    return buf;
}

#[inline(always)]
pub fn get_fields(value: &BinValue, scheme: &Scheme, numbers: &[usize]) -> Vec<u8> {
    let mut response = Vec::with_capacity(numbers.len() * 20);
    let mut written = 0;
    let ptr;
    {
        let len = value.len();
        let size = get_size_for_value_len(len);
        ptr = unsafe { value.ptr.add(size) };
    }
    let mut buf_for_field_len = [0;4];
    let mut read = 0;
    let mut unsized_field_offset = usize::MAX;

    for number in numbers.iter() {
        let info = &scheme[*number];
        let size = info.size;
        let offset = info.offset;
        // get sized
        if size < 17 {
            response.resize(response.len() + size + 2, 0);
            response[written] = size as u8;
            written += 2;
            unsafe {
                std::ptr::copy_nonoverlapping(ptr.add(offset), response.as_mut_ptr().add(written), size);
            }
            written += size;
            continue;
        }

        if unsized_field_offset == usize::MAX {
            unsized_field_offset = offset;
        }

        let necessary_to_read_len = size - 17;
        let mut len_of_field;
        // Here we read a few fields to find an offset to needed field.
        for _ in read..necessary_to_read_len {
            unsafe {
                std::ptr::copy_nonoverlapping(ptr.add(unsized_field_offset), buf_for_field_len.as_mut_ptr(), 2);
            }
            if buf_for_field_len[1] < 255 || buf_for_field_len[0] < 255 {
                len_of_field = buf_for_field_len[0] as usize | (buf_for_field_len[1] as usize) << 8;
                unsized_field_offset += 2;
            } else {
                unsized_field_offset += 2;
                unsafe {
                    std::ptr::copy_nonoverlapping(ptr.add(unsized_field_offset), buf_for_field_len.as_mut_ptr(), 4);
                    len_of_field = buf_for_field_len[0] as usize | (buf_for_field_len[1] as usize) << 8 | (buf_for_field_len[2] as usize) << 16 | (buf_for_field_len[3] as usize) << 24;
                }
                unsized_field_offset += 4;
            }

            unsized_field_offset += len_of_field;
            read += 1;
        }

        let size_of_field_len;
        unsafe {
            std::ptr::copy_nonoverlapping(ptr.add(unsized_field_offset), buf_for_field_len.as_mut_ptr(), 2);
        }
        if buf_for_field_len[1] < 255 || buf_for_field_len[0] < 255 {
            len_of_field = buf_for_field_len[0] as usize | (buf_for_field_len[1] as usize) << 8;
            size_of_field_len = 2;
        } else {
            unsafe {
                std::ptr::copy_nonoverlapping(ptr.add(unsized_field_offset + 2), buf_for_field_len.as_mut_ptr(), 4);
            }
            len_of_field = buf_for_field_len[0] as usize | (buf_for_field_len[1] as usize) << 8 | (buf_for_field_len[2] as usize) << 16 | (buf_for_field_len[3] as usize) << 24;
            size_of_field_len = 6;
        }

        let real_len = len_of_field + size_of_field_len;
        response.resize(response.len() + real_len, 0);
        unsafe {
            std::ptr::copy_nonoverlapping(ptr.add(unsized_field_offset), response.as_mut_ptr().add(response.len() - real_len), real_len);
        }
    }

    return response;
}

#[test]
fn test_scheme() {
    let mut scheme_to_json: SchemeJSON = SchemeJSON{
        sized_fields: Map::with_capacity(10),
        unsized_fields: Map::with_capacity(100),
    };
    for i in 0..10 {
        scheme_to_json.sized_fields.insert(i.to_string(), json!("Uint64".to_string()));
    }
    for i in 10..110 {
        scheme_to_json.unsized_fields.insert(i.to_string(), json!("String".to_string()));
    }
    let scheme_json = serde_json::to_vec_pretty(&scheme_to_json).expect("Can't serialize scheme");
    let scheme = scheme_from_bytes(&scheme_json).expect("Can't deserialize scheme");

    let mut value = Vec::with_capacity(80 + (12 * 100));
    for i in 0..10u64 {
        value.extend_from_slice(&i.to_le_bytes());
    }

    for i in 0..100u64 {
        value.extend_from_slice(&[10, 0]);
        let mut v = Vec::with_capacity(10);
        v.extend_from_slice("value12".as_bytes());
        let mut str_i = i.to_string();
        if str_i.len() == 2 {
            str_i += "0";
        } else if str_i.len() == 1 {
            str_i += "00";
        }
        v.extend_from_slice(str_i.as_bytes());
        value.extend_from_slice(&v);
    }
    let bin_value = BinValue::new(&value);

    for i in 0..10u64 {
        let single_sized_res = get_field(&bin_value, &scheme, i as usize);
        if single_sized_res.len() != 10 {
            panic!("Wrong sized field len");
        }
        if u64(&single_sized_res[2..]) != i {
            panic!("Wrong sized field value");
        }
    }

    for i in 10..110u64 {
        let single_unsized_res = get_field(&bin_value, &scheme, i as usize);
        if single_unsized_res.len() != 12 {
            panic!("Wrong unsized field len");
        }
        let mut v = Vec::with_capacity(10);
        v.extend_from_slice("value12".as_bytes());
        let mut str_i = i.to_string();
        if str_i.len() == 2 {
            str_i += "0";
        } else if str_i.len() == 1 {
            str_i += "00";
        }
        v.extend_from_slice(str_i.as_bytes());
        if single_unsized_res[2..].eq(&v) {
            panic!("Wrong unsized field value: {:?} != {:?}", &single_unsized_res[2..], &v);
        }
    }

    let fields = get_fields(&bin_value, &scheme, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    if fields.len() != 100 {
        panic!("Wrong fields len");
    }
    for i in 0..10 {
        let field = &fields[i*10..(i+1)*10];
        let size = u16(&field[0..2]);
        if size != 8 {
            panic!("Wrong sized field len in get fields test: {} != 8, field: {:?}", size, &field);
        }
        let value = u64(&field[2..10]);
        if value != (i as u64) {
            panic!("Wrong sized field value in get sized fields test");
        }
    }

    let fields = get_fields(&bin_value, &scheme, &[105, 106, 107, 108, 109]);
    if fields.len() != 60 {
        panic!("Wrong fields len");
    }
    for i in 0..5 {
        let field = &fields[i*12..(i+1)*12];
        let size = u16(&field[0..2]);
        if size != 10 {
            panic!("Wrong unsized field len in get unsized fields test: {} != 10, field: {:?}", size, field);
        }
        let mut v = Vec::with_capacity(10);
        v.extend_from_slice("value12".as_bytes());
        let mut str_i = (i + 95).to_string();
        if str_i.len() == 2 {
            str_i += "0";
        } else if str_i.len() == 1 {
            str_i += "00";
        }
        v.extend_from_slice(str_i.as_bytes());
        if field[2..] != v {
            panic!("Wrong unsized field value in get unsized fields test: {:?} != {:?}", &field[2..], &v);
        }
    }
}
