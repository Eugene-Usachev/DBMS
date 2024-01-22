use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use crate::bin_types::BinValue;
use crate::scheme::field_info::{field_type_from_string, FieldInfo, get_size};
use crate::utils::fastbytes::uint::{u16, u64, u64tob};
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
    let mut buf = vec![0u8; real_len];
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

#[inline(always)]
pub fn update_field(value: &mut BinValue, scheme: &Scheme, number: usize, new_value: &[u8]) {
    let info = &scheme[number];
    let size = info.size;
    let mut offset = info.offset;
    let bin_value_len = value.len();
    let bin_value_size = get_size_for_value_len(bin_value_len);
    let ptr = unsafe {
        value.ptr.add(bin_value_size)
    };
    if size < 17 {
        unsafe {
            std::ptr::copy_nonoverlapping(new_value.as_ptr(), ptr.add(offset), size);
        }
        return;
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

    if len_of_field == new_value.len() {
        // fast
        unsafe {
            std::ptr::copy_nonoverlapping(new_value.as_ptr(), ptr.add(offset + size_of_field_len), len_of_field);
        }
        return;
    }

    // slow
    let real_len_of_old_value = len_of_field + size_of_field_len;
    let new_value_len_size = get_size_for_value_len(new_value.len());
    let new_bin_value_len = bin_value_len - real_len_of_old_value + new_value.len() + new_value_len_size;
    let mut buf = BinValue::with_len(new_bin_value_len);
    let mut buf_offset = get_size_for_value_len(new_bin_value_len);
    unsafe {
        // copy before
        std::ptr::copy_nonoverlapping(ptr, buf.ptr.add(buf_offset), offset);
        buf_offset += offset;
        // copy new value
        if new_value_len_size < 65535 {
            *buf.ptr.add(buf_offset) = new_value.len() as u8;
            buf_offset += 1;
            *buf.ptr.add(buf_offset) = (new_value.len() >> 8) as u8;
            buf_offset += 1;
        } else {
            let tmp_buf = [255, 255, new_value.len() as u8, (new_value.len() >> 8) as u8, (new_value.len() >> 16) as u8, (new_value.len() >> 24) as u8];
            std::ptr::copy_nonoverlapping(tmp_buf.as_ptr(), buf.ptr.add(buf_offset), 6);
            buf_offset += 6;
        }
        std::ptr::copy_nonoverlapping(new_value.as_ptr(), buf.ptr.add(buf_offset), new_value.len());
        offset += real_len_of_old_value;
        let left_bytes = bin_value_len + bin_value_size - offset;
        if left_bytes > 0 {
            buf_offset += new_value.len();
            // copy after value
            std::ptr::copy_nonoverlapping(ptr.add(offset), buf.ptr.add(buf_offset), left_bytes);
        }
        // TODO: check for memory leak
        *value = buf;
    }
}

// TODO: update_fields
// #[inline(always)]
// pub fn update_fields(value: &mut BinValue, scheme: &Scheme, numbers: &[usize], new_values: &[u8]) {
//     let mut diff: isize = 0;
//     let mut new_values_offset = 0;
//     let mut i = 0;
//     let mut j = 0;
//     let mut offset;
//     let bin_value_len = value.len();
//     let bin_value_size = get_size_for_value_len(bin_value_len);
//     let mut offsets_to_unsized_fields = Vec::with_capacity(numbers.len());
//     let mut buf_for_field_len = [0;4];
//     let mut len_of_field;
//     let ptr = unsafe {
//         value.ptr.add(bin_value_size)
//     };
//
//     // we update sized value fields
//     loop {
//         let ref info = scheme[numbers[i]];
//         offset = info.offset;
//         if info.size > 16 {
//             break;
//         }
//
//         unsafe {
//             std::ptr::copy_nonoverlapping(new_values.as_ptr().add(new_values_offset), ptr.add(offset), info.size);
//         }
//         new_values_offset += info.size;
//
//         i += 1;
//         j = i;
//         if (numbers.len() == i) {
//             break;
//         }
//     }
//
//     loop {
//         unsafe {
//             std::ptr::copy_nonoverlapping(ptr.add(offset), buf_for_field_len.as_mut_ptr(), 2);
//         }
//         if buf_for_field_len[1] < 255 || buf_for_field_len[0] < 255 {
//             len_of_field = buf_for_field_len[0] as usize | (buf_for_field_len[1] as usize) << 8;
//             offset += 2;
//         } else {
//             offset += 2;
//             unsafe {
//                 std::ptr::copy_nonoverlapping(ptr.add(offset), buf_for_field_len.as_mut_ptr(), 4);
//                 len_of_field = buf_for_field_len[0] as usize | (buf_for_field_len[1] as usize) << 8 | (buf_for_field_len[2] as usize) << 16 | (buf_for_field_len[3] as usize) << 24;
//             }
//             offset += 4;
//         }
//
//         offset += len_of_field;
//
//         if numbers[j] == i {
//             let old_len = len_of_field;
//             unsafe {
//                 std::ptr::copy_nonoverlapping(new_values.as_ptr().add(new_values_offset), buf_for_field_len.as_mut_ptr(), 2);
//             }
//             if buf_for_field_len[1] < 255 || buf_for_field_len[0] < 255 {
//                 len_of_field = buf_for_field_len[0] as usize | (buf_for_field_len[1] as usize) << 8;
//                 new_values_offset += 2;
//             } else {
//                 new_values_offset += 2;
//                 unsafe {
//                     std::ptr::copy_nonoverlapping(new_values.as_ptr().add(new_values_offset), buf_for_field_len.as_mut_ptr(), 4);
//                     len_of_field = buf_for_field_len[0] as usize | (buf_for_field_len[1] as usize) << 8 | (buf_for_field_len[2] as usize) << 16 | (buf_for_field_len[3] as usize) << 24;
//                 }
//                 new_values_offset += 4;
//             }
//             new_values_offset += new_values_offset;
//
//             offsets_to_unsized_fields.push(offset);
//             j += 1;
//             if (j == numbers.len()){
//                 break;
//             }
//         }
//         i += 1;
//     }
// }

#[test]
fn test_scheme_get() {
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

#[test]
fn test_scheme_update() {
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
    let mut bin_value = BinValue::new(&value);

    let copy = bin_value.clone();
    let new_value = &u64tob(2555)[..];
    update_field(&mut bin_value, &scheme, 0, new_value);
    // check that the field have been updated
    if !get_field(&bin_value, &scheme, 0)[2..].eq(new_value) {
        panic!("Wrong value after update! {:?} != {:?}", &get_field(&bin_value, &scheme, 0)[2..], new_value);
    }
    // check that all other fields have not been updated
    if !copy.deref()[8..].eq(&bin_value.deref()[8..]) {
        panic!("Wrong update! All value have been updated!");
    }

    let copy = bin_value.clone();
    update_field(&mut bin_value, &scheme, 5, new_value);

    // check that the field have been updated
    if !get_field(&bin_value, &scheme, 5)[2..].eq(new_value) {
        panic!("Wrong value after update! {:?} != {:?}", &get_field(&bin_value, &scheme, 5)[2..], new_value);
    }

    // check that all other fields have not been updated
    if !copy.deref()[..8*5].eq(&bin_value.deref()[..8*5]) || !copy.deref()[8*6..].eq(&bin_value.deref()[8*6..]) {
        panic!("Wrong update! All value have been updated!");
    }

    // check update unsized field but with the same size
    let copy = bin_value.clone();
    let new_value = "value12000".as_bytes();
    update_field(&mut bin_value, &scheme, 10, new_value);

    // check that the field have been updated
    if !get_field(&bin_value, &scheme, 10)[2..].eq(new_value) {
        panic!("Wrong value after update! {:?} != {:?}", &get_field(&bin_value, &scheme, 10)[2..], new_value);
    }

    // check that all other fields have not been updated
    if !copy.deref()[..8*10].eq(&bin_value.deref()[..8*10]) || !copy.deref()[8*10 + 12..].eq(&bin_value.deref()[8*10 + 12..]) {
        panic!("Wrong update! All value have been updated!");
    }

    let copy = bin_value.clone();
    let new_value = "1".as_bytes();
    update_field(&mut bin_value, &scheme, 50, new_value);
    if bin_value.len() != copy.len() - 9 {
        panic!("Wrong update! All value have been updated! Len is {}, must be {}", bin_value.len(), copy.len() - 9);
    }

    // check that the field have been updated
    if !get_field(&bin_value, &scheme, 50)[2..].eq(new_value) {
        panic!("Wrong value after update! {:?} != {:?}", &get_field(&bin_value, &scheme, 50)[2..], new_value);
    }


    // check that all other fields have not been updated
    for i in 0..110 {
        let sized_field = get_field(&bin_value, &scheme, i);
        if !sized_field.eq(&get_field(&copy, &scheme, i)) {
            if i == 50 {
                continue;
            }
            panic!("Field {} have been updated! {:?} != {:?}", i, sized_field, get_field(&copy, &scheme, i));
        }
    }
}
