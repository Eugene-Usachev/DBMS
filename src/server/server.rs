use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc};
use std::thread;

use crate::constants::actions;
use crate::constants::size::{READ_BUFFER_SIZE, READ_BUFFER_SIZE_WITHOUT_SIZE, WRITE_BUFFER_SIZE};
use crate::server::cfg::Config;
use crate::space::cache_space::CacheSpace;
use crate::space::space::{SpaceInterface};
use crate::space::in_memory_space::InMemorySpace;
use crate::storage::Storage;
use crate::utils::fastbytes::uint;

pub struct Server {
    storage: Arc<Storage>,
    is_running: bool,
    password: String,
    port: u16,
}

impl Server {
    pub(crate) fn new() -> Self {
        let config = Config::new();
        let storage = Arc::new(Storage::new());
        Storage::start_cache_clearer(storage.clone());
        Self {
            storage: storage.clone(),
            port: config.port,
            password: config.password,
            is_running: false,
        }
    }

    pub(crate) fn run(&mut self) {
        if self.is_running {
            return;
        }
        self.is_running = true;
        let listener_ = TcpListener::bind(format!("dbms:{}", self.port));
        let listener = match listener_ {
            Ok(listener) => listener,
            Err(e) => {
                panic!("Can't bind to port: {}, the error is: {:?}", self.port, e);
            }
        };
        println!("Server tcp listening on port {}", self.port);
        for stream in listener.incoming() {
            let storage= self.storage.clone();
            thread::spawn(move || {
                match stream {
                    Ok(stream) => {
                        Self::handle_client(storage, stream);
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            });
        }
    }

    #[inline(always)]
    fn handle_client(storage: Arc<Storage>, mut stream: TcpStream) {
        let mut read_buffer = [0u8; READ_BUFFER_SIZE];
        let mut write_buffer = [0u8; WRITE_BUFFER_SIZE];
        while match stream.read(&mut read_buffer) {
            Ok(0) => false,
            Ok(mut pipe_size) => {
                let real_pipe_size = uint::u32(&read_buffer[0..4]);
                while real_pipe_size != pipe_size as u32 {
                    match stream.read(&mut read_buffer[pipe_size..]) {
                        Ok(0) => {
                            break;
                        }
                        Ok(size) => {
                            pipe_size += size;
                        }
                        Err(e) => {
                            println!("Error: {}", e);
                        }
                    }
                }
                let mut offset = 4;
                let mut write_offset = 0;
                loop {
                    if offset >= pipe_size - 2 {
                        break;
                    }
                    let size:u16 = uint::u16(&read_buffer[offset..offset+2]);
                    if size == 65535 {
                        let big_size:u32 = uint::u32(&read_buffer[offset+2..offset+6]);
                        offset += 6;
                        write_offset = Self::handle_message(&mut stream, storage.clone(), &read_buffer[offset..offset + big_size as usize], &mut write_buffer, write_offset);
                        offset += big_size as usize;
                    } else {
                        offset += 2;
                        write_offset = Self::handle_message(&mut stream, storage.clone(), &read_buffer[offset..offset + size as usize], &mut write_buffer, write_offset);
                        offset += size as usize;
                    }
                }
                stream.write_all(&write_buffer[..write_offset]).expect("Can't write to stream");
                true
            },
            Err(e) => {
                println!("An error occurred, terminating connection with {}, error has a message: {:?}", stream.peer_addr().unwrap(), e);
                stream.shutdown(std::net::Shutdown::Both).unwrap();
                false
            }
        } {};
    }

    #[inline(always)]
    fn handle_message(stream: &mut TcpStream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize) -> usize {
        return match message[0] {
            actions::PING => {
                write_msg(stream, write_buf, write_offset, &[actions::PING])
            },
            actions::CREATE_SPACE_IN_MEMORY => {
                if message.len() < 4 {
                    return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
                }
                let mut spaces;
                let size = uint::u16(&message[1..3]);
                let name = String::from_utf8(message[3..].to_vec()).unwrap();
                match storage.spaces_names.write() {
                    Ok(mut spaces_names) => {
                        let mut i = 0;
                        for exists_name in spaces_names.iter() {
                            if *exists_name == name {
                                return write_msg(stream, write_buf, write_offset, &[actions::DONE, i as u8, ((i as u16) >> 8) as u8]);
                            }
                            i += 1;
                        }
                        spaces_names.push(name);
                    }
                    Err(_) => {
                        return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
                    }
                }
                let spaces_not_unwrapped = storage.spaces.write();
                match spaces_not_unwrapped {
                    Ok(spaces_unwrapped) => {
                        spaces = spaces_unwrapped;
                    }
                    Err(_) => {
                        return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
                    }
                }
                let l = spaces.len();
                spaces.push(
                    Box::new(InMemorySpace::new(size as usize))
                );
                write_msg(stream, write_buf, write_offset, &[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
            }
            actions::CREATE_SPACE_CACHE => {
                if message.len() < 8 {
                    return write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST]);
                }
                let mut spaces;
                let size = uint::u16(&message[1..3]);
                let cache_duration = uint::u64(&message[3..11]);
                let name = String::from_utf8(message[11..].to_vec()).unwrap();
                match storage.spaces_names.write() {
                    Ok(mut spaces_names) => {
                        let mut i = 0;
                        for exists_name in spaces_names.iter() {
                            if *exists_name == name {
                                return write_msg(stream, write_buf, write_offset, &[actions::DONE, i as u8, ((i as u16) >> 8) as u8]);
                            }
                            i += 1;
                        }
                        spaces_names.push(name);
                    }
                    Err(_) => {
                        return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
                    }
                }
                let spaces_not_unwrapped = storage.spaces.write();
                match spaces_not_unwrapped {
                    Ok(spaces_unwrapped) => {
                        spaces = spaces_unwrapped;
                    }
                    Err(_) => {
                        return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
                    }
                }

                let l = spaces.len();
                spaces.push(
                    Box::new(CacheSpace::new(size as usize, cache_duration))
                );
                storage.cache_spaces_indexes.write().unwrap().push(l);
                write_msg(stream, write_buf, write_offset, &[actions::DONE, l as u8, ((l as u16) >> 8) as u8])
            }
            actions::GET_SPACES_NAMES => {
                let spaces_names;
                let spaces_names_not_unwrapped = storage.spaces_names.read();
                match spaces_names_not_unwrapped {
                    Ok(spaces_names_unwrapped) => {
                        spaces_names = spaces_names_unwrapped;
                    }
                    Err(_) => {
                        return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
                    }
                }

                let mut local_buffer = [0u8;32367];
                let mut local_offset = 1;

                local_buffer[0] = actions::DONE;
                for name in spaces_names.iter() {
                    let l = name.len() as u16;
                    local_buffer[local_offset..local_offset+2].copy_from_slice(&[l as u8, ((l >> 8) as u8)]);
                    local_buffer[local_offset+2..local_offset+2+l as usize].copy_from_slice(name.as_bytes());
                    local_offset += 2 + l as usize;
                }
                write_msg(stream, write_buf, write_offset, &local_buffer[..local_offset])
            },
            actions::GET => {
                let mut spaces;
                let spaces_not_unwrapped = storage.spaces.read();
                match spaces_not_unwrapped {
                    Ok(spaces_unwrapped) => {
                        spaces = spaces_unwrapped;
                    }
                    Err(_) => {
                        return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
                    }
                }
                let key = &message[3..].to_vec();
                match spaces.get(uint::u16(&message[1..3]) as usize) {
                    Some(space) => {
                        let res = space.get(key);
                        if res.is_none() {
                            return write_msg(stream, write_buf, write_offset, &[actions::NOT_FOUND]);
                        }
                        let value = res.unwrap();
                        let l = value.len() as u16;
                        let mut v = Vec::with_capacity(3 + value.len());
                        v.append(&mut vec![actions::DONE, l as u8, ((l >> 8) as u8)]);
                        v.append(&mut value.clone());
                        write_msg(stream, write_buf, write_offset, v.as_slice())
                    }
                    None => {
                        write_msg(stream, write_buf, write_offset, &[actions::SPACE_NOT_FOUND])
                    }
                }
            },
            actions::INSERT => {
                let mut spaces;
                let spaces_not_unwrapped = storage.spaces.read();
                match spaces_not_unwrapped {
                    Ok(spaces_unwrapped) => {
                        spaces = spaces_unwrapped;
                    }
                    Err(_) => {
                        return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
                    }
                }
                let key_size = uint::u16(&message[3..5]) as usize;
                let key = message[5..5+key_size].to_vec();
                let value = message[5+key_size..].to_vec();
                return match spaces.get(uint::u16(&message[1..3]) as usize) {
                    Some(space) => {
                        space.insert(value, key);
                        write_msg(stream, write_buf, write_offset, &[actions::DONE])
                    }
                    None => {
                        write_msg(stream, write_buf, write_offset, &[actions::SPACE_NOT_FOUND])
                    }
                }
            },
            actions::SET => {
                let mut spaces;
                let spaces_not_unwrapped = storage.spaces.read();
                match spaces_not_unwrapped {
                    Ok(spaces_unwrapped) => {
                        spaces = spaces_unwrapped;
                    }
                    Err(_) => {
                        return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
                    }
                }
                let key_size = uint::u16(&message[3..5]) as usize;
                let key = message[5..5+key_size].to_vec();
                let value = message[5+key_size..].to_vec();
                return match spaces.get(uint::u16(&message[1..3]) as usize) {
                    Some(space) => {
                        space.set(value, key);
                        write_msg(stream, write_buf, write_offset, &[actions::DONE])
                    }
                    None => {
                        write_msg(stream, write_buf, write_offset, &[actions::SPACE_NOT_FOUND])
                    }
                }
            },
            actions::DELETE => {
                let mut spaces;
                let spaces_not_unwrapped = storage.spaces.read();
                match spaces_not_unwrapped {
                    Ok(spaces_unwrapped) => {
                        spaces = spaces_unwrapped;
                    }
                    Err(_) => {
                        return write_msg(stream, write_buf, write_offset, &[actions::INTERNAL_ERROR]);
                    }
                }
                let key = &message[3..].to_vec();
                match spaces.get(uint::u16(&message[1..3]) as usize) {
                    Some(space) => {
                        space.delete(key);
                        write_msg(stream, write_buf, write_offset, &[actions::DONE])
                    }
                    None => {
                        write_msg(stream, write_buf, write_offset, &[actions::SPACE_NOT_FOUND])
                    }
                }
            },
            _ => {
                write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST])
            }
        }
    }
}

#[inline(always)]
fn write_msg(stream: &mut TcpStream, buf: &mut [u8], mut offset: usize, msg: &[u8]) -> usize {
    let l = msg.len();

    if l + offset > READ_BUFFER_SIZE_WITHOUT_SIZE {
        stream.write(&buf).expect("Can't write to stream");
        offset = 0; // We flushed the buffer. Now we need to start from the beginning, but we still are responding for the same pipe.
    }

    // 65535 is 2 << 16 - 1
    if l < 65535 {
        buf[offset..offset+2].copy_from_slice(&[l as u8, ((l >> 8) as u8)]);
        offset += 2;
    } else {
        buf[offset..offset+6].copy_from_slice(&[255, 255, l as u8, ((l >> 8) as u8), ((l >> 16) as u8), ((l >> 24) as u8)]);
        offset += 6;
    }

    // We try to write all the message. If l > allowed size, we write a lot of times.
    let mut can_write = READ_BUFFER_SIZE - offset;
    let mut written = 0;
    while l > can_write {
        buf[offset..offset+can_write].copy_from_slice(&msg[written..can_write]);
        written += can_write;
        stream.write(&buf).expect("Can't write to stream");
        offset = 0;
        can_write = READ_BUFFER_SIZE;
    }

    buf[offset..offset+l].copy_from_slice(msg);
    return offset + l;
}