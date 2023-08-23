use std::borrow::{Borrow, BorrowMut};
use std::fmt::Error;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use std::thread;

use crate::server::cfg::Config;
use crate::constants::actions;
use crate::constants::size;
use crate::space::space::{Space, SpaceEngineType, SpaceInterface, CACHE};
use crate::console::write_errors;
use crate::utils;
use crate::utils::fastbytes::uint;

pub struct ServerInner {
    spaces: RwLock<Vec<Space>>,
    spaces_names: RwLock<Vec<String>>,
}

pub struct Server {
    inner: Arc<ServerInner>,
    is_running: bool,
    password: String,
    port: u16,
}

impl Server {
    pub(crate) fn new() -> Self {
        let config = Config::new();
        Self {
            inner: Arc::new(ServerInner{
                spaces: RwLock::new(Vec::with_capacity(1)),
                spaces_names: RwLock::new(Vec::with_capacity(1)),
            }),
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
        let mut listener = match listener_ {
            Ok(listener) => listener,
            Err(e) => {
                panic!("Can't bind to port: {}, the error is: {:?}", self.port, e);
            }
        };
        println!("Server tcp listening on port {}", self.port);
        for stream in listener.incoming() {
            let server= self.inner.clone();
            thread::spawn(move || {
                match stream {
                    Ok(mut stream) => {
                        Self::handle_client(server, stream);
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            });
        }
    }

    #[inline(always)]
    fn handle_client(server: Arc<ServerInner>, mut stream: TcpStream) {
        let mut read_buffer = [0u8; size::READ_BUFFER_SIZE];
        let mut write_buffer = [0u8; size::WRITE_BUFFER_SIZE];
        while match stream.read(&mut read_buffer) {
            Ok(0) => false,
            Ok(size) => {
                // let mut offset = 0;
                // loop {
                //     let spaces = Arc::clone(&spaces);
                //     if offset == size {
                //         break;
                //     }
                //     let size:u16 = utils::fastbytes::uint::u16(&read_buffer[offset..offset+2]);
                //     if size == 65535 {
                //         let big_size:u32 = utils::fastbytes::uint::u32(&read_buffer[offset+2..offset+6]);
                //         offset += 6 + big_size as usize;
                //         write_offset = Self::handle_message(spaces, &read_buffer[offset..offset + big_size as usize], &mut write_buffer, write_offset);
                //     } else {
                //         offset += 2 + size as usize;
                //         write_offset = Self::handle_message(spaces, &read_buffer[offset..size as usize], &mut write_buffer, write_offset);
                //     }
                // }
                let write_size = Self::handle_message(server.clone(), &read_buffer[..size], &mut write_buffer);
                stream.write_all(&mut write_buffer[..write_size]).expect("Can't write to stream");
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
    fn handle_message(server: Arc<ServerInner>, message: &[u8], buf: &mut [u8]) -> usize {
        return match message[0] {
            actions::PING => {
                buf[0] = actions::PING;
                1
            },
            actions::CREATE_SPACE => {
                let mut spaces;
                let spaces_not_unwrapped = server.spaces.write();
                match spaces_not_unwrapped {
                    Ok(spaces_unwrapped) => {
                        spaces = spaces_unwrapped;
                    }
                    Err(_) => {
                        buf[0] = actions::INTERNAL_ERROR;
                        return 1;
                    }
                }
                if message.len() < 7 {
                    buf[0] = actions::BAD_REQUEST;
                    return 1;
                }
                let engine_type = message[1];
                let size = uint::u32(&message[2..6]);
                let name = String::from_utf8(message[6..].to_vec()).unwrap();
                match engine_type {
                    CACHE => {
                        match server.spaces_names.write() {
                        Ok(mut spaces_names) => {
                            let mut i = 0;
                            for exists_name in spaces_names.iter() {
                                if *exists_name == name {
                                    buf[0..3].copy_from_slice(&[actions::DONE, i as u8, ((i as u16) >> 8) as u8]);
                                    return 3;
                                }
                                i += 1;
                            }
                            spaces_names.push(name);
                        }
                        Err(_) => {
                            buf[0] = actions::INTERNAL_ERROR;
                            return 1;
                        }
                    }
                        spaces.push(
                            Space::new(CACHE, size as usize)
                        );
                        let l = spaces.len() - 1;
                        buf[0..3].copy_from_slice(&[actions::DONE, l as u8, ((l as u16) >> 8) as u8]);
                        3
                    }
                    _ => {
                        buf[0] = actions::BAD_REQUEST;
                        return 1;
                    }
                }
            },
            actions::GET_SPACES_NAMES => {
                let mut spaces_names;
                let spaces_names_not_unwrapped = server.spaces_names.read();
                match spaces_names_not_unwrapped {
                    Ok(spaces_names_unwrapped) => {
                        spaces_names = spaces_names_unwrapped;
                    }
                    Err(_) => {
                        buf[0] = actions::INTERNAL_ERROR;
                        return 1;
                    }
                }

                buf[0] = actions::DONE;
                let mut offset = 1;

                for name in spaces_names.iter() {
                    let l = name.len() as u16;
                    buf[offset..offset+2].copy_from_slice(&[l as u8, ((l >> 8) as u8)]);
                    buf[offset+2..offset+2+l as usize].copy_from_slice(name.as_bytes());
                    offset += 2 + l as usize;
                }

                offset
            },
            actions::INSERT => {
                0
            },
            _ => {
                0
            }
        }
    }
}

#[inline(always)]
fn write_msg(buf: &mut [u8], offset: usize, msg: &[u8]) -> usize {
    let l = msg.len();
    if l < 65535 {
        buf[offset..offset + l].copy_from_slice(msg);
        offset + 2 + l
    } else {
        buf[offset..offset + l].copy_from_slice(msg);
        offset + 6 + l
    }
}