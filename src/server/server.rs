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
use crate::space::space::{Space, SpaceEngineType, SpaceInterface};
use crate::console::write_errors;
use crate::utils;
use crate::utils::fastbytes::uint;

pub(crate) struct Server {
    port: u16,
    is_running: bool,
    password: String,
    spaces: Arc<RwLock<Vec<Space>>>,
}

impl Server {
    pub(crate) fn new() -> Self {
        let config = Config::new();
        Self {
            port: config.port,
            is_running: false,
            password: config.password,
            spaces: Arc::new(RwLock::new(Vec::with_capacity(1))),
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
            let spaces = Arc::clone(&self.spaces);
            thread::spawn(move || {
                match stream {
                    Ok(mut stream) => {
                        let mut buf = [actions::PING;1];
                        while match stream.read(&mut buf) {
                            Ok(0) => false,
                            Ok(size) => {
                                stream.write(&buf).unwrap();
                                true
                            },
                            Err(_) => {
                              false
                            }
                        } {}
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            });
        }
    }

    #[inline(always)]
    fn handle_client(spaces: Arc<RwLock<Vec<Space>>>, mut stream: TcpStream) {
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
                let spaces = Arc::clone(&spaces);
                let write_size = Self::handle_message(spaces, &read_buffer[..size], &mut write_buffer);
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
    fn handle_message(spaces: Arc<RwLock<Vec<Space>>>, message: &[u8], buf: &mut [u8]) -> usize {
        return match message[0] {
            actions::PING => {
                buf[0] = actions::PING;
                1
            },
            actions::CREATE_SPACE => {
                let mut spaces = spaces.write().unwrap();
                spaces.push(
                    Space::new(SpaceEngineType::Cache, 256)
                );
                let l = spaces.len() - 1;
                buf[0..2].copy_from_slice(&[actions::DONE, l as u8, ((l as u16) >> 8) as u8]);
                3
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