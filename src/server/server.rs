use std::borrow::{Borrow, BorrowMut};
use std::fmt::Error;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use std::thread;

use crate::server::cfg::Config;
use crate::constants::actions;
use crate::space::space::{Space, SpaceEngineType, SpaceInterface};
use crate::console::write_errors;
use crate::utils;

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
        let listener_ = TcpListener::bind(format!("127.0.0.1:{}", self.port));
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
                        Self::handle_client(spaces, stream);
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
        let mut read_buffer = [0u8; MB];
        let mut write_buffer = [0u8; 256 * KB];
        let mut write_offset = 0;
        while match stream.read(&mut read_buffer) {
            Ok(0) => false,
            Ok(size) => {
                let mut offset = 0;
                loop {
                    let spaces = Arc::clone(&spaces);
                    if offset == size {
                        break;
                    }
                    let size:u16 = utils::fastbytes::uint::u16(&read_buffer[offset..offset+2]);
                    if size == 65535 {
                        let big_size:u32 = utils::fastbytes::uint::u32(&read_buffer[offset+2..offset+6]);
                        offset += 6 + big_size as usize;
                        write_offset = Self::handle_message(spaces, &read_buffer[offset..offset + big_size as usize], &mut write_buffer, write_offset);
                    } else {
                        offset += 2 + size as usize;
                        write_offset = Self::handle_message(spaces, &read_buffer[offset..size as usize], &mut write_buffer, write_offset);
                    }
                }
                true
            },
            Err(_) => {
                println!("An error occurred, terminating connection with {}", stream.peer_addr().unwrap());
                stream.shutdown(std::net::Shutdown::Both).unwrap();
                false
            }
        } {};
    }

    #[inline(always)]
    fn handle_message(spaces: Arc<RwLock<Vec<Space>>>, message: &[u8], write_buffer: &mut [u8], offset: usize) -> usize {
        return match message[0] {
            actions::PING => {
                write_buffer[offset..offset+3].copy_from_slice(&[0u8, 1u8, actions::PING]);
                3
            },
            actions::CREATE_SPACE => {
                let i = utils::fastbytes::uint::u32(message);
                write_msg(write_buffer, offset, &utils::fastbytes::uint::u32tob(i))
            },
            actions::INSERT => {
                2
            },
            _ => {
                write_buffer[offset..offset+3].copy_from_slice(&[0u8, 0u8]);
                2
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

const KB: usize = 1024;
const MB: usize = 1024 * 1024;