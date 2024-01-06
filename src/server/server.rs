use std::intrinsics::{likely, unlikely};
use std::io::Write;
use std::net::{TcpListener};
#[cfg(not(target_os = "windows"))]
use std::os::unix::net::UnixListener;
use std::sync::{Arc};
use std::thread;

use crate::constants::actions;
use crate::constants::size::{READ_BUFFER_SIZE, READ_BUFFER_SIZE_WITHOUT_SIZE, WRITE_BUFFER_SIZE};
use crate::server::cfg::Config;
use crate::storage::storage::Storage;
use crate::utils::fastbytes::uint;

use crate::server::reactions::status::{ping};
use crate::server::reactions::table::{create_table_cache, create_table_in_memory, create_table_on_disk, get_tables_names};
use crate::server::reactions::work_with_tables::{delete, get, get_and_reset_cache_time, insert, set};
use crate::server::stream_trait::Stream;

pub struct Server {
    storage: Arc<Storage>,
    is_running: bool,
    password: String,
    tcp_port: u16,
    unix_port: u16,
}

impl Server {
    pub fn new(storage: Arc<Storage>) -> Self {
        let config = Config::new();
        Self {
            storage: storage.clone(),
            tcp_port: config.tcp_port,
            unix_port: config.unix_port,
            password: config.password,
            is_running: false,
        }
    }

    pub(crate) fn run(&mut self) {
        if self.is_running {
            return;
        }
        self.is_running = true;
        #[cfg(not(target_os = "windows"))] {
            let storage = self.storage.clone();
            let unix_port = self.unix_port;
            thread::spawn(move || {
                let listener_ = UnixListener::bind(format!("dash_dbms:{}", unix_port));
                let listener = match listener_ {
                    Ok(listener) => listener,
                    Err(e) => {
                        panic!("Can't bind to port: {}, the error is: {:?}", unix_port, e);
                    }
                };
                println!("Server tcp listening on port {}", unix_port);
                for stream in listener.incoming() {
                    let storage = storage.clone();
                    thread::spawn(move || {
                        match stream {
                            Ok(mut stream) => {
                                Self::handle_client(storage, &mut stream);
                            }
                            Err(e) => {
                                println!("Error: {}", e);
                            }
                        }
                    });
                }
            });
        }
        let listener_ = TcpListener::bind(format!("dash_dbms:{}", self.tcp_port));
        let listener = match listener_ {
            Ok(listener) => listener,
            Err(e) => {
                panic!("Can't bind to port: {}, the error is: {:?}", self.tcp_port, e);
            }
        };
        println!("Server tcp listening on port {}", self.tcp_port);
        for stream in listener.incoming() {
            let storage= self.storage.clone();
            thread::spawn(move || {
                match stream {
                    Ok(mut stream) => {
                        Self::handle_client(storage, &mut stream);
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            });
        }
    }

    #[inline(always)]
    fn handle_client(storage: Arc<Storage>, stream: &mut impl Stream) {
        let mut read_buffer = [0u8; READ_BUFFER_SIZE];
        let mut write_buffer = [0u8; WRITE_BUFFER_SIZE];
        let mut log_buffer = [0u8; WRITE_BUFFER_SIZE];
        let mut log_buffer_offset = 0usize;

        while match Stream::read(stream, &mut read_buffer) {
            Ok(0) => false,
            Ok(mut pipe_size) => {
                let real_pipe_size = uint::u32(&read_buffer[0..4]);
                while real_pipe_size != pipe_size as u32 {
                    match Stream::read(stream, &mut read_buffer[pipe_size..]) {
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
                    if unlikely(offset >= pipe_size - 2) {
                        break;
                    }
                    let size:u16 = uint::u16(&read_buffer[offset..offset+2]);
                    if unlikely(size == 65535) {
                        let big_size:u32 = uint::u32(&read_buffer[offset+2..offset+6]);
                        offset += 6;
                        write_offset = Self::handle_message(stream, storage.clone(), &read_buffer[offset..offset + big_size as usize],
                                                            &mut write_buffer, write_offset, &mut log_buffer, &mut log_buffer_offset);
                        offset += big_size as usize;
                    } else {
                        offset += 2;
                        write_offset = Self::handle_message(stream, storage.clone(), &read_buffer[offset..offset + size as usize], 
                                                            &mut write_buffer, write_offset, &mut log_buffer, &mut log_buffer_offset);
                        offset += size as usize;
                    }
                }
                if log_buffer_offset > 0 {
                    storage.log_file.file.lock().unwrap().write(&log_buffer[..log_buffer_offset]).expect("Can't write to log file");
                    log_buffer_offset = 0;
                }
                Stream::write_all(stream, &write_buffer[..write_offset]).expect("Can't write to stream");
                true
            },
            Err(e) => {
                println!("An error occurred, error has a message: {:?}", e);
                stream.shutdown().unwrap();
                false
            }
        } {};
    }

    #[inline(always)]
    fn handle_message(stream: &mut impl Stream, storage: Arc<Storage>, message: &[u8], write_buf: &mut [u8], write_offset: usize, log_buf: &mut [u8], log_offset: &mut usize) -> usize {
        return match message[0] {
            actions::PING => ping(stream, write_buf, write_offset),

            actions::CREATE_TABLE_IN_MEMORY => create_table_in_memory(stream, storage, message, write_buf, write_offset, log_buf, log_offset),
            actions::CREATE_TABLE_CACHE => create_table_cache(stream, storage, message, write_buf, write_offset, log_buf, log_offset),
            actions::CREATE_TABLE_ON_DISK => create_table_on_disk(stream, storage, message, write_buf, write_offset, log_buf, log_offset),
            actions::GET_TABLES_NAMES => get_tables_names(stream, storage, write_buf, write_offset),

            actions::GET => get(stream, storage, message, write_buf, write_offset),
            actions::GET_AND_RESET_CACHE_TIME => get_and_reset_cache_time(stream, storage, message, write_buf, write_offset),
            actions::INSERT => insert(stream, storage, message, write_buf, write_offset, log_buf, log_offset),
            actions::SET => set(stream, storage, message, write_buf, write_offset, log_buf, log_offset),
            actions::DELETE => delete(stream, storage, message, write_buf, write_offset, log_buf, log_offset),
            _ => {
                write_msg(stream, write_buf, write_offset, &[actions::BAD_REQUEST])
            }
        }
    }
}

#[inline(always)]
pub(crate) fn write_msg(stream: &mut impl Stream, buf: &mut [u8], mut offset: usize, msg: &[u8]) -> usize {
    let l = msg.len();

    if unlikely(l + offset > READ_BUFFER_SIZE_WITHOUT_SIZE) {
        stream.write_all(&buf).expect("Can't write to stream");
        offset = 0; // We flushed the buffer. Now we need to start from the beginning, but we still are responding for the same pipe.
    }

    // 65535 is 2 << 16 - 1
    if likely(l < 65535) {
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
        buf[offset..offset+can_write].copy_from_slice(&msg[written..written + can_write]);
        written += can_write;
        stream.write_all(&buf).expect("Can't write to stream");
        offset = 0;
        can_write = READ_BUFFER_SIZE;
    }

    buf[offset..offset+l].copy_from_slice(msg);
    return offset + l;
}

#[inline(always)]
pub(crate) fn write_msg_with_status_separate(stream: &mut impl Stream, buf: &mut [u8], mut offset: usize, status: u8, msg: &[u8]) -> usize {
    let l = msg.len() + 1;

    if unlikely(l + offset > READ_BUFFER_SIZE_WITHOUT_SIZE) {
        stream.write_all(&buf).expect("Can't write to stream");
        offset = 0; // We flushed the buffer. Now we need to start from the beginning, but we still are responding for the same pipe.
    }

    // 65535 is 2 << 16 - 1
    if likely(l < 65535) {
        buf[offset..offset+2].copy_from_slice(&[l as u8, ((l >> 8) as u8)]);
        offset += 2;
    } else {
        buf[offset..offset+6].copy_from_slice(&[255, 255, l as u8, ((l >> 8) as u8), ((l >> 16) as u8), ((l >> 24) as u8)]);
        offset += 6;
    }

    buf[offset] = status;
    offset += 1;

    // We try to write all the message. If l > allowed size, we write a lot of times.
    let mut can_write = READ_BUFFER_SIZE - offset;
    let mut written = 0;
    while l > can_write {
        buf[offset..offset+can_write].copy_from_slice(&msg[written..written + can_write]);
        written += can_write;
        stream.write_all(&buf).expect("Can't write to stream");
        offset = 0;
        can_write = READ_BUFFER_SIZE;
    }

    buf[offset..offset+l - 1].copy_from_slice(msg);
    return offset + l - 1;
}