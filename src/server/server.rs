use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::net::{TcpListener};
#[cfg(not(target_os = "windows"))]
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::{Arc};
use std::thread;
use crate::connection::{BufConnection, Status};

use crate::constants::actions;
use crate::constants::actions::DONE;
use crate::{error, success, warn};
use crate::node::Node;
use crate::server::cfg::Config;
use crate::storage::storage::Storage;

use crate::server::reactions::status::{get_hierarchy, get_shard_metadata, ping};
use crate::server::reactions::table::{create_table_cache, create_table_in_memory, create_table_on_disk, get_tables_names};
use crate::server::reactions::work_with_tables::{delete, get, get_field, get_fields, insert, set};
use crate::stream::Stream;
use crate::utils::fastbytes::uint;
use crate::utils::read_more;
use crate::writers::LogWriter;

pub struct Server {
    storage: &'static Storage,
    is_running: bool,

    password: String,
    tcp_addr: String,
    unix_addr: String,
    node_addr: String,

    pub hierarchy: Vec<Vec<String>>,
    hierarchy_file_path: PathBuf,

    // Shard metadata is array with 65536 length, where every item is 16-bit number of node, that contains this shard.
    pub shard_metadata_file_path: PathBuf,

    node: Node
}

impl Server {
    pub fn new(storage: &'static Storage) -> Self {
        let config = Config::new();
        let hierarchy_file_path: PathBuf = storage.persistence_dir_path.join("hierarchy.bin");
        let shard_metadata_file_path: PathBuf = storage.persistence_dir_path.join("shard metadata.bin");

        let mut server = Self {
            storage: storage.clone(),
            tcp_addr: config.tcp_addr,
            unix_addr: config.unix_addr,
            node_addr: config.node_addr,
            password: config.password,
            is_running: false,
            hierarchy: Vec::with_capacity(0),
            hierarchy_file_path,
            shard_metadata_file_path,
            node: Node::new()
        };

        server.rise_hierarchy_and_lookup_node();

        server.set_up_shard_metadata_file();

        server.connect_to_node();

        server
    }

    fn rise_hierarchy_and_lookup_node(&mut self) {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(&self.hierarchy_file_path);
        let mut hierarchy;
        if file.is_err() {
            panic!("Can't open hierarchy file {}", self.hierarchy_file_path.display());
        } else {
            let mut file = file.unwrap();
            if file.metadata().unwrap().len() < 3 {
                hierarchy = vec![vec![self.tcp_addr.clone()]];
                let l = self.tcp_addr.len();
                file.seek(SeekFrom::Start(0)).expect("Can't seek hierarchy file");
                // Format is: 1 byte for count of machines in the node and next 2 bytes for a name length and name.
                Stream::write_all(&mut file, &[1, l as u8, (l >> 8) as u8]).expect("Can't write to hierarchy file");
                Stream::write_all(&mut file, hierarchy[0][0].as_bytes()).expect("Can't write to hierarchy file");
            } else {
                let mut l;
                let mut read;
                let mut offset;
                let mut number_of_machines = 0;
                let mut buf = vec![0u8; 4096];
                let mut offset_last_record = 0;
                let mut node: Vec<String> = vec![];
                let mut this_node = false;

                hierarchy = Vec::with_capacity(1);
                'read: loop {
                    read = file.read(&mut buf).expect("Can't read from hierarchy file");
                    if read == 0 {
                        break;
                    }

                    offset = 0;
                    read += offset_last_record;

                    loop {
                        if number_of_machines == 0 {
                            if read < 1 + offset {
                                read_more(&mut buf, offset, read, &mut offset_last_record);
                                continue 'read;
                            }
                            number_of_machines = buf[offset];
                            offset += 1;
                            if node.len() > 0 {
                                if this_node {
                                    this_node = false;
                                    let mut list = Vec::with_capacity(node.len() - 1);
                                    for addr in &node {
                                        if addr != &self.tcp_addr {
                                            list.push(addr.to_string());
                                        }
                                    }
                                    self.node.set(&list);
                                }
                                hierarchy.push(node);
                            }
                            node = Vec::with_capacity(number_of_machines as usize);
                        }

                        if read < 2 + offset {
                            read_more(&mut buf, offset, read, &mut offset_last_record);
                            continue 'read;
                        }
                        l = buf[offset] as usize + (buf[offset + 1] as usize) << 8;
                        offset += 2;
                        if read < l + offset {
                            read_more(&mut buf, offset, read, &mut offset_last_record);
                            continue 'read;
                        }
                        let name = String::from_utf8_lossy(&buf[offset..l + offset]).to_string();
                        if self.tcp_addr == name {
                            this_node = true;
                        }
                        node.push(name);
                        offset += l;
                        number_of_machines -= 1;
                    }
                }
                if node.len() == 0 {
                    error!("Incorrect hierarchy file!");
                    self.hierarchy = vec![vec![self.tcp_addr.clone()]];
                    return;
                }
                hierarchy.push(node);
            }
        }

        self.hierarchy = hierarchy
    }
    
    fn set_up_shard_metadata_file(&mut self) {
        // We split all data in shards. We have 65,536 shards, and we distribute shards into different nodes.
        // We store shard metadata as [`number of machine addresses`, [`address length`, `machine address`]; 65,536].
        // We always think that the leftmost alive machine is the master.

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(&self.shard_metadata_file_path);
        if file.is_err() {
            panic!("Can't open shard metadata file {}", self.shard_metadata_file_path.display());
        }
        
        let mut file = file.unwrap();
        if file.metadata().unwrap().len() == 0 {
            if self.hierarchy.len() == 1 {
                let mut buf = Vec::with_capacity(65536 * 2);
                for _i in 0..65_536 {
                    buf.extend_from_slice(&uint::u16tob(0));
                }

                Stream::write_all(&mut file, &buf).expect("Can't write to shard metadata file");
            } else {
                // TODO: set up with cluster
            }
        } else {
            // TODO: set up with cluster
        }
    }

    fn connect_to_node(&mut self) {

    }

    fn connect_to_cluster(&mut self) {

    }

    pub fn run(mut self) {
        if self.is_running {
            return;
        }
        self.is_running = true;

        self.connect_to_cluster();

        let server = Arc::new(self);

        #[cfg(not(target_os = "windows"))] {
            let storage = server.storage.clone();
            let unix_port = server.unix_addr.clone();
            let server = server.clone();
            thread::spawn(move || {
                let listener_ = UnixListener::bind(format!("{}", unix_port));
                let listener = match listener_ {
                    Ok(listener) => listener,
                    Err(e) => {
                        panic!("Can't bind to address: {}, the error is: {:?}", unix_port, e);
                    }
                };
                success!("Server unix listening on address {}", unix_port);
                for stream in listener.incoming() {
                    let storage = storage.clone();
                    let server = server.clone();
                    thread::spawn(move || {
                        match stream {
                            Ok(stream) => {
                                Self::handle_client(server, storage, BufConnection::new(stream));
                            }
                            Err(e) => {
                                error!("Error: {}", e);
                            }
                        }
                    });
                }
            });
        }
        let listener_ = TcpListener::bind(format!("{}", server.tcp_addr.clone()));
        let listener = match listener_ {
            Ok(listener) => listener,
            Err(e) => {
                panic!("Can't bind to address: {}, the error is: {:?}", server.tcp_addr.clone(), e);
            }
        };
        success!("Server tcp listening on address {}", server.tcp_addr.clone());
        for stream in listener.incoming() {
            let server = server.clone();
            let storage= server.storage.clone();
            thread::spawn(move || {
                match stream {
                    Ok(stream) => {
                        Self::handle_client(server, storage, BufConnection::new(stream));
                    }
                    Err(e) => {
                        error!("Error: {}", e);
                    }
                }
            });
        }
    }

    #[inline(always)]
    fn handle_client<S: Stream>(server: Arc<Server>, storage: &'static Storage, mut connection: BufConnection<S>) {
        let mut status;
        let mut is_reading;
        if server.password.len() > 0 {
            let mut buf = vec![0;server.password.len()];
            let reader = &mut connection.reader;
            reader.reader.read_exact(&mut buf).expect("Failed to read password");
            if buf != server.password.as_bytes() {
                warn!("Wrong password. Disconnected.");
                connection.close().expect("Failed to close connection");
                return;
            }
            connection.writer.write_all(&[DONE]).expect("Failed to write DONE");
            connection.writer.flush().expect("Failed to flush connection");
        }
        success!("Connection accepted");

        let mut log_writer = LogWriter::new(storage.log_file.clone());

        let mut message;
        loop {
            (status, is_reading) = connection.read_request();
            if status != Status::Ok {
                connection.close().expect("Failed to close connection");
                return;
            }

            loop {
                (message, status) = connection.read_message();
                if status != Status::Ok {
                    if status == Status::All {
                        log_writer.flush();
                        connection.flush().expect("Failed to flush connection");
                        break;
                    }
                    connection.close().expect("Failed to close connection");
                    return;
                }

                status = Self::handle_message(&mut connection, &server, storage, message, &mut log_writer);
                if status != Status::Ok {
                    connection.close().expect("Failed to close connection");
                    return;
                }
            }
        }
    }

    #[inline(always)]
    fn handle_message<S: Stream>(connection: &mut BufConnection<S>, server: &Arc<Server>, storage: &'static Storage, message: &[u8], log_writer: &mut LogWriter) -> Status {
        return match message[0] {
            actions::PING => ping(connection),
            actions::GET_SHARD_METADATA => get_shard_metadata(connection, server),
            actions::GET_HIERARCHY => get_hierarchy(connection, server),

            actions::CREATE_TABLE_IN_MEMORY => create_table_in_memory(connection, storage, message, log_writer),
            actions::CREATE_TABLE_CACHE => create_table_cache(connection, storage, message, log_writer),
            actions::CREATE_TABLE_ON_DISK => create_table_on_disk(connection, storage, message, log_writer),
            actions::GET_TABLES_NAMES => get_tables_names(connection, storage),

            actions::GET => get(connection, storage, message),
            actions::GET_FIELD => get_field(connection, storage, message),
            actions::GET_FIELDS => get_fields(connection, storage, message),

            actions::INSERT => insert(connection, storage, message, log_writer),
            actions::SET => set(connection, storage, message, log_writer),
            actions::DELETE => delete(connection, storage, message, log_writer),
            _ => {
                connection.write_message(&[actions::BAD_REQUEST])
            }
        }
    }
}