use std::error::Error;
use std::io::{SeekFrom, };
use std::path::PathBuf;
use std::sync::{Arc};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use crate::connection::{BufConnection, Status, SyncBufConnection};
use crate::constants::actions;

use crate::constants::actions::{BAD_REQUEST, DONE};
use crate::constants::paths::PERSISTENCE_DIR;
use crate::error::CustomError;
use crate::node::Node;
use crate::server::cfg::Config;
use crate::shard::{Manager, reactions};
use crate::utils::fastbytes::uint;

pub struct Server {
    shard_manager: Manager,
    is_running: bool,

    password: String,
    tcp_addr: String,
    //unix_addr: String,
    node_addr: String,

    pub hierarchy: Vec<Vec<String>>,
    hierarchy_file_path: PathBuf,

    // Shard metadata is array with 65536 length, where every item is 16-bit number of node, that contains this shard.
    pub shard_metadata_file_path: PathBuf,

    node: Node
}

#[inline(always)]
fn read_more(chunk: &mut [u8], start_offset: usize, bytes_read: usize, offset_last_record: &mut usize) {
    let slice_to_copy = &mut Vec::with_capacity(0);
    chunk[start_offset..bytes_read].clone_into(slice_to_copy);
    *offset_last_record = bytes_read - start_offset;
    chunk[0..*offset_last_record].copy_from_slice(slice_to_copy);
}

impl Server {
    pub async fn new(manager: Manager) -> Server {
        let config = Config::new();

        let hierarchy_file_path: PathBuf = ["..", PERSISTENCE_DIR, "hierarchy.bin"].iter().collect();
        
        let shard_metadata_file_path: PathBuf = ["..", PERSISTENCE_DIR, "shard metadata.bin"].iter().collect();

        let mut server = Self {
            shard_manager: manager,
            tcp_addr: config.tcp_addr,
            //unix_addr: config.unix_addr,
            node_addr: config.node_addr,
            password: config.password,
            is_running: false,
            hierarchy: Vec::with_capacity(0),
            hierarchy_file_path,
            shard_metadata_file_path,
            node: Node::new()
        };

        server.rise_hierarchy_and_lookup_node().await;

        server.set_up_shard_metadata_file().await;

        server.connect_to_node().await;

        server
    }

    async fn rise_hierarchy_and_lookup_node(&mut self) {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(&self.hierarchy_file_path).await;
        let mut hierarchy;
        if file.is_err() {
            panic!("Can't open hierarchy file {}", self.hierarchy_file_path.display());
        } else {
            let mut file = file.unwrap();
            if file.metadata().await.unwrap().len() < 3 {
                hierarchy = vec![vec![self.tcp_addr.clone()]];
                let l = self.tcp_addr.len();
                file.seek(SeekFrom::Start(0)).await.expect("Can't seek hierarchy file");
                // Format is: 1 byte for count of machines in the node and next 2 bytes for a name length and name.
                file.write_all(&[1, l as u8, (l >> 8) as u8]).await.expect("Can't write to hierarchy file");
                file.write_all(hierarchy[0][0].as_bytes()).await.expect("Can't write to hierarchy file");
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
                    read = file.read(&mut buf).await.expect("Can't read from hierarchy file");
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
                    println!("Incorrect hierarchy file!");
                    self.hierarchy = vec![vec![self.tcp_addr.clone()]];
                    return;
                }
                hierarchy.push(node);
            }
        }

        self.hierarchy = hierarchy
    }
    
    async fn set_up_shard_metadata_file(&mut self) {
        // We split all data in shards. We have 65,536 shards, and we distribute shards into different nodes.
        // We store shard metadata as [`number of machine addresses`, [`address length`, `machine address`]; 65,536].
        // We always think that the leftmost alive machine is the master.

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(&self.shard_metadata_file_path).await;
        if file.is_err() {
            panic!("Can't open shard metadata file {}", self.shard_metadata_file_path.display());
        }
        
        let mut file = file.unwrap();
        if file.metadata().await.unwrap().len() == 0 {
            if self.hierarchy.len() == 1 {
                let mut buf = Vec::with_capacity(65536 * 2);
                for _i in 0..65_536 {
                    buf.extend_from_slice(&uint::u16tob(0));
                }

                file.write_all(&buf).await.expect("Can't write to shard metadata file");
            } else {
                // TODO: set up with cluster
            }
        } else {
            // TODO: set up with cluster
        }
    }

    async fn connect_to_node(&mut self) {

    }

    async fn connect_to_cluster(&mut self) {

    }

    pub async fn run(manager: Manager) {
        let mut server = Self::new(manager).await;
        if server.is_running {
            return;
        }
        server.is_running = true;

        server.connect_to_cluster().await;

        let server = Arc::new(server);

        // #[cfg(not(target_os = "windows"))] {
        //     let storage = server.storage.clone();
        //     let unix_port = server.unix_addr.clone();
        //     let server = server.clone();
        //     thread::spawn(move || {
        //         let listener_ = UnixListener::bind(format!("{}", unix_port));
        //         let listener = match listener_ {
        //             Ok(listener) => listener,
        //             Err(e) => {
        //                 panic!("Can't bind to address: {}, the error is: {:?}", unix_port, e);
        //             }
        //         };
        //         println!("Server unix listening on address {}", unix_port);
        //         for stream in listener.incoming() {
        //             let storage = storage.clone();
        //             let server = server.clone();
        //             thread::spawn(move || {
        //                 match stream {
        //                     Ok(stream) => {
        //                         Self::handle_client(server, storage, BufConnection::new(stream));
        //                     }
        //                     Err(e) => {
        //                         println!("Error: {}", e);
        //                     }
        //                 }
        //             });
        //         }
        //     });
        // }
        let listener_ = TcpListener::bind(format!("{}", server.tcp_addr.clone())).await;
        let listener = match listener_ {
            Ok(listener) => listener,
            Err(e) => {
                panic!("Can't bind to address: {}, the error is: {:?}", server.tcp_addr.clone(), e);
            }
        };
        println!("Server tcp listening on address {}", server.tcp_addr.clone());
        loop {
            let res = listener.accept().await;
            if res.is_err() {
                println!("Can't accept TCP connection. Shutting down...");
                return;
            }
            let (stream, _socket_addr) = res.expect("Can't get TCP connection");
            let server_clone = Arc::clone(&server);

            tokio::spawn(async move {
                Self::handle_client(server_clone, stream).await;
            });
        }
    }

    #[inline(always)]
    async fn handle_client(server: Arc<Server>, stream: TcpStream) {
        // TODO: we always allocate 128 KB for BufConnection. But we will be Proxy.
        let conn = BufConnection::new(stream);
        let mut connection = SyncBufConnection::new(conn);
        if server.password.len() > 0 {
            let mut buf = vec![0;server.password.len()];
            let status = connection.get().read_exact(&mut buf).await;
            if status != Status::Ok {
                println!("Can't read password. Disconnected.");
                connection.get().close().expect("Failed to close connection");
                return;
            }
            if buf != server.password.as_bytes() {
                println!("Wrong password. Disconnected.");
                connection.get().close().expect("Failed to close connection");
                return;
            }
            connection.writer().write_all(&[DONE]).await.expect("Failed to write DONE");
            connection.writer().flush().await.expect("Failed to flush connection");
        }
        println!("Connection accepted");

        let mut status;
        let mut shard_number;
        let mut is_ok;
        loop {
            (status, shard_number) = connection.read_request().await;
            if status != Status::Ok {
                println!("Can't read a request.");
                break;
            }

            if shard_number < u32::MAX as usize {
                // request for DB
                let (requests, response) = &server.shard_manager.connectors[shard_number];
                requests.send(connection).expect("Internal error. Can't send connection to a shard");
                (connection, is_ok) = response.recv().expect("Failed to recv");
                if !is_ok {
                    break;
                }
            } else {
                Self::handle_server_request(server.clone(), connection.get()).await.expect("Failed to handle server request");
            }
        }
    }

    async fn handle_server_request(server: Arc<Server>, connection: &mut BufConnection) -> Result<(), Box<dyn Error>> {
        let mut message;
        let mut status;
        loop {
            (message, status) = connection.read_message().await;
            if status != Status::Ok {
                if status == Status::All {
                    connection.flush().await.expect("Failed to flush connection");
                    break;
                }
                connection.close().expect("Failed to close connection");
                return Err(Box::new(CustomError::new("Bad request.")));
            }

            // TODO: log
            status = Self::handle_server_message(server.clone(), connection, message).await;
            if status != Status::Ok {
                connection.close().expect("Failed to close connection");
                return Err(Box::new(CustomError::new("Bad request.")));
            }
        }

        return Ok(());
    }

    async fn handle_server_message(server: Arc<Server>, connection: &mut BufConnection, message: &[u8]) -> Status {
        return match message[0] {
            actions::PING => reactions::status::ping(connection).await,
            actions::GET_SHARD_METADATA => reactions::status::get_shard_metadata(connection, &server).await,
            actions::GET_HIERARCHY => reactions::status::get_hierarchy(connection, &server).await,

            actions::CREATE_TABLE_IN_MEMORY => reactions::table::create_table_in_memory(connection, &server.shard_manager, message).await,
            actions::CREATE_TABLE_CACHE => reactions::table::create_table_cache(connection, &server.shard_manager, message).await,
            actions::CREATE_TABLE_ON_DISK => reactions::table::create_table_on_disk(connection, &server.shard_manager, message).await,
            actions::GET_TABLES_NAMES => reactions::table::get_tables_names(connection, &server.shard_manager).await,
            _ => {
                println!("Unknown action: {}.", message[0]);
                connection.write_message(&[BAD_REQUEST]).await
            }
        }
    }
}