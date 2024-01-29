use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use crate::connection::{BufConnection, Status};
use crate::constants::actions;
use crate::server::server::Server;
use crate::stream::Stream;

#[inline(always)]
pub fn ping<S: Stream>(connection: &mut BufConnection<S>) -> Status {
    connection.write_message(&[actions::DONE, actions::PING])
}

#[inline(always)]
pub fn get_shard_metadata<S: Stream>(connection: &mut BufConnection<S>, server: &Arc<Server>) -> Status {
    let ref path= server.shard_metadata_file_path;
    let mut file = File::open(path).expect("Can't open shard metadata file!");
    let mut buf = Vec::with_capacity(65536 * 2);
    file.read_to_end(&mut buf).expect("Can't read shard metadata file!");
    connection.write_message_and_status(&buf, actions::DONE)
}

#[inline(always)]
pub fn get_hierarchy<S: Stream>(connection: &mut BufConnection<S>, server: &Arc<Server>) -> Status {
    let ref hierarchy = server.hierarchy;
    // We think that average production machine address length is 20 and average node contains 3 machines.
    // Anyway get_hierarchy is rare action, so we are ready to be patient, even if it is slow.
    let mut buf = Vec::with_capacity(hierarchy.len() * 20 * 3);

    let mut machine_addr_len;
    for machines_addr in hierarchy.iter() {
        buf.extend_from_slice(&[machines_addr.len() as u8]);
        for machine_addr in machines_addr.iter() {
            machine_addr_len = machine_addr.len();
            buf.extend_from_slice(&[machine_addr_len as u8, (machine_addr_len >> 8) as u8]);
            buf.extend_from_slice(machine_addr.as_bytes());
        }
    }

    connection.write_message_and_status(&buf, actions::DONE)
}