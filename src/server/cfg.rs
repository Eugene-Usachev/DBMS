use std::{env};

pub struct Config {
    pub tcp_addr: String,
    //pub unix_addr: String,
    pub password: String,
    pub node_addr: String,
}

impl Config {
    pub(crate) fn new() -> Self {
        match env::var("DUMP_INTERVAL") {
            Ok(value) => {
                println!("The dump interval was set to: {} minutes using the environment variable \"DUMP_INTERVAL\"", value);
            },
            Err(_) => {
                println!("The dump interval was not set using the environment variable \"DUMP_INTERVAL\", setting it to 24 hours");
            }
        };

        let tcp_addr = match env::var("TCP_ADDR") {
            Ok(value) => {
                println!("The address was set to: {} using the environment variable \"TCP_ADDR\"", value);
                value.parse().unwrap_or("localhost:10000".to_string())
            },
            Err(_) => {
                println!("The address was not set using the environment variable \"TCP_ADDR\", setting it to \"localhost:10000\"");
                "localhost:10000".to_string()
            }
        };

        // let unix_addr = match env::var("UNIX_ADDR") {
        //     Ok(value) => {
        //         println!("The address was set to: {} using the environment variable \"UNIX_ADDR\"", value);
        //         value.parse().unwrap_or("localhost:10002".to_string())
        //     },
        //     Err(_) => {
        //         println!("The address was not set using the environment variable \"UNIX_PORT\", setting it to \"localhost:10002\"");
        //         "localhost:10002".to_string()
        //     }
        // };

        let password = match env::var("PASSWORD") {
            Ok(value) => {
                println!("The password was set to: {} using the environment variable \"PASSWORD\"", value);
                value
            },
            Err(_) => {
                println!("The password was not set using the environment variable \"PASSWORD\", setting it to \"\" (empty string)");
                String::new()
            }
        };

        let node_addr = match env::var("NODE_ADDR") {
            Ok(value) => {
                println!("The node address was set to: {} using the environment variable \"NODE_ADDR\"", value);
                value
            },
            Err(_) => {
                println!("The node address was not set using the environment variable \"NODE_ADDR\". Single mode is used.");
                String::new()
            }
        };

        Self { tcp_addr, password, node_addr }
    }
}
