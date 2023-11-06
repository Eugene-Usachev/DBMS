use std::{env};

pub struct Config {
    pub(crate) tcp_port: u16,
    pub(crate) unix_port: u16,
    pub(crate) password: String,
}

impl Config {
    pub(crate) fn new() -> Self {
        let tcp_port = match env::var("TCP_PORT") {
            Ok(value) => {
                println!("The port was set to: {} using the environment variable \"TCP_PORT\"", value);
                value.parse().unwrap_or(10000)
            },
            Err(_) => {
                println!("The port was not set using the environment variable \"TCP_PORT\", setting it to 10000");
                10000
            }
        };

        let unix_port = match env::var("UNIX_PORT") {
            Ok(value) => {
                println!("The port was set to: {} using the environment variable \"UNIX_PORT\"", value);
                value.parse().unwrap_or(10002)
            },
            Err(_) => {
                println!("The port was not set using the environment variable \"UNIX_PORT\", setting it to 10002");
                10002
            }
        };

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

        Self { tcp_port, password, unix_port }
    }
}
