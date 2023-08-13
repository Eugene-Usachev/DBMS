use std::{env, io};
use crate::console::input::input;
use std::io::{BufRead, Write};

pub struct Config {
    pub(crate) port: u16,
    pub(crate) password: String,
}

impl Config {
    pub(crate) fn new() -> Self {
        let port = match env::var("PORT") {
            Ok(value) => {
                println!("The port was set to: {} using the environment variable \"PORT\"", value);
                value.parse().unwrap_or(10000)
            },
            Err(_) => {
                input("Please enter the port (default: 10000): ", 10000)
            }
        };

        let password = match env::var("PASSWORD") {
            Ok(value) => {
                println!("The password was set to: {} using the environment variable \"PASSWORD\"", value);
                value
            },
            Err(_) => {
                input("Please enter the password (all machines in the cluster must have the same password): ", "".to_string())
            }
        };



        Self { port, password }
    }
}
