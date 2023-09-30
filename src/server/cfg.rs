use std::{env};

use crate::console::input::input;

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
                println!("The port was not set using the environment variable \"PORT\", setting it to 10000");
                10000
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



        Self { port, password }
    }
}
