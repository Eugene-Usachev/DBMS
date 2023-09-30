use std::io;
use std::io::{BufRead, Read, Write};

#[inline(always)]
pub fn input<T: std::str::FromStr>(text: &str, default: T) -> T {
    let mut input = String::new();
    print!("{}", text);
    io::stdout().flush().unwrap();
    let stdin = io::stdin();
    //let mut lock = stdin.lock();
    stdin.read_line(&mut input).unwrap();
    return input.trim().parse().unwrap_or(default);
}