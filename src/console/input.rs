use std::io;
use std::io::{BufRead, Write};

#[inline(always)]
pub fn input<T: std::str::FromStr>(text: &str, default: T) -> T {
    let mut input = String::new();
    print!("{}", text);
    io::stdout().flush().unwrap();
    io::stdin().lock().read_line(&mut input).unwrap();
    return input.trim().parse().unwrap_or(default);
}