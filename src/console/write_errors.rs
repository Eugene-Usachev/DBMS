pub fn can_not_write(error: std::io::Error) -> usize {
    println!("[error] Failed to write in the tcp stream! Error: {:}", error);
    0
}