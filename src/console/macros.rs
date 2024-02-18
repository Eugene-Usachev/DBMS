#[macro_export]
macro_rules! info {
    ($msg:expr) => {
        let blue = colored::Colorize::blue($msg);
        println!("{}", blue);
    };
    ($msg:expr, $($arg:expr),*) => {
        let blue = colored::Colorize::blue(format!($msg, $($arg),*).as_str());
        println!("{blue}");
    };
}

#[macro_export]
macro_rules! warn {
    ($msg:expr) => {
        let yellow = colored::Colorize::yellow($msg);
        println!("{}", yellow);
    };
    ($msg:expr, $($arg:expr),*) => {
        let yellow = colored::Colorize::yellow(format!($msg, $($arg),*).as_str());
        println!("{yellow}");
    };
}

#[macro_export]
macro_rules! success {
    ($msg:expr) => {
        let green = colored::Colorize::bright_green($msg);
        println!("{}", green);
    };
    ($msg:expr, $($arg:expr),*) => {
        let green = colored::Colorize::bright_green(format!($msg, $($arg),*).as_str());
        println!("{green}");
    };
}

#[macro_export]
macro_rules! error {
    ($msg:expr) => {
        let red = colored::Colorize::red($msg);
        println!("{}", red);
    };
    ($msg:expr, $($arg:expr),*) => {
        let red = colored::Colorize::red(format!($msg, $($arg),*).as_str());
        println!("{red}");
    };
}