#[allow(dead_code)]
pub(crate) fn start_message() {
    let welcome_message = format!("
    ------------------------------------------------------------------
    |{: ^64}|
    |{: ^64}|
    |{: ^64}|
    ------------------------------------------------------------------
    ", "Welcome to the nimble db!",
       "Version: 0.1.0",
       "Complete the setup, it will only take a couple of seconds",
    );

    println!("{}", welcome_message);
}