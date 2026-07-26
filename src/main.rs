// src/main.rs

use promisedb::domain::Interval;

fn main() {
    let interval = match Interval::new(10, 20) {
        Ok(interval) => interval,
        Err(_) => {
            println!("Invalid interval");
            return;
        }
    };

    println!("{interval:?}");
}
