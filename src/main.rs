use promisedb::clock::{Clock, SystemClock};
use promisedb::domain::{Bundle, Claim, DomainError, Interval, PromiseId, Timestamp};
use promisedb::engine::Engine;
use std::error::Error;
use std::io;

fn timestamp_after(timestamp: Timestamp, seconds: Timestamp) -> Result<Timestamp, io::Error> {
    timestamp
        .checked_add(seconds)
        .ok_or_else(|| io::Error::other("timestamp overflow"))
}

fn print_promise(
    engine: &Engine,
    promise_id: PromiseId,
    transition: &str,
) -> Result<(), DomainError> {
    let promise = engine
        .promise(promise_id)
        .ok_or(DomainError::PromiseNotFound)?;

    println!(
        "{transition}: state={:?}, version={}, sequence={}",
        promise.state(),
        promise.version().get(),
        engine.sequence().get()
    );

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut engine = Engine::new();
    let now = SystemClock.now()?;

    let pool_id = engine.create_resource_pool("Assembly machines".into(), "machines".into(), 2)?;
    println!(
        "resource pool created: id={pool_id:?}, sequence={}",
        engine.sequence().get()
    );

    let usage_start = timestamp_after(now, 3_600)?;
    let usage_end = timestamp_after(now, 7_200)?;
    let hold_deadline = timestamp_after(now, 300)?;

    let interval = Interval::new(usage_start, usage_end)?;
    let claim = Claim::new(pool_id, interval, 1)?;
    let bundle = Bundle::new(vec![claim])?;

    let promise_id = engine.hold(bundle, hold_deadline)?;
    print_promise(&engine, promise_id, "hold")?;

    let held_version = engine
        .promise(promise_id)
        .ok_or(DomainError::PromiseNotFound)?
        .version();
    engine.commit(promise_id, held_version)?;
    print_promise(&engine, promise_id, "commit")?;

    let committed_version = engine
        .promise(promise_id)
        .ok_or(DomainError::PromiseNotFound)?
        .version();
    engine.release(promise_id, committed_version)?;
    print_promise(&engine, promise_id, "release")?;

    Ok(())
}
