use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use structopt::StructOpt;
use tokio::sync::Mutex;
use warp::{hyper::StatusCode, reply::with_status, Filter};

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opts {
    #[structopt(short, long, default_value = "8080")]
    port: u16,

    #[structopt(long, default_value = "60")]
    default_timeout: u16,
    #[structopt(long, default_value = "300")]
    max_timeout: u16,

    #[structopt(long, default_value = "100")]
    max_key_len: usize,
}

#[derive(Serialize, Deserialize, Default)]
struct LockJobQueryParams {
    timeout: Option<u16>,
}

#[tokio::main]
async fn main() {
    let opts = Opts::from_args();
    let default_timeout = opts.default_timeout;
    let max_timeout = opts.max_timeout;
    let max_key_len = opts.max_key_len;

    let table = Arc::new(Mutex::new(HashMap::new()));
    let kv = table.clone();
    let server = warp::path!("lock-job" / String)
        .and(warp::put())
        .and(
            warp::filters::query::query::<LockJobQueryParams>()
                .or(warp::any().map(LockJobQueryParams::default))
                .unify(),
        )
        .then(move |key: String, query: LockJobQueryParams| {
            let kv = kv.clone();
            async move {
                let len = key.len();
                if len > max_key_len {
                    let msg = format!("key too long! max: {max_key_len}, found: {len}");
                    return with_status(msg, StatusCode::from_u16(400).unwrap());
                }

                let timeout_s = query.timeout.unwrap_or(default_timeout).min(max_timeout);
                let mut kv = kv.lock().await;
                if let Entry::Vacant(v) = kv.entry(key) {
                    let t = Instant::now() + Duration::from_secs(timeout_s as u64);
                    v.insert(t);
                    with_status("".to_owned(), StatusCode::from_u16(201).unwrap())
                } else {
                    with_status("".to_owned(), StatusCode::from_u16(200).unwrap())
                }
            }
        });

    let kv = table.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;

            let mut kv = kv.lock().await;
            let now = Instant::now();
            kv.retain(|_, t| *t > now);
            drop(kv);
        }
    });

    warp::serve(server).run(([0, 0, 0, 0], opts.port)).await;
}
