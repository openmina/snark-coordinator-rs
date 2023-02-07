use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
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

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "kind")]
enum SnarkWorkerJobGetError {
    NoAvailableJob,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "kind")]
enum SnarkWorkerStatsPut {
    JobGetInit {
        time: u64,
    },
    JobGetError {
        time: u64,
        error: SnarkWorkerJobGetError,
    },
    JobGetSuccess {
        time: u64,
        ids: String,
    },
    WorkCreateError {
        time: u64,
        ids: String,
        error: String,
    },
    WorkCreateSuccess {
        time: u64,
        ids: String,
    },
    WorkSubmitError {
        time: u64,
        ids: String,
        error: String,
    },
    WorkSubmitSuccess {
        time: u64,
        ids: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "kind")]
enum SnarkWorkerState {
    JobGetPending {
        job_get_init_t: u64,
    },
    JobGetError {
        job_get_init_t: u64,
        job_get_error_t: u64,
        error: SnarkWorkerJobGetError,
    },
    WorkCreatePending {
        job_get_init_t: u64,
        job_get_success_t: u64,
        ids: String,
    },
    WorkCreateError {
        job_get_init_t: u64,
        job_get_success_t: u64,
        work_create_error_t: u64,
        ids: String,
        error: String,
    },
    WorkSubmitPending {
        job_get_init_t: u64,
        job_get_success_t: u64,
        work_create_success_t: u64,
        ids: String,
    },
    WorkSubmitError {
        job_get_init_t: u64,
        job_get_success_t: u64,
        work_create_success_t: u64,
        work_submit_error_t: u64,
        ids: String,
        error: String,
    },
    WorkSubmitSuccess {
        job_get_init_t: u64,
        job_get_success_t: u64,
        work_create_success_t: u64,
        work_submit_success_t: u64,
        ids: String,
    },
}

impl SnarkWorkerState {
    fn init(time: u64) -> Self {
        Self::JobGetPending {
            job_get_init_t: time,
        }
    }

    fn apply(&mut self, v: SnarkWorkerStatsPut) -> bool {
        match self.clone() {
            Self::JobGetPending { job_get_init_t } => {
                *self = match v {
                    SnarkWorkerStatsPut::JobGetError { time, error } => Self::JobGetError {
                        job_get_init_t,
                        job_get_error_t: time,
                        error,
                    },
                    SnarkWorkerStatsPut::JobGetSuccess { time, ids } => Self::WorkCreatePending {
                        job_get_init_t,
                        job_get_success_t: time,
                        ids,
                    },
                    _ => return false,
                }
            }
            Self::WorkCreatePending {
                job_get_init_t,
                job_get_success_t,
                ids: expected_ids,
            } => {
                *self = match v {
                    SnarkWorkerStatsPut::WorkCreateError {
                        time, error, ids, ..
                    } if ids == expected_ids => Self::WorkCreateError {
                        job_get_init_t,
                        job_get_success_t,
                        work_create_error_t: time,
                        ids,
                        error,
                    },
                    SnarkWorkerStatsPut::WorkCreateSuccess { time, ids } if ids == expected_ids => {
                        Self::WorkSubmitPending {
                            job_get_init_t,
                            job_get_success_t,
                            work_create_success_t: time,
                            ids,
                        }
                    }
                    _ => return false,
                };
            }
            Self::WorkSubmitPending {
                job_get_init_t,
                job_get_success_t,
                work_create_success_t,
                ids: expected_ids,
            } => {
                *self = match v {
                    SnarkWorkerStatsPut::WorkSubmitError {
                        time, error, ids, ..
                    } if ids == expected_ids => Self::WorkSubmitError {
                        job_get_init_t,
                        job_get_success_t,
                        work_create_success_t,
                        work_submit_error_t: time,
                        ids,
                        error,
                    },
                    SnarkWorkerStatsPut::WorkSubmitSuccess { time, ids } if ids == expected_ids => {
                        Self::WorkSubmitSuccess {
                            job_get_init_t,
                            job_get_success_t,
                            work_create_success_t,
                            work_submit_success_t: time,
                            ids,
                        }
                    }
                    _ => return false,
                };
            }
            _ => return false,
        }

        true
    }
}

impl Default for SnarkWorkerState {
    fn default() -> Self {
        Self::JobGetPending { job_get_init_t: 0 }
    }
}

#[tokio::main]
async fn main() {
    let opts = Opts::from_args();
    let default_timeout = opts.default_timeout;
    let max_timeout = opts.max_timeout;
    let max_key_len = opts.max_key_len;

    let table = Arc::new(Mutex::new(HashMap::new()));
    let worker_stats = Arc::new(Mutex::new(HashMap::new()));

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

    let kv = table.clone();
    let lock_job_put = warp::path!("lock-job" / String)
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

    let stats = worker_stats.clone();
    let worker_stats_put = warp::path!("worker-stats" / String)
        .and(warp::put())
        .and(warp::filters::body::json())
        .then(move |pub_key: String, req: SnarkWorkerStatsPut| {
            let stats = stats.clone();
            async move {
                let mut stats = stats.lock().await;
                match stats.entry(pub_key) {
                    Entry::Vacant(v) => match req {
                        SnarkWorkerStatsPut::JobGetInit { time } => {
                            let mut val = VecDeque::new();
                            val.push_front(SnarkWorkerState::init(time));
                            v.insert(val);
                        }
                        req => {
                            let err = format!(
                                "unexpected worker_stats/put\nstate: None\nrequest: {:?}",
                                req
                            );
                            eprintln!("{}", err);
                            return with_status(err, StatusCode::from_u16(400).unwrap());
                        }
                    },
                    Entry::Occupied(v) => {
                        let v = v.into_mut();
                        match req {
                            SnarkWorkerStatsPut::JobGetInit { time } => {
                                v.push_front(SnarkWorkerState::init(time));
                            }
                            req => {
                                if !v.front_mut().unwrap().apply(req.clone()) {
                                    let err = format!(
                                        "unexpected worker_stats/put\nstate: {:?}\nrequest: {:?}",
                                        v, req
                                    );
                                    eprintln!("{}", err);
                                    return with_status(err, StatusCode::from_u16(400).unwrap());
                                }
                            }
                        }
                    }
                }
                with_status("".to_owned(), StatusCode::from_u16(200).unwrap())
            }
        });

    let stats = worker_stats.clone();
    let worker_stats_get = warp::path!("worker-stats")
        .and(warp::get())
        .then(move || {
            let stats = stats.clone();
            async move {
                let stats = stats.lock().await;
                with_status(serde_json::to_string(&*stats).unwrap(), StatusCode::from_u16(200).unwrap())
            }
        });

    let routes = lock_job_put
        .or(worker_stats_put)
        .or(worker_stats_get);
    warp::serve(routes).run(([0, 0, 0, 0], opts.port)).await;
}
