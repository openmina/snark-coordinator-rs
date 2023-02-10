use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize, Serializer};
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

#[derive(Serialize, Deserialize, Default)]
struct WorkerStatsGetParams {
    workers: Option<String>,
    from_t: Option<u64>,
    to_t: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "kind")]
enum SnarkWorkerJobGetError {
    NoAvailableJob,
    Other { error: String },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "kind")]
enum SnarkWorkerStatsPut {
    Register {
        time: u64,
    },
    JobGetInit {
        time: u64,
    },
    JobGetError {
        time: u64,
        job_get_node_received_t: Option<u64>,
        job_get_node_request_work_init_t: Option<u64>,
        job_get_node_request_work_success_t: Option<u64>,
        error: SnarkWorkerJobGetError,
    },
    JobGetSuccess {
        time: u64,
        job_get_node_received_t: Option<u64>,
        job_get_node_request_work_init_t: Option<u64>,
        job_get_node_request_work_success_t: Option<u64>,
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
        work_submit_node_received_t: Option<u64>,
        work_submit_node_add_work_init_t: Option<u64>,
        work_submit_node_add_work_success_t: Option<u64>,
        ids: String,
        error: String,
    },
    WorkSubmitSuccess {
        time: u64,
        work_submit_node_received_t: Option<u64>,
        work_submit_node_add_work_init_t: Option<u64>,
        work_submit_node_add_work_success_t: Option<u64>,
        ids: String,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "kind")]
enum SnarkWorkerState {
    Registered {
        registered_t: u64,
    },
    JobGetPending {
        job_get_init_t: u64,
    },
    JobGetError {
        job_get_init_t: u64,
        job_get_node_received_t: Option<u64>,
        job_get_node_request_work_init_t: Option<u64>,
        job_get_node_request_work_success_t: Option<u64>,
        job_get_error_t: u64,
        error: SnarkWorkerJobGetError,
    },
    WorkCreatePending {
        job_get_init_t: u64,
        job_get_node_received_t: Option<u64>,
        job_get_node_request_work_init_t: Option<u64>,
        job_get_node_request_work_success_t: Option<u64>,
        job_get_success_t: u64,
        ids: String,
    },
    WorkCreateError {
        job_get_init_t: u64,
        job_get_node_received_t: Option<u64>,
        job_get_node_request_work_init_t: Option<u64>,
        job_get_node_request_work_success_t: Option<u64>,
        job_get_success_t: u64,
        work_create_error_t: u64,
        ids: String,
        error: String,
    },
    WorkSubmitPending {
        job_get_init_t: u64,
        job_get_node_received_t: Option<u64>,
        job_get_node_request_work_init_t: Option<u64>,
        job_get_node_request_work_success_t: Option<u64>,
        job_get_success_t: u64,
        work_create_success_t: u64,
        ids: String,
    },
    WorkSubmitError {
        job_get_init_t: u64,
        job_get_node_received_t: Option<u64>,
        job_get_node_request_work_init_t: Option<u64>,
        job_get_node_request_work_success_t: Option<u64>,
        job_get_success_t: u64,
        work_create_success_t: u64,
        work_submit_error_t: u64,
        ids: String,
        error: String,
    },
    WorkSubmitSuccess {
        job_get_init_t: u64,
        job_get_node_received_t: Option<u64>,
        job_get_node_request_work_init_t: Option<u64>,
        job_get_node_request_work_success_t: Option<u64>,
        job_get_success_t: u64,
        work_create_success_t: u64,
        work_submit_node_received_t: Option<u64>,
        work_submit_node_add_work_init_t: Option<u64>,
        work_submit_node_add_work_success_t: Option<u64>,
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

    fn start_time(&self) -> u64 {
        match self {
            Self::Registered { registered_t } => *registered_t,
            Self::JobGetPending { job_get_init_t }
            | Self::JobGetError { job_get_init_t, .. }
            | Self::WorkCreatePending { job_get_init_t, .. }
            | Self::WorkCreateError { job_get_init_t, .. }
            | Self::WorkSubmitPending { job_get_init_t, .. }
            | Self::WorkSubmitError { job_get_init_t, .. }
            | Self::WorkSubmitSuccess { job_get_init_t, .. } => *job_get_init_t,
        }
    }

    fn end_time(&self) -> u64 {
        match self {
            Self::Registered { registered_t } => *registered_t,
            Self::JobGetPending { job_get_init_t } => *job_get_init_t,
            Self::JobGetError {
                job_get_error_t, ..
            } => *job_get_error_t,
            Self::WorkCreatePending {
                job_get_success_t, ..
            } => *job_get_success_t,
            Self::WorkCreateError {
                work_create_error_t,
                ..
            } => *work_create_error_t,
            Self::WorkSubmitPending {
                work_create_success_t,
                ..
            } => *work_create_success_t,
            Self::WorkSubmitError {
                work_submit_error_t,
                ..
            } => *work_submit_error_t,
            Self::WorkSubmitSuccess {
                work_submit_success_t,
                ..
            } => *work_submit_success_t,
        }
    }

    fn apply(&mut self, v: SnarkWorkerStatsPut) -> bool {
        match self.clone() {
            Self::JobGetPending { job_get_init_t } => {
                *self = match v {
                    SnarkWorkerStatsPut::JobGetError {
                        time,
                        job_get_node_received_t,
                        job_get_node_request_work_init_t,
                        job_get_node_request_work_success_t,
                        error,
                    } => Self::JobGetError {
                        job_get_init_t,
                        job_get_node_received_t,
                        job_get_node_request_work_init_t,
                        job_get_node_request_work_success_t,
                        job_get_error_t: time,
                        error,
                    },
                    SnarkWorkerStatsPut::JobGetSuccess {
                        time,
                        job_get_node_received_t,
                        job_get_node_request_work_init_t,
                        job_get_node_request_work_success_t,
                        ids,
                    } => Self::WorkCreatePending {
                        job_get_init_t,
                        job_get_node_received_t,
                        job_get_node_request_work_init_t,
                        job_get_node_request_work_success_t,
                        job_get_success_t: time,
                        ids,
                    },
                    _ => return false,
                }
            }
            Self::WorkCreatePending {
                job_get_init_t,
                job_get_node_received_t,
                job_get_node_request_work_init_t,
                job_get_node_request_work_success_t,
                job_get_success_t,
                ids: expected_ids,
            } => {
                *self = match v {
                    SnarkWorkerStatsPut::WorkCreateError {
                        time, error, ids, ..
                    } if ids == expected_ids => Self::WorkCreateError {
                        job_get_init_t,
                        job_get_node_received_t,
                        job_get_node_request_work_init_t,
                        job_get_node_request_work_success_t,
                        job_get_success_t,
                        work_create_error_t: time,
                        ids,
                        error,
                    },
                    SnarkWorkerStatsPut::WorkCreateSuccess { time, ids } if ids == expected_ids => {
                        Self::WorkSubmitPending {
                            job_get_init_t,
                            job_get_node_received_t,
                            job_get_node_request_work_init_t,
                            job_get_node_request_work_success_t,
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
                job_get_node_received_t,
                job_get_node_request_work_init_t,
                job_get_node_request_work_success_t,
                job_get_success_t,
                work_create_success_t,
                ids: expected_ids,
            } => {
                *self = match v {
                    SnarkWorkerStatsPut::WorkSubmitError {
                        time, error, ids, ..
                    } if ids == expected_ids => Self::WorkSubmitError {
                        job_get_init_t,
                        job_get_node_received_t,
                        job_get_node_request_work_init_t,
                        job_get_node_request_work_success_t,
                        job_get_success_t,
                        work_create_success_t,
                        work_submit_error_t: time,
                        ids,
                        error,
                    },
                    SnarkWorkerStatsPut::WorkSubmitSuccess {
                        time,
                        work_submit_node_received_t,
                        work_submit_node_add_work_init_t,
                        work_submit_node_add_work_success_t,
                        ids,
                    } if ids == expected_ids => Self::WorkSubmitSuccess {
                        job_get_init_t,
                        job_get_node_received_t,
                        job_get_node_request_work_init_t,
                        job_get_node_request_work_success_t,
                        job_get_success_t,
                        work_create_success_t,
                        work_submit_node_received_t,
                        work_submit_node_add_work_init_t,
                        work_submit_node_add_work_success_t,
                        work_submit_success_t: time,
                        ids,
                    },
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
        .then(move |worker_id: String, req: SnarkWorkerStatsPut| {
            let stats = stats.clone();
            async move {
                let mut stats = stats.lock().await;

                match &req {
                    SnarkWorkerStatsPut::Register { time } => {
                        for i in 1..4096 {
                            let id = format!("{worker_id}_{i}");
                            match stats.entry(id) {
                                Entry::Vacant(stats) => {
                                    let registered = SnarkWorkerState::Registered {
                                        registered_t: *time,
                                    };
                                    let id = stats.key().clone();
                                    stats.insert(std::iter::once(registered).collect());
                                    return with_status(id, StatusCode::from_u16(200).unwrap());
                                }
                                _ => continue,
                            }
                        }
                        let err = format!("too many workers under same worker_id: {worker_id}");
                        eprintln!("{}", err);
                        return with_status(err, StatusCode::from_u16(400).unwrap());
                    }
                    _ => {}
                }

                match stats.entry(worker_id) {
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
                                if v.front_mut()
                                    .map(|v| !v.apply(req.clone()))
                                    .unwrap_or(false)
                                {
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
    let workers_get = warp::path!("workers").and(warp::get()).then(move || {
        let stats = stats.clone();
        async move {
            let stats = stats.lock().await;
            with_status(
                serde_json::to_string(&stats.keys().collect::<Vec<_>>()).unwrap(),
                StatusCode::from_u16(200).unwrap(),
            )
        }
    });

    let stats = worker_stats.clone();
    let worker_stats_get = warp::path!("worker-stats")
        .and(
            warp::filters::query::query::<WorkerStatsGetParams>()
                .or(warp::any().map(WorkerStatsGetParams::default))
                .unify(),
        )
        .and(warp::get())
        .then(move |params: WorkerStatsGetParams| {
            let stats = stats.clone();
            async move {
                let stats = stats.lock().await;
                let workers_filter = params
                    .workers
                    .map(|s| s.split(",").map(|s| s.to_owned()).collect::<Vec<_>>());
                let start_t_filter = params.from_t;
                let end_t_filter = params.to_t;

                let iter = stats
                    .iter()
                    .filter(|(k, _)| workers_filter.as_ref().map_or(true, |f| f.contains(k)))
                    .map(|(k, states)| {
                        let v = states
                            .iter()
                            .skip_while(|v| end_t_filter.map_or(false, |f| f < v.end_time()))
                            .take_while(|v| start_t_filter.map_or(true, |f| v.start_time() >= f))
                            .collect::<Vec<_>>();
                        (k, v)
                    });
                let mut buf = Vec::with_capacity(32 * 1024);
                let mut ser = serde_json::Serializer::new(&mut buf);
                ser.collect_map(iter).unwrap();

                with_status(
                    String::from_utf8(buf).unwrap(),
                    StatusCode::from_u16(200).unwrap(),
                )
            }
        });

    let routes = lock_job_put
        .or(worker_stats_put)
        .or(workers_get)
        .or(worker_stats_get);
    warp::serve(routes).run(([0, 0, 0, 0], opts.port)).await;
}
