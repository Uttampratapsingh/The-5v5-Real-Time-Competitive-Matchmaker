use std::{
    collections::{BTreeMap, HashMap},
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

const N_SHARDS: usize = 8;
const BRACKET_SIZE: f64 = 200.0;
const BASE_WINDOW: f64 = 150.0;
const WINDOW_EXPAND_PER_SEC: f64 = 50.0;
const MAX_WINDOW: f64 = 600.0;
const CROSS_REGION_WAIT_SECS: u64 = 15;
const WORKER_IDLE_MS: u64 = 5;
const METRICS_PORT: u16 = 9090;
const API_PORT: u16 = 8080;
const AVG_WAIT_SAMPLES: usize = 100;
const MAX_RATING: f64 = 3000.0;

#[derive(Clone)]
struct Player {
    id: u64,
    skill_rating: f64,
    ping_region: String,
    join_timestamp: Instant,
}

struct PlayerEntry {
    player: Player,
    evicted: AtomicBool,
}

#[derive(Clone, Hash, Eq, PartialEq)]
struct BucketKey {
    bracket: u32,
    region: String,
}

#[derive(Copy, Clone, Debug, PartialEq)]
struct OrdF64(f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

struct Shard {
    buckets: HashMap<BucketKey, BTreeMap<OrdF64, Vec<Arc<PlayerEntry>>>>,
}

struct PlayerLocator {
    shard_idx: usize,
    bucket_key: BucketKey,
    rating_key: OrdF64,
    entry: Arc<PlayerEntry>,
}

struct Candidate {
    entry: Arc<PlayerEntry>,
    bucket_key: BucketKey,
    rating_key: OrdF64,
    shard_idx: usize,
    join_timestamp: Instant,
    id: u64,
}

struct MatchResult {
    team_a_avg_sr: f64,
    team_b_avg_sr: f64,
    sr_delta: f64,
    match_quality_score: f64,
}

struct WaitStats {
    samples: [u64; AVG_WAIT_SAMPLES],
    idx: usize,
    len: usize,
    sum: u64,
}

impl WaitStats {
    fn new() -> Self {
        Self {
            samples: [0; AVG_WAIT_SAMPLES],
            idx: 0,
            len: 0,
            sum: 0,
        }
    }

    fn record(&mut self, wait_ms: u64) {
        if self.len < AVG_WAIT_SAMPLES {
            self.len += 1;
        } else {
            self.sum = self.sum.saturating_sub(self.samples[self.idx]);
        }
        self.samples[self.idx] = wait_ms;
        self.sum = self.sum.saturating_add(wait_ms);
        self.idx = (self.idx + 1) % AVG_WAIT_SAMPLES;
    }

    fn average(&self) -> u64 {
        if self.len == 0 {
            0
        } else {
            self.sum / self.len as u64
        }
    }
}

struct Metrics {
    total_matches_formed: AtomicU64,
    total_players_queued: AtomicU64,
    queue_depth: AtomicU64,
    wait_stats: Mutex<WaitStats>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            total_matches_formed: AtomicU64::new(0),
            total_players_queued: AtomicU64::new(0),
            queue_depth: AtomicU64::new(0),
            wait_stats: Mutex::new(WaitStats::new()),
        }
    }

    fn snapshot(&self) -> MetricsSnapshot {
        let avg_wait_ms = self.wait_stats.lock().unwrap().average();
        MetricsSnapshot {
            total_matches_formed: self.total_matches_formed.load(Ordering::Relaxed),
            total_players_queued: self.total_players_queued.load(Ordering::Relaxed),
            avg_wait_ms,
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
        }
    }

    fn record_wait_ms(&self, wait_ms: u64) {
        self.wait_stats.lock().unwrap().record(wait_ms);
    }
}

#[derive(Serialize)]
struct MetricsSnapshot {
    total_matches_formed: u64,
    total_players_queued: u64,
    avg_wait_ms: u64,
    queue_depth: u64,
}

#[derive(Deserialize)]
struct QueueRequest {
    id: u64,
    skill_rating: f64,
    ping_region: String,
}

#[derive(Serialize)]
struct QueueResponse {
    queued: bool,
    position: usize,
}

#[derive(Serialize)]
struct DeleteResponse {
    removed: bool,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize)]
struct MatchLog {
    matched_ids: Vec<u64>,
    regions: Vec<String>,
    team_a_avg_sr: f64,
    team_b_avg_sr: f64,
    sr_delta: f64,
    match_quality_score: f64,
}

struct Pivot {
    region: String,
    median_rating: f64,
    oldest_wait_secs: f64,
    allow_cross_region: bool,
}

struct Matchmaker {
    shards: Vec<Mutex<Shard>>,
    index: Mutex<HashMap<u64, PlayerLocator>>,
    metrics: Arc<Metrics>,
}

impl Matchmaker {
    fn new(metrics: Arc<Metrics>) -> Self {
        let mut shards = Vec::with_capacity(N_SHARDS);
        for _ in 0..N_SHARDS {
            shards.push(Mutex::new(Shard {
                buckets: HashMap::new(),
            }));
        }
        Self {
            shards,
            index: Mutex::new(HashMap::new()),
            metrics,
        }
    }

    fn enqueue(&self, req: QueueRequest) -> Result<usize, EnqueueError> {
        if !req.skill_rating.is_finite() {
            return Err(EnqueueError::InvalidRating);
        }

        let skill_rating = clamp_rating(req.skill_rating);
        let bracket = rating_to_bracket(skill_rating);
        let shard_idx = shard_index(bracket, &req.ping_region);

        let player = Player {
            id: req.id,
            skill_rating,
            ping_region: req.ping_region.clone(),
            join_timestamp: Instant::now(),
        };
        let entry = Arc::new(PlayerEntry {
            player,
            evicted: AtomicBool::new(false),
        });
        let rating_key = OrdF64(skill_rating);
        let bucket_key = BucketKey {
            bracket,
            region: req.ping_region.clone(),
        };

        let mut index = self.index.lock().unwrap();
        if index.contains_key(&req.id) {
            return Err(EnqueueError::DuplicateId);
        }

        let mut shard = self.shards[shard_idx].lock().unwrap();
        let bucket = shard.buckets.entry(bucket_key.clone()).or_default();
        bucket.entry(rating_key).or_default().push(entry.clone());

        let position = (self
            .metrics
            .queue_depth
            .fetch_add(1, Ordering::Relaxed)
            + 1) as usize;
        self.metrics
            .total_players_queued
            .fetch_add(1, Ordering::Relaxed);

        index.insert(
            req.id,
            PlayerLocator {
                shard_idx,
                bucket_key,
                rating_key,
                entry,
            },
        );

        Ok(position)
    }

    fn remove(&self, id: u64) -> bool {
        let locator = {
            let mut index = self.index.lock().unwrap();
            index.remove(&id)
        };

        let Some(locator) = locator else {
            return false;
        };

        let mut shard = self.shards[locator.shard_idx].lock().unwrap();
        let removed = remove_locator(&mut shard, &locator);
        if removed {
            locator.entry.evicted.store(true, Ordering::Release);
            self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
        }
        removed
    }

    fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    fn try_form_match(&self, log_tx: &Sender<MatchLog>) -> bool {
        let now = Instant::now();
        let Some(pivot) = self.find_pivot(now) else {
            return false;
        };

        let mut candidates = self.collect_candidates(&pivot, BASE_WINDOW);
        if candidates.len() < 10 {
            let expanded =
                (BASE_WINDOW + WINDOW_EXPAND_PER_SEC * pivot.oldest_wait_secs).min(MAX_WINDOW);
            if expanded > BASE_WINDOW {
                candidates = self.collect_candidates(&pivot, expanded);
            }
        }

        if candidates.len() < 10 {
            return false;
        }

        self.evict_candidates(&candidates, now, log_tx)
    }

    fn find_pivot(&self, now: Instant) -> Option<Pivot> {
        let mut oldest_wait = Duration::from_secs(0);
        let mut oldest_bucket: Option<BucketKey> = None;

        for shard in &self.shards {
            let shard = shard.lock().unwrap();
            for (bucket_key, tree) in &shard.buckets {
                for vec in tree.values() {
                    for entry in vec {
                        if entry.evicted.load(Ordering::Acquire) {
                            continue;
                        }
                        let wait = now.duration_since(entry.player.join_timestamp);
                        if wait > oldest_wait {
                            oldest_wait = wait;
                            oldest_bucket = Some(bucket_key.clone());
                        }
                    }
                }
            }
        }

        let bucket = oldest_bucket?;
        let median_rating = self.bucket_median_rating(&bucket)?;
        let oldest_wait_secs = oldest_wait.as_secs_f64();
        let allow_cross_region = oldest_wait.as_secs() >= CROSS_REGION_WAIT_SECS;

        Some(Pivot {
            region: bucket.region,
            median_rating,
            oldest_wait_secs,
            allow_cross_region,
        })
    }

    fn bucket_median_rating(&self, bucket: &BucketKey) -> Option<f64> {
        let shard_idx = shard_index(bucket.bracket, &bucket.region);
        let shard = self.shards[shard_idx].lock().unwrap();
        let tree = shard.buckets.get(bucket)?;
        let total: usize = tree.values().map(|vec| vec.len()).sum();
        if total == 0 {
            return None;
        }

        let mut mid = total / 2;
        for (rating_key, vec) in tree {
            if mid < vec.len() {
                return Some(rating_key.0);
            }
            mid -= vec.len();
        }
        None
    }

    fn collect_candidates(&self, pivot: &Pivot, window: f64) -> Vec<Candidate> {
        let min_rating = (pivot.median_rating - window).max(0.0);
        let max_rating = (pivot.median_rating + window).min(MAX_RATING);
        let min_bracket = rating_to_bracket(min_rating);
        let max_bracket = rating_to_bracket(max_rating);

        let mut candidates = Vec::new();

        for (shard_idx, shard_mutex) in self.shards.iter().enumerate() {
            let shard = shard_mutex.lock().unwrap();
            for (bucket_key, tree) in &shard.buckets {
                if bucket_key.bracket < min_bracket || bucket_key.bracket > max_bracket {
                    continue;
                }
                if !pivot.allow_cross_region && bucket_key.region != pivot.region {
                    continue;
                }

                for (rating_key, vec) in tree.range(OrdF64(min_rating)..=OrdF64(max_rating)) {
                    for entry in vec {
                        if entry.evicted.load(Ordering::Acquire) {
                            continue;
                        }
                        let player = &entry.player;
                        candidates.push(Candidate {
                            entry: entry.clone(),
                            bucket_key: bucket_key.clone(),
                            rating_key: *rating_key,
                            shard_idx,
                            join_timestamp: player.join_timestamp,
                            id: player.id,
                        });
                    }
                }
            }
        }

        candidates.sort_by(|a, b| {
            a.join_timestamp
                .cmp(&b.join_timestamp)
                .then_with(|| a.id.cmp(&b.id))
        });
        candidates
    }

    fn evict_candidates(
        &self,
        candidates: &[Candidate],
        now: Instant,
        log_tx: &Sender<MatchLog>,
    ) -> bool {
        if candidates.len() < 10 {
            return false;
        }
        let selected = &candidates[..10];

        let mut shard_indices: Vec<usize> = selected.iter().map(|c| c.shard_idx).collect();
        shard_indices.sort_unstable();
        shard_indices.dedup();

        let mut guards: Vec<(usize, std::sync::MutexGuard<Shard>)> = Vec::new();
        for idx in shard_indices {
            let guard = self.shards[idx].lock().unwrap();
            guards.push((idx, guard));
        }

        let mut guard_map = HashMap::new();
        for (pos, (idx, _)) in guards.iter().enumerate() {
            guard_map.insert(*idx, pos);
        }

        for cand in selected {
            let Some(pos) = guard_map.get(&cand.shard_idx) else {
                return false;
            };
            let shard = &guards[*pos].1;
            if !candidate_present(shard, cand) {
                return false;
            }
        }

        let mut claimed: Vec<&Candidate> = Vec::new();
        for cand in selected {
            if cand
                .entry
                .evicted
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                for prev in claimed {
                    prev.entry.evicted.store(false, Ordering::Release);
                }
                return false;
            }
            claimed.push(cand);
        }

        for cand in selected {
            let pos = guard_map.get(&cand.shard_idx).copied().unwrap_or(0);
            let shard = &mut guards[pos].1;
            let _ = remove_candidate(shard, cand);
        }

        drop(guards);

        let mut index = self.index.lock().unwrap();
        for cand in selected {
            index.remove(&cand.id);
        }
        drop(index);

        self.metrics
            .total_matches_formed
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_depth.fetch_sub(10, Ordering::Relaxed);

        for cand in selected {
            let wait_ms = now.duration_since(cand.join_timestamp).as_millis() as u64;
            self.metrics.record_wait_ms(wait_ms);
        }

        let mut players: Vec<Player> = selected.iter().map(|c| c.entry.player.clone()).collect();
        let match_result = compute_match_result(&mut players);
        let matched_ids = selected.iter().map(|c| c.id).collect();
        let regions = selected
            .iter()
            .map(|c| c.entry.player.ping_region.clone())
            .collect();
        let _ = log_tx.send(MatchLog {
            matched_ids,
            regions,
            team_a_avg_sr: match_result.team_a_avg_sr,
            team_b_avg_sr: match_result.team_b_avg_sr,
            sr_delta: match_result.sr_delta,
            match_quality_score: match_result.match_quality_score,
        });

        true
    }
}

#[derive(Debug)]
enum EnqueueError {
    DuplicateId,
    InvalidRating,
}

impl EnqueueError {
    fn status(&self) -> StatusCode {
        match self {
            EnqueueError::DuplicateId => StatusCode(409),
            EnqueueError::InvalidRating => StatusCode(400),
        }
    }

    fn message(&self) -> &'static str {
        match self {
            EnqueueError::DuplicateId => "player id already queued",
            EnqueueError::InvalidRating => "skill_rating must be a finite number",
        }
    }
}

fn main() {
    if let Err(err) = run() {
        eprintln!("fatal error: {err}");
    }
}

fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_handler = shutdown.clone();
    ctrlc::set_handler(move || {
        shutdown_handler.store(true, Ordering::Relaxed);
    })?;

    let metrics = Arc::new(Metrics::new());
    let matchmaker = Arc::new(Matchmaker::new(metrics.clone()));

    let (log_tx, log_rx) = mpsc::channel::<MatchLog>();
    spawn_logger(log_rx, shutdown.clone());
    spawn_metrics_server(metrics, shutdown.clone());
    spawn_workers(matchmaker.clone(), log_tx.clone(), shutdown.clone());

    run_api_server(matchmaker, shutdown)?;
    Ok(())
}

fn spawn_workers(
    matchmaker: Arc<Matchmaker>,
    log_tx: Sender<MatchLog>,
    shutdown: Arc<AtomicBool>,
) {
    let worker_count = thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4);

    for _ in 0..worker_count {
        let mm = matchmaker.clone();
        let tx = log_tx.clone();
        let sd = shutdown.clone();
        thread::spawn(move || worker_loop(mm, tx, sd));
    }
}

fn worker_loop(matchmaker: Arc<Matchmaker>, log_tx: Sender<MatchLog>, shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Relaxed) {
        let formed = matchmaker.try_form_match(&log_tx);
        if !formed {
            thread::sleep(Duration::from_millis(WORKER_IDLE_MS));
        }
    }
}

fn spawn_logger(rx: Receiver<MatchLog>, shutdown: Arc<AtomicBool>) {
    thread::spawn(move || {
        let mut stdout = std::io::stdout();
        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(log) => {
                    if let Ok(line) = serde_json::to_string(&log) {
                        let _ = writeln!(stdout, "{line}");
                        let _ = stdout.flush();
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    });
}

fn run_api_server(
    matchmaker: Arc<Matchmaker>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = Server::http(("0.0.0.0", API_PORT))?;
    while !shutdown.load(Ordering::Relaxed) {
        match server.recv_timeout(Duration::from_millis(100)) {
            Ok(Some(request)) => {
                if let Err(err) = handle_request(request, &matchmaker) {
                    eprintln!("request error: {err}");
                }
            }
            Ok(None) => continue,
            Err(err) => eprintln!("api server error: {err}"),
        }
    }
    Ok(())
}

fn handle_request(mut request: Request, matchmaker: &Matchmaker) -> std::io::Result<()> {
    match (request.method(), request.url()) {
        (&Method::Post, "/queue") => {
            let mut body = String::new();
            request.as_reader().read_to_string(&mut body)?;
            let payload: QueueRequest = match serde_json::from_str(&body) {
                Ok(payload) => payload,
                Err(_) => {
                    return respond_error(request, StatusCode(400), "invalid JSON payload")
                }
            };

            match matchmaker.enqueue(payload) {
                Ok(position) => respond_json(
                    request,
                    StatusCode(200),
                    &QueueResponse {
                        queued: true,
                        position,
                    },
                ),
                Err(err) => respond_error(request, err.status(), err.message()),
            }
        }
        (&Method::Delete, path) if path.starts_with("/queue/") => {
            let id_str = path.trim_start_matches("/queue/");
            let id: u64 = match id_str.parse() {
                Ok(id) => id,
                Err(_) => return respond_error(request, StatusCode(400), "invalid id"),
            };
            let removed = matchmaker.remove(id);
            respond_json(request, StatusCode(200), &DeleteResponse { removed })
        }
        (&Method::Get, "/metrics") => {
            respond_json(request, StatusCode(200), &matchmaker.metrics_snapshot())
        }
        _ => respond_error(request, StatusCode(404), "not found"),
    }
}

fn respond_json<T: Serialize>(
    request: Request,
    status: StatusCode,
    payload: &T,
) -> std::io::Result<()> {
    let body = serde_json::to_string(payload).unwrap_or_else(|_| "{}".to_string());
    let response = Response::from_string(body)
        .with_status_code(status)
        .with_header(Header::from_bytes("Content-Type", "application/json").unwrap());
    request.respond(response)
}

fn respond_error(request: Request, status: StatusCode, message: &str) -> std::io::Result<()> {
    respond_json(
        request,
        status,
        &ErrorResponse {
            error: message.to_string(),
        },
    )
}

fn spawn_metrics_server(metrics: Arc<Metrics>, shutdown: Arc<AtomicBool>) {
    thread::spawn(move || {
        if let Err(err) = run_metrics_server(metrics, shutdown) {
            eprintln!("metrics server error: {err}");
        }
    });
}

fn run_metrics_server(metrics: Arc<Metrics>, shutdown: Arc<AtomicBool>) -> std::io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", METRICS_PORT))?;
    listener.set_nonblocking(true)?;

    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                let _ = handle_metrics_connection(stream, &metrics);
            }
            Err(ref err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(ref err) if err.kind() == std::io::ErrorKind::TimedOut => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn handle_metrics_connection(mut stream: TcpStream, metrics: &Metrics) -> std::io::Result<()> {
    let mut buffer = [0u8; 1024];
    let _ = stream.read(&mut buffer);

    let body = serde_json::to_string(&metrics.snapshot()).unwrap_or_else(|_| "{}".to_string());
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes())
}

fn candidate_present(shard: &Shard, cand: &Candidate) -> bool {
    if cand.entry.evicted.load(Ordering::Acquire) {
        return false;
    }
    let Some(tree) = shard.buckets.get(&cand.bucket_key) else {
        return false;
    };
    let Some(vec) = tree.get(&cand.rating_key) else {
        return false;
    };
    vec.iter().any(|entry| Arc::ptr_eq(entry, &cand.entry))
}

fn remove_candidate(shard: &mut Shard, cand: &Candidate) -> bool {
    let Some(tree) = shard.buckets.get_mut(&cand.bucket_key) else {
        return false;
    };

    let mut removed = false;
    let mut remove_rating_key = false;

    if let Some(vec) = tree.get_mut(&cand.rating_key) {
        let before = vec.len();
        vec.retain(|entry| !Arc::ptr_eq(entry, &cand.entry));
        removed = vec.len() < before;
        remove_rating_key = vec.is_empty();
    }

    if remove_rating_key {
        tree.remove(&cand.rating_key);
    }
    if tree.is_empty() {
        shard.buckets.remove(&cand.bucket_key);
    }

    removed
}

fn remove_locator(shard: &mut Shard, locator: &PlayerLocator) -> bool {
    let Some(tree) = shard.buckets.get_mut(&locator.bucket_key) else {
        return false;
    };

    let mut removed = false;
    let mut remove_rating_key = false;

    if let Some(vec) = tree.get_mut(&locator.rating_key) {
        let before = vec.len();
        vec.retain(|entry| !Arc::ptr_eq(entry, &locator.entry));
        removed = vec.len() < before;
        remove_rating_key = vec.is_empty();
    }

    if remove_rating_key {
        tree.remove(&locator.rating_key);
    }
    if tree.is_empty() {
        shard.buckets.remove(&locator.bucket_key);
    }

    removed
}

fn compute_match_result(players: &mut [Player]) -> MatchResult {
    players.sort_by(|a, b| a.skill_rating.total_cmp(&b.skill_rating));

    let team_a_indices = [0usize, 3, 5, 7, 9];
    let team_b_indices = [1usize, 2, 4, 6, 8];

    let mut team_a_sum = 0.0;
    let mut team_b_sum = 0.0;

    for idx in team_a_indices {
        team_a_sum += players[idx].skill_rating;
    }
    for idx in team_b_indices {
        team_b_sum += players[idx].skill_rating;
    }

    let team_a_avg_sr = team_a_sum / 5.0;
    let team_b_avg_sr = team_b_sum / 5.0;
    let sr_delta = (team_a_avg_sr - team_b_avg_sr).abs();
    let match_quality_score = (1.0 - (sr_delta / (MAX_WINDOW * 2.0))).clamp(0.0, 1.0);

    MatchResult {
        team_a_avg_sr,
        team_b_avg_sr,
        sr_delta,
        match_quality_score,
    }
}

fn rating_to_bracket(rating: f64) -> u32 {
    (rating / BRACKET_SIZE).floor() as u32
}

fn shard_index(bracket: u32, region: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    bracket.hash(&mut hasher);
    region.hash(&mut hasher);
    (hasher.finish() as usize) % N_SHARDS
}

fn clamp_rating(rating: f64) -> f64 {
    rating.clamp(0.0, MAX_RATING)
}
