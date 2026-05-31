use std::{
    collections::hash_map::DefaultHasher,
    collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque},
    env,
    hash::{Hash, Hasher},
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
        Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

const N_SHARDS: usize = 8;
const BRACKET_SIZE: f64 = 200.0;
const DEFAULT_BASE_WINDOW: f64 = 150.0;
const DEFAULT_WINDOW_EXPAND_PER_SEC: f64 = 50.0;
const DEFAULT_MAX_WINDOW: f64 = 600.0;
const DEFAULT_CROSS_REGION_WAIT_SECS: u64 = 15;
const DEFAULT_WORKER_IDLE_MS: u64 = 5;
const DEFAULT_API_WORKER_MULTIPLIER: usize = 4;
const DEFAULT_API_QUEUE_LIMIT: usize = 4096;
const METRICS_PORT: u16 = 9090;
const API_PORT: u16 = 8080;
const AVG_WAIT_SAMPLES: usize = 100;
const MAX_RATING: f64 = 3000.0;

struct Config {
    base_window: f64,
    window_expand_per_sec: f64,
    max_window: f64,
    cross_region_wait_secs: u64,
    worker_idle_ms: u64,
    api_worker_count: usize,
}

impl Config {
    fn from_env() -> Self {
        let cpu_count = thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(4);
        let default_api_workers = cpu_count.saturating_mul(DEFAULT_API_WORKER_MULTIPLIER);
        let mut config = Self {
            base_window: DEFAULT_BASE_WINDOW,
            window_expand_per_sec: DEFAULT_WINDOW_EXPAND_PER_SEC,
            max_window: DEFAULT_MAX_WINDOW,
            cross_region_wait_secs: DEFAULT_CROSS_REGION_WAIT_SECS,
            worker_idle_ms: DEFAULT_WORKER_IDLE_MS,
            api_worker_count: default_api_workers,
        };

        if env_bool("MATCHMAKER_FAST_CROSS_REGION") || env_bool("MATCHMAKER_FAST") {
            config.cross_region_wait_secs = 0;
        }

        config.base_window = env_f64("MATCHMAKER_BASE_WINDOW", config.base_window);
        config.window_expand_per_sec = env_f64(
            "MATCHMAKER_WINDOW_EXPAND_PER_SEC",
            config.window_expand_per_sec,
        );
        config.max_window = env_f64("MATCHMAKER_MAX_WINDOW", config.max_window)
            .max(config.base_window);
        config.cross_region_wait_secs =
            env_u64("MATCHMAKER_CROSS_REGION_WAIT_SECS", config.cross_region_wait_secs);
        if env_bool("MATCHMAKER_FAST_CROSS_REGION") {
            config.cross_region_wait_secs = 0;
        }
        config.worker_idle_ms = env_u64("MATCHMAKER_WORKER_IDLE_MS", config.worker_idle_ms);
        config.api_worker_count =
            env_usize("MATCHMAKER_API_WORKERS", config.api_worker_count).max(1);

        config
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str) -> bool {
    matches!(
        env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

fn env_f64(name: &str, default: f64) -> f64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

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

#[derive(Clone)]
struct OldestEntry {
    join_timestamp: Instant,
    id: u64,
    rating: f64,
    region: String,
    entry: Arc<PlayerEntry>,
}

impl PartialEq for OldestEntry {
    fn eq(&self, other: &Self) -> bool {
        self.join_timestamp == other.join_timestamp && self.id == other.id
    }
}

impl Eq for OldestEntry {}

impl PartialOrd for OldestEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OldestEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.join_timestamp
            .cmp(&other.join_timestamp)
            .then_with(|| self.id.cmp(&other.id))
    }
}

struct CandidateKey {
    join_timestamp: Instant,
    id: u64,
    candidate: Candidate,
}

impl PartialEq for CandidateKey {
    fn eq(&self, other: &Self) -> bool {
        self.join_timestamp == other.join_timestamp && self.id == other.id
    }
}

impl Eq for CandidateKey {}

impl PartialOrd for CandidateKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CandidateKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.join_timestamp
            .cmp(&other.join_timestamp)
            .then_with(|| self.id.cmp(&other.id))
    }
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
}

struct ApiRequestQueue {
    queue: Mutex<VecDeque<Request>>,
    cv: Condvar,
    shutdown: Arc<AtomicBool>,
}

impl ApiRequestQueue {
    fn new(shutdown: Arc<AtomicBool>) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            shutdown,
        }
    }

    fn push(&self, request: Request) -> Result<(), Box<Request>> {
        let mut queue = self.queue.lock().unwrap();
        if queue.len() >= DEFAULT_API_QUEUE_LIMIT {
            return Err(Box::new(request));
        }
        queue.push_back(request);
        self.cv.notify_one();
        Ok(())
    }

    fn pop(&self) -> Option<Request> {
        let mut queue = self.queue.lock().unwrap();
        loop {
            if let Some(request) = queue.pop_front() {
                return Some(request);
            }
            if self.shutdown.load(Ordering::Relaxed) {
                return None;
            }
            queue = self.cv.wait(queue).unwrap();
        }
    }

    fn close(&self) {
        self.cv.notify_all();
    }
}

struct Matchmaker {
    shards: Vec<Mutex<Shard>>,
    index: Mutex<HashMap<u64, PlayerLocator>>,
    regions: Mutex<HashSet<String>>,
    oldest_heap: Mutex<BinaryHeap<std::cmp::Reverse<OldestEntry>>>,
    metrics: Arc<Metrics>,
    config: Arc<Config>,
}

impl Matchmaker {
    fn new(metrics: Arc<Metrics>, config: Arc<Config>) -> Self {
        let mut shards = Vec::with_capacity(N_SHARDS);
        for _ in 0..N_SHARDS {
            shards.push(Mutex::new(Shard {
                buckets: HashMap::new(),
            }));
        }
        Self {
            shards,
            index: Mutex::new(HashMap::new()),
            regions: Mutex::new(HashSet::new()),
            oldest_heap: Mutex::new(BinaryHeap::new()),
            metrics,
            config,
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

        self.oldest_heap
            .lock()
            .unwrap()
            .push(std::cmp::Reverse(OldestEntry {
                join_timestamp: entry.player.join_timestamp,
                id: entry.player.id,
                rating: entry.player.skill_rating,
                region: entry.player.ping_region.clone(),
                entry: entry.clone(),
            }));

        self.regions
            .lock()
            .unwrap()
            .insert(req.ping_region.clone());

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

    fn regions_snapshot(&self) -> Vec<String> {
        self.regions
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }

    fn oldest_entry_snapshot(&self) -> Option<OldestEntry> {
        let mut heap = self.oldest_heap.lock().unwrap();
        loop {
            let top = heap.peek()?;
            if top.0.entry.evicted.load(Ordering::Acquire) {
                heap.pop();
                continue;
            }
            return Some(top.0.clone());
        }
    }

    fn try_form_match(&self, log_tx: &Sender<MatchLog>) -> bool {
        if self.metrics.queue_depth.load(Ordering::Relaxed) < 10 {
            return false;
        }
        let now = Instant::now();
        let Some(pivot) = self.find_pivot(now) else {
            return false;
        };

        let allow_cross_region =
            pivot.oldest_wait_secs >= self.config.cross_region_wait_secs as f64;
        let base_window = self.config.base_window;
        let expanded = (base_window
            + self.config.window_expand_per_sec * pivot.oldest_wait_secs)
            .min(self.config.max_window);
        let mut candidates = self.collect_candidates(&pivot, base_window, allow_cross_region);
        if candidates.len() < 10 && expanded > base_window {
            candidates = self.collect_candidates(&pivot, expanded, allow_cross_region);
        }

        if candidates.len() < 10 {
            return false;
        }

        self.evict_candidates(&candidates, now, log_tx)
    }

    fn find_pivot(&self, now: Instant) -> Option<Pivot> {
        let oldest = self.oldest_entry_snapshot()?;
        if oldest.entry.evicted.load(Ordering::Acquire) {
            return None;
        }

        let bracket = rating_to_bracket(oldest.rating);
        let bucket = BucketKey {
            bracket,
            region: oldest.region.clone(),
        };
        let median_rating = self.bucket_median_rating(&bucket)?;
        let oldest_wait_secs = now.duration_since(oldest.join_timestamp).as_secs_f64();

        Some(Pivot {
            region: oldest.region,
            median_rating,
            oldest_wait_secs,
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

    fn collect_candidates(
        &self,
        pivot: &Pivot,
        window: f64,
        allow_cross_region: bool,
    ) -> Vec<Candidate> {
        let min_rating = (pivot.median_rating - window).max(0.0);
        let max_rating = (pivot.median_rating + window).min(MAX_RATING);
        let min_bracket = rating_to_bracket(min_rating);
        let max_bracket = rating_to_bracket(max_rating);

        let mut candidates = BinaryHeap::with_capacity(10);

        let regions = if allow_cross_region {
            let snapshot = self.regions_snapshot();
            if snapshot.is_empty() {
                vec![pivot.region.clone()]
            } else {
                snapshot
            }
        } else {
            vec![pivot.region.clone()]
        };

        let mut buckets_by_shard: HashMap<usize, Vec<BucketKey>> = HashMap::new();
        for region in regions {
            for bracket in min_bracket..=max_bracket {
                let bucket_key = BucketKey {
                    bracket,
                    region: region.clone(),
                };
                let shard_idx = shard_index(bracket, &region);
                buckets_by_shard
                    .entry(shard_idx)
                    .or_default()
                    .push(bucket_key);
            }
        }

        for (shard_idx, bucket_keys) in buckets_by_shard {
            let shard = self.shards[shard_idx].lock().unwrap();
            for bucket_key in bucket_keys {
                let Some(tree) = shard.buckets.get(&bucket_key) else {
                    continue;
                };
                for (rating_key, vec) in tree.range(OrdF64(min_rating)..=OrdF64(max_rating)) {
                    for entry in vec {
                        if entry.evicted.load(Ordering::Acquire) {
                            continue;
                        }
                        let player = &entry.player;
                        let candidate = Candidate {
                            entry: entry.clone(),
                            bucket_key: bucket_key.clone(),
                            rating_key: *rating_key,
                            shard_idx,
                            join_timestamp: player.join_timestamp,
                            id: player.id,
                        };
                        candidates.push(CandidateKey {
                            join_timestamp: candidate.join_timestamp,
                            id: candidate.id,
                            candidate,
                        });
                        if candidates.len() > 10 {
                            let _ = candidates.pop();
                        }
                    }
                }
            }
        }

        let mut trimmed: Vec<Candidate> = candidates.into_iter().map(|key| key.candidate).collect();
        trimmed.sort_by(|a, b| {
            a.join_timestamp
                .cmp(&b.join_timestamp)
                .then_with(|| a.id.cmp(&b.id))
        });
        trimmed
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
        let match_result = compute_match_result(&mut players, self.config.max_window);
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
    let config = Arc::new(Config::from_env());
    let matchmaker = Arc::new(Matchmaker::new(metrics.clone(), config.clone()));

    let (log_tx, log_rx) = mpsc::channel::<MatchLog>();
    spawn_logger(log_rx, shutdown.clone());
    spawn_metrics_server(metrics, shutdown.clone());
    spawn_workers(
        matchmaker.clone(),
        log_tx.clone(),
        shutdown.clone(),
        config.clone(),
    );

    run_api_server(matchmaker, shutdown, config)?;
    Ok(())
}

fn spawn_workers(
    matchmaker: Arc<Matchmaker>,
    log_tx: Sender<MatchLog>,
    shutdown: Arc<AtomicBool>,
    config: Arc<Config>,
) {
    let worker_count = thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4);

    for _ in 0..worker_count {
        let mm = matchmaker.clone();
        let tx = log_tx.clone();
        let sd = shutdown.clone();
        let cfg = config.clone();
        thread::spawn(move || worker_loop(mm, tx, sd, cfg));
    }
}

fn worker_loop(
    matchmaker: Arc<Matchmaker>,
    log_tx: Sender<MatchLog>,
    shutdown: Arc<AtomicBool>,
    config: Arc<Config>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        let formed = matchmaker.try_form_match(&log_tx);
        if !formed {
            let idle_ms = config.worker_idle_ms;
            if idle_ms == 0 {
                thread::yield_now();
            } else {
                thread::sleep(Duration::from_millis(idle_ms));
            }
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
    config: Arc<Config>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = Server::http(("0.0.0.0", API_PORT))?;
    let request_queue = Arc::new(ApiRequestQueue::new(shutdown.clone()));
    spawn_api_workers(matchmaker.clone(), request_queue.clone(), shutdown.clone(), config);
    while !shutdown.load(Ordering::Relaxed) {
        match server.recv_timeout(Duration::from_millis(100)) {
            Ok(Some(request)) => {
                if let Err(request) = request_queue.push(request) {
                    let request = *request;
                    let _ = respond_error(request, StatusCode(503), "server busy");
                }
            }
            Ok(None) => continue,
            Err(err) => eprintln!("api server error: {err}"),
        }
    }
    request_queue.close();
    Ok(())
}

fn spawn_api_workers(
    matchmaker: Arc<Matchmaker>,
    request_queue: Arc<ApiRequestQueue>,
    shutdown: Arc<AtomicBool>,
    config: Arc<Config>,
) {
    for _ in 0..config.api_worker_count {
        let mm = matchmaker.clone();
        let queue = request_queue.clone();
        let sd = shutdown.clone();
        thread::spawn(move || loop {
            if sd.load(Ordering::Relaxed) {
                break;
            }
            match queue.pop() {
                Some(request) => {
                    if let Err(err) = handle_request(request, &mm) {
                        eprintln!("request error: {err}");
                    }
                }
                None => break,
            }
        });
    }
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

fn compute_match_result(players: &mut [Player], max_window: f64) -> MatchResult {
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
    let denom = (max_window * 2.0).max(1.0);
    let match_quality_score = (1.0 - (sr_delta / denom)).clamp(0.0, 1.0);

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
