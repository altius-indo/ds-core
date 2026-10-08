//! `fault power-cut`: acknowledged writes must survive every node losing power at once
//! (REQ-0016 AC1, STORY-0001 E2).
//!
//! Each trial runs a 3-voter group in a child process (`node-group`) whose data lives on a
//! LazyFS mount. The parent writes keys and records which were acknowledged; immediately
//! after the last acknowledgment it kills the child with SIGKILL and tells LazyFS to drop
//! every byte that was not fsynced (`lazyfs::clear-cache`), which is what a simultaneous
//! power loss does to a page cache. It then restarts the group on the same data and checks
//! that every acknowledged key is present.
//!
//! Without LazyFS (`--no-lazyfs`) only the process is killed; the OS page cache survives, so
//! that mode cannot catch a missing fsync and does not satisfy E2.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

pub struct Options {
    pub trials: usize,
    pub writes: usize,
    pub data_root: PathBuf,
    pub lazyfs_fifo: Option<PathBuf>,
    /// LazyFS `fifo_path_completed`: it writes `finished::clear-cache` there when done.
    pub lazyfs_done_fifo: Option<PathBuf>,
    pub unsafe_no_fsync: bool,
}

pub struct Outcome {
    pub trials: usize,
    pub acknowledged: usize,
    pub lost: usize,
}

struct Group {
    child: Child,
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
}

const STEP_TIMEOUT: Duration = Duration::from_secs(30);

impl Group {
    async fn start(dir: &Path, unsafe_no_fsync: bool) -> Result<Self, String> {
        let exe = std::env::current_exe().map_err(|e| e.to_string())?;
        let mut cmd = Command::new(exe);
        cmd.arg("node-group").arg("--dir").arg(dir);
        if unsafe_no_fsync {
            cmd.arg("--unsafe-no-fsync");
        }
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("spawn node-group: {e}"))?;
        let stdin = child.stdin.take().expect("piped stdin");
        let lines = BufReader::new(child.stdout.take().expect("piped stdout")).lines();
        let mut g = Self {
            child,
            stdin,
            lines,
        };
        match g.read().await?.as_str() {
            "ready" => Ok(g),
            other => Err(format!("node-group did not start: {other}")),
        }
    }

    async fn read(&mut self) -> Result<String, String> {
        match tokio::time::timeout(STEP_TIMEOUT, self.lines.next_line()).await {
            Ok(Ok(Some(line))) => Ok(line),
            Ok(Ok(None)) => Err("node-group exited".into()),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err("node-group timed out".into()),
        }
    }

    async fn request(&mut self, line: &str) -> Result<String, String> {
        self.stdin
            .write_all(format!("{line}\n").as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        self.read().await
    }

    /// Power cut: SIGKILL, no shutdown path runs.
    async fn kill(mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }

    async fn quit(mut self) {
        let _ = self.request("quit").await;
        let _ = self.child.wait().await;
    }
}

/// Drop every unsynced byte on the LazyFS mount and wait until LazyFS reports it is done, so
/// the restart cannot race the fault.
async fn clear_cache(fifo: &Path, done: Option<&Path>) -> Result<(), String> {
    let fifo = fifo.to_path_buf();
    let done = done.map(Path::to_path_buf);
    let work = tokio::task::spawn_blocking(move || -> Result<(), String> {
        use std::io::BufRead;
        // Open the completion FIFO first: LazyFS writes to it as soon as the fault finishes.
        let reader = match &done {
            Some(d) => Some(std::io::BufReader::new(
                std::fs::File::open(d).map_err(|e| format!("{}: {e}", d.display()))?,
            )),
            None => None,
        };
        std::fs::write(&fifo, b"lazyfs::clear-cache\n")
            .map_err(|e| format!("{}: {e}", fifo.display()))?;
        if let Some(mut r) = reader {
            let mut line = String::new();
            loop {
                line.clear();
                if r.read_line(&mut line).map_err(|e| e.to_string())? == 0 {
                    return Err("lazyfs completion fifo closed".into());
                }
                if line.contains("clear-cache") {
                    return Ok(());
                }
            }
        }
        Ok(())
    });
    match tokio::time::timeout(STEP_TIMEOUT, work).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("lazyfs clear-cache did not complete".into()),
    }
}

pub async fn run(opts: &Options) -> Result<Outcome, String> {
    let mut out = Outcome {
        trials: 0,
        acknowledged: 0,
        lost: 0,
    };
    for trial in 0..opts.trials {
        let dir = opts.data_root.join(format!("trial-{trial}"));
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

        let mut group = Group::start(&dir, opts.unsafe_no_fsync).await?;
        let mut acked = Vec::new();
        for i in 0..opts.writes {
            let key = format!("t{trial}-k{i}");
            let reply = group.request(&format!("put {key} v{i}")).await?;
            if reply == format!("ack {key}") {
                acked.push((key, format!("v{i}")));
            }
        }
        // Power cut immediately after the last acknowledgment.
        group.kill().await;
        if let Some(fifo) = &opts.lazyfs_fifo {
            clear_cache(fifo, opts.lazyfs_done_fifo.as_deref()).await?;
        }

        let mut group = Group::start(&dir, opts.unsafe_no_fsync).await?;
        let mut lost = 0;
        for (key, value) in &acked {
            let reply = group.request(&format!("get {key}")).await?;
            if reply != format!("val {key} {value}") {
                lost += 1;
                eprintln!(
                    "trial {trial}: acknowledged {key}={value} lost after power cut ({reply})"
                );
            }
        }
        group.quit().await;
        let _ = std::fs::remove_dir_all(&dir);

        out.trials += 1;
        out.acknowledged += acked.len();
        out.lost += lost;
        if (trial + 1) % 50 == 0 {
            println!(
                "power-cut: {} trials, {} acknowledged, {} lost",
                out.trials, out.acknowledged, out.lost
            );
        }
    }
    Ok(out)
}
