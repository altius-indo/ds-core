//! dscore-harness: fault injection and verification for DS-CORE.
//!
//!   dscore-harness fault power-cut --trials 1000 --assert-zero-loss \
//!       [--writes 20] [--data-root DIR --lazyfs-fifo FIFO | --no-lazyfs]
//!       [--unsafe-no-fsync --expect-loss]
//!
//! See powercut.rs for the method. `node-group` is the child process it drives.

mod node_group;
mod powercut;

use std::path::PathBuf;
use std::process::ExitCode;

fn usage() -> ExitCode {
    eprintln!(
        "usage: dscore-harness fault power-cut --trials N [--writes N] [--assert-zero-loss]\n\
         \x20      [--data-root DIR --lazyfs-fifo FIFO | --no-lazyfs] [--unsafe-no-fsync --expect-loss]\n\
         \x20      dscore-harness --version"
    );
    ExitCode::from(2)
}

fn flag_value(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn has(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

async fn power_cut(args: &[String]) -> ExitCode {
    let trials = flag_value(args, "--trials")
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let writes = flag_value(args, "--writes")
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let lazyfs_fifo = flag_value(args, "--lazyfs-fifo").map(PathBuf::from);
    if lazyfs_fifo.is_none() && !has(args, "--no-lazyfs") {
        eprintln!(
            "power-cut needs --lazyfs-fifo (and --data-root on the LazyFS mount) to drop unsynced \
             data; pass --no-lazyfs to run a process-kill-only trial that cannot satisfy STORY-0001 E2"
        );
        return ExitCode::from(2);
    }
    let data_root = flag_value(args, "--data-root")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("dscore-powercut-{}", std::process::id()))
        });
    let opts = powercut::Options {
        trials,
        writes,
        data_root,
        lazyfs_fifo,
        lazyfs_done_fifo: flag_value(args, "--lazyfs-done-fifo").map(PathBuf::from),
        unsafe_no_fsync: has(args, "--unsafe-no-fsync"),
    };
    match powercut::run(&opts).await {
        Ok(o) => {
            println!(
                "power-cut: {} trials, {} acknowledged writes, {} lost ({})",
                o.trials,
                o.acknowledged,
                o.lost,
                if opts.lazyfs_fifo.is_some() {
                    "lazyfs clear-cache"
                } else {
                    "process kill only"
                }
            );
            if has(args, "--assert-zero-loss") && o.lost > 0 {
                eprintln!("FAIL: acknowledged writes were lost");
                return ExitCode::FAILURE;
            }
            if has(args, "--expect-loss") && o.lost == 0 {
                eprintln!(
                    "FAIL: expected losses with --unsafe-no-fsync; the harness is not detecting them"
                );
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("power-cut: {e}");
            ExitCode::FAILURE
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["--version"] | [] => {
            println!("dscore-harness {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        ["fault", "power-cut", ..] => power_cut(&args[2..]).await,
        ["node-group", ..] => {
            let Some(dir) = flag_value(&args, "--dir") else {
                return usage();
            };
            match node_group::run(&PathBuf::from(dir), has(&args, "--unsafe-no-fsync")).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("node-group: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        _ => usage(),
    }
}
