//! Background compaction thread.
//!
//! Compaction runs on its own thread, decoupled from the flush worker, so a
//! long merge never blocks flushes from publishing new SSTables. The thread is
//! channel-triggered (a flush sends [`CompactMsg::Run`] after publishing) and
//! shuts down cleanly when the [`Db`](crate::Db) is dropped.
//!
//! It holds only a `Weak` reference to the engine state, so it never keeps the
//! database alive on its own.

use std::sync::mpsc::Receiver;
use std::sync::Weak;

use crate::db::DbInner;

/// A message to the compaction thread.
pub(crate) enum CompactMsg {
    /// Run compaction until no qualifying run of SSTables remains.
    Run,
    /// Stop the thread.
    Shutdown,
}

/// The compaction thread's main loop: drains triggers until shutdown.
pub(crate) fn compaction_loop(rx: Receiver<CompactMsg>, inner: Weak<DbInner>) {
    while let Ok(msg) = rx.recv() {
        match msg {
            CompactMsg::Shutdown => break,
            CompactMsg::Run => {
                let Some(inner) = inner.upgrade() else { break };
                loop {
                    match inner.compact_step() {
                        Ok(true) => continue,
                        Ok(false) => break,
                        Err(e) => {
                            eprintln!("lsm: background compaction failed: {e}");
                            break;
                        }
                    }
                }
            }
        }
    }
}
