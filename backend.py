import json
import os
import time
from datetime import datetime
from shutil import disk_usage
from PySide6.QtCore import QThread, Signal

SESSION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "session_state.json")

MB = 1024 * 1024

class SectorWorker(QThread):
    """
    Single‑phase sector isolation worker.

    Writes full‑size dummy files while measuring write time.
    Squares go White -> Green (fast) or Red (slow / fail).
    """

    # Signal(list of (int, str)) — batched chunk updates
    chunk_status_batch = Signal(list)
    # Signal(int current, int total)
    progress_changed = Signal(int, int)
    # Signal(str level, str message)
    log_message = Signal(str, str)
    # Signal()
    work_finished = Signal()

    def __init__(self, disk_path, chunk_size_mb, threshold_s, parent=None):
        super().__init__(parent)
        self.disk_path = disk_path
        self.chunk_size_bytes = int(chunk_size_mb * MB)
        self.threshold_s = threshold_s

        self.total_chunks = 0
        # Each entry: {"index": int, "status": "white"|"green"|"red", "filename": str}
        self.chunks = []

        self._paused = False
        self._stopped = False

        self.current_chunk_index = 0  # next chunk to process

        # Batching
        self._batch = []
        self._last_flush_time = 0
        self._flush_interval = 0.1  # flush UI updates at most every 100ms

    # ------------------------------------------------------------------ state
    def save_state(self):
        state = {
            "disk_path": self.disk_path,
            "chunk_size_mb": self.chunk_size_bytes / MB,
            "threshold_s": self.threshold_s,
            "total_chunks": self.total_chunks,
            "current_chunk_index": self.current_chunk_index,
            "chunks": self.chunks,
        }
        try:
            with open(SESSION_FILE, "w") as f:
                json.dump(state, f, indent=2)
            self.log_message.emit('info', f"Session state saved ({len(self.chunks)} chunks)")
        except Exception as e:
            self.log_message.emit('error', f"Failed to save session state: {e}")

    @staticmethod
    def load_state():
        """Returns a dict with saved state or None."""
        if os.path.isfile(SESSION_FILE):
            try:
                with open(SESSION_FILE, "r") as f:
                    return json.load(f)
            except Exception:
                return None
        return None

    @staticmethod
    def clear_state():
        if os.path.isfile(SESSION_FILE):
            os.remove(SESSION_FILE)

    @classmethod
    def from_state(cls, state, parent=None):
        """Reconstruct a worker from saved state."""
        worker = cls(
            disk_path=state["disk_path"],
            chunk_size_mb=state["chunk_size_mb"],
            threshold_s=state["threshold_s"],
            parent=parent,
        )
        worker.total_chunks = state["total_chunks"]
        worker.chunks = state["chunks"]
        worker.current_chunk_index = state["current_chunk_index"]
        return worker

    # --------------------------------------------------------------- helpers
    def _sectors_dir(self):
        d = os.path.join(self.disk_path, "sectors")
        if not os.path.isdir(d):
            os.makedirs(d, exist_ok=True)
        return d

    def _chunk_filepath(self, chunk):
        return os.path.join(self._sectors_dir(), chunk["filename"])

    @staticmethod
    def _padded_name(index):
        return str(index).rjust(20, "0")

    def _wait_if_paused(self):
        while self._paused and not self._stopped:
            time.sleep(0.1)

    def _queue_status(self, index, status):
        """Queue a chunk status update and flush if enough time has passed."""
        self._batch.append((index, status))
        now = time.monotonic()
        if now - self._last_flush_time >= self._flush_interval:
            self._flush_batch()

    def _flush_batch(self):
        """Emit all queued status updates as a single signal."""
        if self._batch:
            self.chunk_status_batch.emit(list(self._batch))
            self._batch.clear()
            self._last_flush_time = time.monotonic()

    # ------------------------------------------------------------------- run
    def run(self):
        try:
            self._run_internal()
        except Exception as e:
            self.log_message.emit('error', f"Worker error: {e}")
        finally:
            self._flush_batch()
            self.save_state()
            self.work_finished.emit()

    def _run_internal(self):
        sectors_dir = self._sectors_dir()

        # ----- calculate total chunks if fresh start (with 0.5 % safety margin)
        if self.total_chunks == 0:
            free = disk_usage(self.disk_path).free
            # Reserve a small amount for filesystem metadata to avoid ENOSPC
            usable = int(free * 0.995)
            self.total_chunks = max(1, int(usable // self.chunk_size_bytes))
            self.chunks = []
            for i in range(self.total_chunks):
                self.chunks.append({
                    "index": i,
                    "status": "white",
                    "filename": f"{self._padded_name(i + 1)}.dat",
                })
            self.current_chunk_index = 0
            self.log_message.emit('info',
                f"Calculated {self.total_chunks} chunks "
                f"({self.chunk_size_bytes / MB:.1f} MB each) "
                f"from {free / MB:.1f} MB free space "
                f"(0.5 % reserved for metadata overhead)"
            )
            self.save_state()

        # Emit existing chunk states (for resume)
        resume_batch = []
        for chunk in self.chunks:
            if chunk["status"] != "white":
                resume_batch.append((chunk["index"], chunk["status"]))
        if resume_batch:
            self.chunk_status_batch.emit(resume_batch)

        # --------------------------------------------------- Single‑phase: write & time
        self.log_message.emit('info', "Writing and verifying chunks...")
        total_work = self.total_chunks

        for i in range(self.current_chunk_index, self.total_chunks):
            self._wait_if_paused()
            if self._stopped:
                self.current_chunk_index = i
                self._flush_batch()
                return

            chunk = self.chunks[i]

            # Skip chunks already done (green or red from a previous run)
            if chunk["status"] in ("green", "red"):
                self.progress_changed.emit(i + 1, total_work)
                continue

            filepath = self._chunk_filepath(chunk)

            try:
                # Write the full chunk in 1 MB blocks, measure total time
                start = datetime.now()
                with open(filepath, "wb") as f:
                    block_size = 1024 * 1024  # 1 MB
                    buf = b'\x00' * block_size
                    remaining = self.chunk_size_bytes
                    while remaining > 0:
                        to_write = min(block_size, remaining)
                        f.write(buf[:to_write])
                        remaining -= to_write
                    f.flush()
                    os.fsync(f.fileno())
                write_time = (datetime.now() - start).total_seconds()

                if write_time < self.threshold_s:
                    chunk["status"] = "green"
                    self._queue_status(i, "green")
                    # Rename to GOOD_ prefix
                    new_filename = f"GOOD_{chunk['filename']}"
                    new_filepath = os.path.join(self._sectors_dir(), new_filename)
                    try:
                        os.rename(filepath, new_filepath)
                        chunk["filename"] = new_filename
                    except Exception as rename_err:
                        self.log_message.emit('error', f"Could not rename chunk {i + 1}: {rename_err}")
                    self.log_message.emit('info',
                        f"GOOD chunk {i + 1}/{self.total_chunks} "
                        f"(write time: {write_time:.3f}s)"
                    )
                else:
                    chunk["status"] = "red"
                    self._queue_status(i, "red")
                    self.log_message.emit('info',
                        f"BAD chunk {i + 1}/{self.total_chunks} "
                        f"(write time: {write_time:.3f}s, threshold: {self.threshold_s}s)"
                    )
            except Exception as e:
                # Write failed – mark red, log, and continue
                chunk["status"] = "red"
                self._queue_status(i, "red")
                self.log_message.emit('warning',
                    f"FAILED chunk {i + 1}/{self.total_chunks}: {e}"
                )

            self.progress_changed.emit(i + 1, total_work)

        self._flush_batch()
        self.log_message.emit('info', "Verification complete!")

        # Final summary
        good = sum(1 for c in self.chunks if c["status"] == "green")
        bad = sum(1 for c in self.chunks if c["status"] == "red")
        self.log_message.emit('info', f"Summary: {good} GOOD, {bad} BAD out of {self.total_chunks}")

    # --------------------------------------------------------------- controls
    def pause(self):
        self._paused = True
        self.log_message.emit('info', "Paused")

    def resume(self):
        self._paused = False
        self.log_message.emit('info', "Resumed")

    def stop(self):
        self._stopped = True
        self._paused = False
        self.log_message.emit('info', "Stopping...")