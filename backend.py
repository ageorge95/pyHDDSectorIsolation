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
    Two-phase sector isolation worker.

    Phase 1 (allocation): Creates dummy files (seek + single byte) to reserve
    all available disk space. Squares go White -> Yellow.

    Phase 2 (verification): Overwrites each dummy file with a real write
    (truncate) and measures write time. Squares go Yellow -> Green or Red.
    """

    # Signal(list of (int, str)) — batched chunk updates
    chunk_status_batch = Signal(list)
    # Signal(int current, int total)
    progress_changed = Signal(int, int)
    # Signal(str message)
    log_message = Signal(str)
    # Signal()
    work_finished = Signal()

    def __init__(self, disk_path, chunk_size_mb, threshold_s, parent=None):
        super().__init__(parent)
        self.disk_path = disk_path
        self.chunk_size_bytes = int(chunk_size_mb * MB)
        self.threshold_s = threshold_s

        self.total_chunks = 0
        # Each entry: {"index": int, "status": "white"|"yellow"|"green"|"red", "filename": str}
        self.chunks = []

        self._paused = False
        self._stopped = False

        # Which phase we are in and where we left off
        self.current_phase = 1  # 1 = allocation, 2 = verification
        self.current_chunk_index = 0  # next chunk to process in current phase

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
            "current_phase": self.current_phase,
            "current_chunk_index": self.current_chunk_index,
            "chunks": self.chunks,
        }
        try:
            with open(SESSION_FILE, "w") as f:
                json.dump(state, f, indent=2)
            self.log_message.emit(f"Session state saved ({len(self.chunks)} chunks)")
        except Exception as e:
            self.log_message.emit(f"Failed to save session state: {e}")

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
        worker.current_phase = state["current_phase"]
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
            self.log_message.emit(f"Worker error: {e}")
        finally:
            self._flush_batch()
            self.save_state()
            self.work_finished.emit()

    def _run_internal(self):
        sectors_dir = self._sectors_dir()

        # ------------------------------------------------------ calculate total chunks if fresh start
        if self.total_chunks == 0:
            free = disk_usage(self.disk_path).free
            # Reserve one chunk worth of space to avoid running out during allocation
            # due to filesystem metadata overhead
            usable = free
            self.total_chunks = max(1, int(usable // self.chunk_size_bytes))
            self.chunks = []
            for i in range(self.total_chunks):
                self.chunks.append({
                    "index": i,
                    "status": "white",
                    "filename": f"{self._padded_name(i + 1)}.dat",
                })
            self.current_phase = 1
            self.current_chunk_index = 0
            self.log_message.emit(
                f"Calculated {self.total_chunks} chunks "
                f"({self.chunk_size_bytes / MB:.1f} MB each) "
                f"for {free / MB:.1f} MB free space "
                f"(reserving {self.chunk_size_bytes / MB:.1f} MB for filesystem overhead)"
            )
            self.save_state()

        # Emit existing chunk states (for resume)
        total_work = self.total_chunks * 2  # phase1 + phase2
        resume_batch = []
        for chunk in self.chunks:
            if chunk["status"] != "white":
                resume_batch.append((chunk["index"], chunk["status"]))
        if resume_batch:
            self.chunk_status_batch.emit(resume_batch)

        # ------------------------------------------------------ Phase 1: Allocation
        if self.current_phase == 1:
            self.log_message.emit("Phase 1: Allocating dummy files...")
            for i in range(self.current_chunk_index, self.total_chunks):
                self._wait_if_paused()
                if self._stopped:
                    self.current_chunk_index = i
                    self._flush_batch()
                    return

                chunk = self.chunks[i]
                filepath = self._chunk_filepath(chunk)

                try:
                    with open(filepath, "wb") as f:
                        f.seek(self.chunk_size_bytes - 1)
                        f.write(b"\0")
                    chunk["status"] = "yellow"
                    self._queue_status(i, "yellow")
                    self.log_message.emit(f"Allocated chunk {i + 1}/{self.total_chunks}")
                except Exception as e:
                    # If we can't even allocate, mark red immediately
                    chunk["status"] = "red"
                    self._queue_status(i, "red")
                    self.log_message.emit(f"Failed to allocate chunk {i + 1}: {e}")

                completed = i + 1
                self.progress_changed.emit(completed, total_work)

                # Save state periodically (every 50 chunks)
                if (i + 1) % 50 == 0:
                    self.current_chunk_index = i + 1
                    self.save_state()

            # Phase 1 complete
            self._flush_batch()
            self.current_phase = 2
            self.current_chunk_index = 0
            self.save_state()

        # ------------------------------------------------------ Phase 2: Verification
        if self.current_phase == 2:
            self.log_message.emit("Phase 2: Verifying sectors with real writes...")
            for i in range(self.current_chunk_index, self.total_chunks):
                self._wait_if_paused()
                if self._stopped:
                    self.current_chunk_index = i
                    self._flush_batch()
                    return

                chunk = self.chunks[i]

                # Skip chunks that already failed allocation
                if chunk["status"] == "red":
                    completed = self.total_chunks + i + 1
                    self.progress_changed.emit(completed, total_work)
                    continue

                filepath = self._chunk_filepath(chunk)

                try:
                    start = datetime.now()
                    # Overwrite the existing file in‑place — never truncate
                    with open(filepath, "r+b") as f:
                        data = b'\x00' * self.chunk_size_bytes  # one full chunk_size_bytes buffer
                        f.write(data)
                        f.flush()
                        os.fsync(f.fileno())  # ensure data reaches disk
                    write_time = (datetime.now() - start).total_seconds()

                    if write_time < self.threshold_s:
                        chunk["status"] = "green"
                        self._queue_status(i, "green")
                        # Rename file to GOOD_ prefix
                        new_filename = f"GOOD_{chunk['filename']}"
                        new_filepath = os.path.join(self._sectors_dir(), new_filename)
                        try:
                            os.rename(filepath, new_filepath)
                            chunk["filename"] = new_filename
                        except Exception as rename_err:
                            self.log_message.emit(f"Could not rename chunk {i + 1}: {rename_err}")
                        self.log_message.emit(
                            f"GOOD chunk {i + 1}/{self.total_chunks} "
                            f"(write time: {write_time:.3f}s)"
                        )
                    else:
                        chunk["status"] = "red"
                        self._queue_status(i, "red")
                        self.log_message.emit(
                            f"BAD chunk {i + 1}/{self.total_chunks} "
                            f"(write time: {write_time:.3f}s, threshold: {self.threshold_s}s)"
                            # File remains intact → space stays allocated
                        )
                except Exception as e:
                    # Write failed, but the file was never truncated → space is still occupied
                    chunk["status"] = "red"
                    self._queue_status(i, "red")
                    self.log_message.emit(
                        f"FAILED chunk {i + 1}/{self.total_chunks}: {e}"
                    )
                    # No need to recreate a dummy; the original file already reserves the space

                completed = self.total_chunks + i + 1
                self.progress_changed.emit(completed, total_work)

                # Save state periodically (every 50 chunks)
                if (i + 1) % 50 == 0:
                    self.current_chunk_index = i + 1
                    self.save_state()

            self._flush_batch()
            self.log_message.emit("Verification complete!")

            # Final summary
            good = sum(1 for c in self.chunks if c["status"] == "green")
            bad = sum(1 for c in self.chunks if c["status"] == "red")
            self.log_message.emit(f"Summary: {good} GOOD, {bad} BAD out of {self.total_chunks}")

    # --------------------------------------------------------------- controls
    def pause(self):
        self._paused = True
        self.log_message.emit("Paused")

    def resume(self):
        self._paused = False
        self.log_message.emit("Resumed")

    def stop(self):
        self._stopped = True
        self._paused = False
        self.log_message.emit("Stopping...")