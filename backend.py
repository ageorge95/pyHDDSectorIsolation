import ctypes
import json
import os
import threading
import time
import win32file
import win32con
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

        self._max_write_attempts = 2
        self._retry_delay = 2  # seconds between retries, lets USB cache flush
        self._max_recovery_wait = 30  # max seconds to wait for drive to recover after interrupted write

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

    def _open_write_through(self, filepath):
        import msvcrt
        handle = win32file.CreateFile(
            filepath,
            win32con.GENERIC_WRITE,
            0,
            None,
            win32con.CREATE_ALWAYS,
            win32con.FILE_FLAG_WRITE_THROUGH,
            None,
        )
        h_int = int(handle)
        raw_handle = handle.Detach()
        fd = msvcrt.open_osfhandle(raw_handle, os.O_BINARY)
        return os.fdopen(fd, "wb", 1024 * 1024), h_int

    @staticmethod
    def _write_chunk_thread(f, chunk_size_bytes, result_dict):
        """
        Thread target: write full chunk in 1 MB blocks, flush, fsync.

        Stores outcome in *result_dict*:
          completed, write_time, sync_time, error.
        Any exception (including handle closed from another thread)
        is caught and stored rather than raised.
        """
        try:
            block_size = 1024 * 1024
            buf = b'\x00' * block_size

            write_start = time.monotonic()
            remaining = chunk_size_bytes
            while remaining > 0:
                to_write = min(block_size, remaining)
                f.write(buf[:to_write])
                remaining -= to_write
            f.flush()
            write_time = time.monotonic() - write_start

            sync_start = time.monotonic()
            os.fsync(f.fileno())
            sync_time = time.monotonic() - sync_start

            result_dict['completed'] = True
            result_dict['write_time'] = write_time
            result_dict['sync_time'] = sync_time
            result_dict['error'] = None
        except Exception as e:
            result_dict['completed'] = False
            result_dict['write_time'] = 0.0
            result_dict['sync_time'] = 0.0
            result_dict['error'] = str(e)

    @staticmethod
    def _safe_close_handle(h_int, f):
        """Close the Win32 handle (interrupts pending I/O) then the Python file object."""
        try:
            ctypes.windll.kernel32.CloseHandle(h_int)
        except Exception:
            pass
        try:
            f.close()
        except Exception:
            pass

    def _allocate_file_space(self, filepath, size_bytes):
        """
        Ensure *filepath* occupies exactly *size_bytes* on disk.

        Uses SetFilePointerEx + SetEndOfFile — NTFS allocates the clusters
        in the MFT without physically writing data to them.  Reading the
        file returns zeros but the platters are never touched.
        If the file already exists it is extended in-place; otherwise it
        is created fresh (without WRITE_THROUGH).
        """
        if os.path.exists(filepath):
            disposition = win32con.OPEN_EXISTING
        else:
            disposition = win32con.CREATE_ALWAYS

        handle = win32file.CreateFile(
            filepath,
            win32con.GENERIC_WRITE,
            0,
            None,
            disposition,
            0,  # no WRITE_THROUGH — filesystem metadata only
            None,
        )
        h_int = int(handle)
        handle.Detach()
        ctypes.windll.kernel32.SetFilePointerEx(h_int, size_bytes, None, 0)
        ctypes.windll.kernel32.SetEndOfFile(h_int)
        ctypes.windll.kernel32.CloseHandle(h_int)

    def _wait_for_drive_ready(self, sectors_dir):
        """
        Block until the drive responds to a tiny probe write, or *max_recovery_wait*
        seconds elapse.

        Uses a plain file write (not WRITE_THROUGH) so that the probe itself
        is less likely to trigger another firmware-level stall.

        Returns True if the drive became ready, False if it did not.
        """
        deadline = time.monotonic() + self._max_recovery_wait
        while time.monotonic() < deadline:
            probe = {'completed': False, 'error': None}
            probe_handle = {'h_int': None, 'f': None}
            probe_path = os.path.join(sectors_dir, "_probe_.dat")

            def _probe():
                try:
                    f, h_int = self._open_write_through(probe_path)
                    probe_handle['h_int'] = h_int
                    probe_handle['f'] = f
                    f.write(b'\x00')
                    f.flush()
                    os.fsync(f.fileno())
                    f.close()
                    probe_handle['f'] = None
                    os.remove(probe_path)
                    probe['completed'] = True
                except Exception as e:
                    probe['error'] = str(e)
                    try:
                        if os.path.exists(probe_path):
                            os.remove(probe_path)
                    except Exception:
                        pass

            t = threading.Thread(target=_probe, daemon=True)
            t.start()
            t.join(timeout=2.0)
            if t.is_alive():
                self._safe_close_handle(probe_handle['h_int'], probe_handle['f'])
                self.log_message.emit('info',
                    "Drive still busy after interrupted write — waiting for firmware recovery..."
                )
                t.join(timeout=1.0)
                time.sleep(1)
                continue

            if probe['completed']:
                return True
            self.log_message.emit('info',
                f"Drive probe failed ({probe['error']}), retrying..."
            )
            time.sleep(1)

        self.log_message.emit('warning',
            f"Drive did not recover within {self._max_recovery_wait}s"
        )
        return False

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

            passed = False
            for attempt in range(self._max_write_attempts):
                f = None
                h_int = None
                result = {'completed': False, 'write_time': 0.0, 'sync_time': 0.0, 'error': None}

                try:
                    f, h_int = self._open_write_through(filepath)

                    write_thread = threading.Thread(
                        target=self._write_chunk_thread,
                        args=(f, self.chunk_size_bytes, result),
                        daemon=True,
                    )
                    write_thread.start()
                    write_thread.join(timeout=self.threshold_s)

                    if write_thread.is_alive():
                        # --- TIMEOUT: write exceeded threshold, interrupt it ---
                        self._safe_close_handle(h_int, f)
                        f = None
                        h_int = None
                        write_thread.join(timeout=5.0)

                        # Wait for drive firmware to stop retrying before next I/O
                        if not self._wait_for_drive_ready(sectors_dir):
                            chunk["status"] = "red"
                            self._queue_status(i, "red")
                            self.log_message.emit('warning',
                                f"Drive unresponsive — marking chunk {i + 1}/{self.total_chunks} as BAD"
                            )
                            break

                        if attempt < self._max_write_attempts - 1:
                            if os.path.exists(filepath):
                                try:
                                    os.remove(filepath)
                                except Exception:
                                    pass
                            time.sleep(self._retry_delay)
                            self.log_message.emit('info',
                                f"Retrying chunk {i + 1}/{self.total_chunks} "
                                f"(attempt {attempt + 1}: write exceeded {self.threshold_s}s threshold)"
                            )
                            continue
                        else:
                            chunk["status"] = "red"
                            self._queue_status(i, "red")
                            try:
                                self._allocate_file_space(filepath, self.chunk_size_bytes)
                            except Exception:
                                pass
                            self.log_message.emit('info',
                                f"BAD chunk {i + 1}/{self.total_chunks} "
                                f"(all {self._max_write_attempts} attempts exceeded "
                                f"{self.threshold_s}s threshold)"
                            )

                    else:
                        # --- COMPLETED: thread finished within threshold ---
                        f.close()
                        f = None

                        if result['completed']:
                            total_time = result['write_time'] + result['sync_time']

                            if total_time < self.threshold_s:
                                chunk["status"] = "green"
                                self._queue_status(i, "green")
                                new_filename = f"GOOD_{chunk['filename']}"
                                new_filepath = os.path.join(sectors_dir, new_filename)
                                try:
                                    os.rename(filepath, new_filepath)
                                    chunk["filename"] = new_filename
                                except Exception as rename_err:
                                    self.log_message.emit('error', f"Could not rename chunk {i + 1}: {rename_err}")
                                self.log_message.emit('info',
                                    f"GOOD chunk {i + 1}/{self.total_chunks} "
                                    f"(write: {result['write_time']:.3f}s  sync: {result['sync_time']:.3f}s  "
                                    f"total: {total_time:.3f}s)"
                                )
                                passed = True
                                break
                            else:
                                if attempt < self._max_write_attempts - 1:
                                    if os.path.exists(filepath):
                                        os.remove(filepath)
                                    time.sleep(self._retry_delay)
                                    self.log_message.emit('info',
                                        f"Retrying chunk {i + 1}/{self.total_chunks} "
                                        f"(attempt {attempt + 1}: total {total_time:.3f}s >= {self.threshold_s}s)"
                                    )
                                    continue
                                else:
                                    chunk["status"] = "red"
                                    self._queue_status(i, "red")
                                    self.log_message.emit('info',
                                        f"BAD chunk {i + 1}/{self.total_chunks} "
                                        f"(total {total_time:.3f}s >= {self.threshold_s}s)"
                                    )
                        else:
                            # Thread completed with an exception
                            if os.path.exists(filepath):
                                try:
                                    os.remove(filepath)
                                except Exception:
                                    pass
                            if attempt < self._max_write_attempts - 1:
                                time.sleep(self._retry_delay)
                                self.log_message.emit('info',
                                    f"Retrying chunk {i + 1}/{self.total_chunks} "
                                    f"(attempt {attempt + 1} failed: {result['error']})"
                                )
                            else:
                                chunk["status"] = "red"
                                self._queue_status(i, "red")
                                try:
                                    self._allocate_file_space(filepath, self.chunk_size_bytes)
                                except Exception:
                                    pass
                                self.log_message.emit('warning',
                                    f"FAILED chunk {i + 1}/{self.total_chunks}: {result['error']}"
                                )

                except Exception as e:
                    if os.path.exists(filepath):
                        try:
                            os.remove(filepath)
                        except Exception:
                            pass
                    if attempt < self._max_write_attempts - 1:
                        time.sleep(self._retry_delay)
                        self.log_message.emit('info',
                            f"Retrying chunk {i + 1}/{self.total_chunks} "
                            f"(attempt {attempt + 1} failed: {e})"
                        )
                    else:
                        chunk["status"] = "red"
                        self._queue_status(i, "red")
                        try:
                            self._allocate_file_space(filepath, self.chunk_size_bytes)
                        except Exception:
                            pass
                        self.log_message.emit('warning',
                            f"FAILED chunk {i + 1}/{self.total_chunks}: {e}"
                        )
                finally:
                    if f is not None:
                        try:
                            f.close()
                        except Exception:
                            pass

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