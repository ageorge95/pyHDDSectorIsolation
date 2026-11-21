import ctypes
import hashlib
import json
import logging
import os
import sys
import tempfile
import threading
import time
from shutil import disk_usage

import win32con
import win32file
from PySide6.QtCore import QThread, Signal, Slot
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from ag95 import configure_logger

MB = 1024 * 1024
ICON_FILE = "icon.ico"
VERSION_FILE = "version.txt"
STAGE1_FILE = "stage1.json"
STAGE2_FILE = "stage2.json"
APP_TITLE = "Sector Stage Runner"
APP_ID = "AG95.pyHDDSectorIsolation.module1"


def get_running_path(relative_path):
    """Return a bundled file's path in both source and PyInstaller builds."""
    base_path = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)


def get_app_root():
    """Return the folder that contains the exe (or module1.py when run from source)."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


def check_app_root_writable():
    """Raise a clear OSError if stage JSON files cannot be written in the app root."""
    app_root = get_app_root()
    probe_path = None
    try:
        fd, probe_path = tempfile.mkstemp(prefix=".stage_write_test_", dir=app_root)
        with os.fdopen(fd, "w", encoding="utf-8") as file_object:
            file_object.write("test")
    except OSError as exc:
        raise OSError(
            "Cannot write the stage JSON files to the app folder:\n"
            f"{app_root}\n\n{exc}"
        ) from exc
    finally:
        if probe_path is not None:
            try:
                os.remove(probe_path)
            except OSError:
                pass


def read_version():
    try:
        with open(get_running_path(VERSION_FILE), "r", encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return ""


def pattern_byte(block_index):
    """Deterministic byte value per block so the written payload can be hashed."""
    return (block_index * 31 + 7) % 256


def pattern_block(block_index, size_bytes):
    return bytes([pattern_byte(block_index)]) * size_bytes


def compute_expected_checksum(chunk_size_bytes):
    """SHA-256 of the deterministic payload written for one chunk."""
    digest = hashlib.sha256()
    remaining = chunk_size_bytes
    block_index = 0
    while remaining > 0:
        to_hash = min(MB, remaining)
        digest.update(pattern_block(block_index, to_hash))
        remaining -= to_hash
        block_index += 1
    return digest.hexdigest()


class StageWorker(QThread):
    """Write every chunk, then read every non-ghost chunk and verify its hash."""

    log_message = Signal(str, str)
    progress_changed = Signal(int, int)
    stage_changed = Signal(str)
    work_finished = Signal(object)

    def __init__(self, disk_path, chunk_size_mb, parent=None):
        super().__init__(parent)
        self.disk_path = disk_path
        self.chunk_size_bytes = int(float(chunk_size_mb) * MB)
        self.total_chunks = 0
        self.expected_checksum = None
        self._paused = False
        self._error = None
        self.results = {
            "total": 0,
            "ghost_count": 0,
            "read_failure_count": 0,
            "hash_mismatch_count": 0,
            "stage1_path": "",
            "stage2_path": "",
            "error": None,
        }

    # --------------------------------------------------------------- helpers
    def _sectors_dir(self):
        sectors_dir = os.path.join(self.disk_path, "sectors")
        os.makedirs(sectors_dir, exist_ok=True)
        return sectors_dir

    def _wait_if_paused(self):
        while self._paused:
            time.sleep(0.1)

    @staticmethod
    def _open_write_through(filepath):
        """Open a file with FILE_FLAG_WRITE_THROUGH and return (file, handle_int)."""
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
        handle_int = int(handle)
        raw_handle = handle.Detach()
        fd = msvcrt.open_osfhandle(raw_handle, os.O_BINARY)
        return os.fdopen(fd, "wb", MB), handle_int

    @staticmethod
    def _open_read_no_buffer(filepath):
        """Open a file with FILE_FLAG_NO_BUFFERING for reads from the platters."""
        import msvcrt

        handle = win32file.CreateFile(
            filepath,
            win32con.GENERIC_READ,
            0,
            None,
            win32con.OPEN_EXISTING,
            win32con.FILE_FLAG_NO_BUFFERING,
            None,
        )
        raw_handle = handle.Detach()
        fd = msvcrt.open_osfhandle(raw_handle, os.O_RDONLY)
        return os.fdopen(fd, "rb")

    @staticmethod
    def _allocate_file_space(filepath, size_bytes):
        """Allocate size_bytes on disk without writing the payload."""
        disposition = (
            win32con.OPEN_EXISTING if os.path.exists(filepath) else win32con.CREATE_ALWAYS
        )
        handle = win32file.CreateFile(
            filepath,
            win32con.GENERIC_WRITE,
            0,
            None,
            disposition,
            0,
            None,
        )
        handle_int = int(handle)
        handle.Detach()
        try:
            if not ctypes.windll.kernel32.SetFilePointerEx(
                handle_int, ctypes.c_longlong(size_bytes), None, 0
            ):
                raise ctypes.WinError()
            if not ctypes.windll.kernel32.SetEndOfFile(handle_int):
                raise ctypes.WinError()
        finally:
            ctypes.windll.kernel32.CloseHandle(handle_int)

    # --------------------------------------------------------------- write/read
    def _write_chunk(self, filepath):
        """Write one chunk and return the time from first write to flush end."""
        file_object = None
        try:
            file_object, _handle_int = self._open_write_through(filepath)
            write_start = time.monotonic()
            remaining = self.chunk_size_bytes
            block_index = 0
            while remaining > 0:
                to_write = min(MB, remaining)
                file_object.write(pattern_block(block_index, to_write))
                remaining -= to_write
                block_index += 1
            file_object.flush()
            write_time = time.monotonic() - write_start
            os.fsync(file_object.fileno())
            file_object.close()
            file_object = None
            return write_time
        finally:
            if file_object is not None:
                try:
                    file_object.close()
                except Exception:
                    pass

    def _read_chunk(self, filepath):
        """Read one chunk and return (read_time, sha256_hex)."""
        file_object = None
        try:
            file_object = self._open_read_no_buffer(filepath)
            digest = hashlib.sha256()
            read_start = time.monotonic()
            remaining = self.chunk_size_bytes
            while remaining > 0:
                to_read = min(MB, remaining)
                data = file_object.read(to_read)
                got = 0 if data is None else len(data)
                if got != to_read:
                    raise IOError(
                        f"Short read: expected {to_read} bytes, got {got}"
                    )
                digest.update(data)
                remaining -= to_read
            read_time = time.monotonic() - read_start
            return read_time, digest.hexdigest()
        finally:
            if file_object is not None:
                try:
                    file_object.close()
                except Exception:
                    pass

    # --------------------------------------------------------------- drive probe
    def _probe_once(self, sectors_dir):
        """Try one tiny write-through probe. Returns (ok, error_text)."""
        probe_path = os.path.join(sectors_dir, "_probe_.dat")
        result = {"completed": False, "error": None}
        holder = {"handle": None, "force_closed": False}

        def _probe():
            handle = None
            try:
                handle = win32file.CreateFile(
                    probe_path,
                    win32con.GENERIC_WRITE,
                    0,
                    None,
                    win32con.CREATE_ALWAYS,
                    win32con.FILE_FLAG_WRITE_THROUGH,
                    None,
                )
                holder["handle"] = handle
                win32file.WriteFile(handle, b"\x00")
                win32file.FlushFileBuffers(handle)
                handle.Close()
                handle = None
                holder["handle"] = None
                try:
                    os.remove(probe_path)
                except OSError:
                    pass
                result["completed"] = True
            except Exception as exc:
                result["error"] = str(exc)
            finally:
                if handle is not None:
                    try:
                        if holder["force_closed"]:
                            # The waiting thread already closed the raw handle.
                            handle.Detach()
                        else:
                            handle.Close()
                    except Exception:
                        pass
                holder["handle"] = None
                try:
                    if os.path.exists(probe_path):
                        os.remove(probe_path)
                except OSError:
                    pass

        thread = threading.Thread(target=_probe, daemon=True)
        thread.start()
        thread.join(timeout=2.0)
        if thread.is_alive():
            holder["force_closed"] = True
            handle = holder["handle"]
            if handle is not None:
                try:
                    handle.Close()
                except Exception:
                    pass
            thread.join(timeout=1.0)
            return False, "probe timed out"
        return result["completed"], result["error"]

    def _wait_for_drive_ready(self, sectors_dir):
        """Block until the drive answers a probe. No deadline."""
        self.log_message.emit("info", "Waiting for the drive to become ready...")
        while True:
            ok, error = self._probe_once(sectors_dir)
            if ok:
                self.log_message.emit("info", "Drive is ready.")
                return
            if error == "probe timed out":
                self.log_message.emit(
                    "info",
                    "Drive still busy after interrupted write - waiting for "
                    "firmware recovery...",
                )
            else:
                self.log_message.emit(
                    "info", f"Drive probe failed ({error}), retrying..."
                )
            time.sleep(1)

    # --------------------------------------------------------------- stages
    def _run_stage1(self, sectors_dir, total, total_work):
        entries = []
        for index0 in range(total):
            self._wait_if_paused()
            index = index0 + 1
            filename = f"{str(index).rjust(20, '0')}.dat"
            filepath = os.path.join(sectors_dir, filename)
            self.log_message.emit(
                "info", f"Stage 1 [{index}/{total}] writing {filename}..."
            )
            try:
                write_time = self._write_chunk(filepath)
                speed = (
                    self.chunk_size_bytes / write_time / MB
                    if write_time > 0
                    else 0.0
                )
                entries.append(
                    {
                        "file": filename,
                        "write_time_s": write_time,
                        "write_speed_mbps": speed,
                        "ghost_status": 0,
                    }
                )
                self.log_message.emit(
                    "info",
                    f"Stage 1 [{index}/{total}] OK {filename}: "
                    f"{write_time:.3f} s, {speed:.1f} MB/s",
                )
            except Exception as exc:
                self.log_message.emit(
                    "warning",
                    f"Stage 1 [{index}/{total}] GHOST {filename}: {exc}",
                )
                self._wait_for_drive_ready(sectors_dir)
                try:
                    self._allocate_file_space(filepath, self.chunk_size_bytes)
                    self.log_message.emit(
                        "warning",
                        f"Stage 1 [{index}/{total}] ghost file allocated: {filename}",
                    )
                except Exception as alloc_error:
                    self.log_message.emit(
                        "error",
                        f"Stage 1 [{index}/{total}] could not allocate ghost file "
                        f"{filename}: {alloc_error}",
                    )
                entries.append(
                    {
                        "file": filename,
                        "write_time_s": -1,
                        "write_speed_mbps": -1,
                        "ghost_status": 1,
                    }
                )
            self.progress_changed.emit(index, total_work)
        return entries

    def _run_stage2(self, sectors_dir, stage1_entries, total_work):
        self.log_message.emit(
            "info", "Computing expected SHA-256 for read verification..."
        )
        self.expected_checksum = compute_expected_checksum(self.chunk_size_bytes)
        entries = []
        total = len(stage1_entries)
        for index0, stage1_entry in enumerate(stage1_entries):
            self._wait_if_paused()
            index = index0 + 1
            filename = stage1_entry["file"]
            if stage1_entry["ghost_status"] == 1:
                entries.append(
                    {
                        "file": filename,
                        "read_time_s": -1,
                        "read_speed_mbps": -1,
                        "ghost_status": 1,
                        "hash_ok": None,
                    }
                )
                self.log_message.emit(
                    "info", f"Stage 2 [{index}/{total}] skipped ghost {filename}"
                )
                self.progress_changed.emit(total + index, total_work)
                continue

            filepath = os.path.join(sectors_dir, filename)
            self.log_message.emit(
                "info", f"Stage 2 [{index}/{total}] reading {filename}..."
            )
            try:
                read_time, digest = self._read_chunk(filepath)
                speed = (
                    self.chunk_size_bytes / read_time / MB
                    if read_time > 0
                    else 0.0
                )
                hash_ok = digest == self.expected_checksum
                entries.append(
                    {
                        "file": filename,
                        "read_time_s": read_time,
                        "read_speed_mbps": speed,
                        "ghost_status": 0,
                        "hash_ok": hash_ok,
                    }
                )
                if hash_ok:
                    self.log_message.emit(
                        "info",
                        f"Stage 2 [{index}/{total}] OK {filename}: "
                        f"{read_time:.4f} s, {speed:.1f} MB/s, hash OK",
                    )
                else:
                    self.log_message.emit(
                        "warning",
                        f"Stage 2 [{index}/{total}] HASH MISMATCH {filename}: "
                        f"{read_time:.4f} s, {speed:.1f} MB/s",
                    )
            except Exception as exc:
                entries.append(
                    {
                        "file": filename,
                        "read_time_s": -1,
                        "read_speed_mbps": -1,
                        "ghost_status": 0,
                        "hash_ok": False,
                    }
                )
                self.log_message.emit(
                    "warning",
                    f"Stage 2 [{index}/{total}] READ FAILED {filename}: {exc}",
                )
            self.progress_changed.emit(total + index, total_work)
        return entries

    @staticmethod
    def _write_json(path, entries):
        try:
            with open(path, "w", encoding="utf-8") as file_object:
                json.dump({"entries": entries}, file_object, indent=2)
        except OSError as exc:
            raise OSError(
                f"Cannot write stage JSON file:\n{path}\n\n{exc}"
            ) from exc

    def _run_internal(self):
        check_app_root_writable()
        sectors_dir = self._sectors_dir()
        app_root = get_app_root()
        stage1_path = os.path.join(app_root, STAGE1_FILE)
        stage2_path = os.path.join(app_root, STAGE2_FILE)
        self.results["stage1_path"] = stage1_path
        self.results["stage2_path"] = stage2_path

        if os.path.exists(stage1_path) or os.path.exists(stage2_path):
            self.log_message.emit(
                "warning",
                "Existing stage JSON file(s) will be overwritten. Old chunk files "
                "are not removed.",
            )

        free = disk_usage(self.disk_path).free
        usable = int(free * 0.995)
        total = max(1, int(usable // self.chunk_size_bytes))
        self.total_chunks = total
        self.results["total"] = total
        total_work = total * 2

        self.log_message.emit(
            "info",
            f"Calculated {total} chunks "
            f"({self.chunk_size_bytes / MB:.1f} MB each) "
            f"from {free / MB:.1f} MB free space "
            f"(0.5 % reserved for metadata overhead)",
        )

        self.stage_changed.emit(f"Stage 1 - writing {total} chunks")
        stage1_entries = self._run_stage1(sectors_dir, total, total_work)
        self._write_json(stage1_path, stage1_entries)
        self.results["ghost_count"] = sum(
            1 for entry in stage1_entries if entry["ghost_status"] == 1
        )
        self.log_message.emit("info", f"Stage 1 complete. Saved {stage1_path}")

        self.stage_changed.emit(f"Stage 2 - reading {total} chunks")
        stage2_entries = self._run_stage2(sectors_dir, stage1_entries, total_work)
        self._write_json(stage2_path, stage2_entries)

        read_failures = sum(
            1
            for entry in stage2_entries
            if entry["ghost_status"] == 0 and entry["read_time_s"] == -1
        )
        hash_mismatches = sum(
            1
            for entry in stage2_entries
            if entry["ghost_status"] == 0
            and entry["read_time_s"] >= 0
            and entry["hash_ok"] is False
        )
        self.results["read_failure_count"] = read_failures
        self.results["hash_mismatch_count"] = hash_mismatches

        self.log_message.emit("info", f"Stage 2 complete. Saved {stage2_path}")
        self.stage_changed.emit("Complete")
        self.progress_changed.emit(total_work, total_work)
        self.log_message.emit(
            "info",
            f"Summary: {total} chunks, {self.results['ghost_count']} ghost(s), "
            f"{read_failures} read failure(s), {hash_mismatches} hash mismatch(es)",
        )

    def run(self):
        try:
            self._run_internal()
        except Exception as exc:
            self._error = str(exc)
            self.log_message.emit("error", f"Run failed: {exc}")
        finally:
            self.results["error"] = self._error
            if self._error is None:
                self.log_message.emit("info", "Run finished.")
            self.work_finished.emit(dict(self.results))

    # --------------------------------------------------------------- controls
    def pause(self):
        self._paused = True
        self.log_message.emit(
            "info", "Paused - waiting for the current chunk to finish."
        )

    def resume(self):
        self._paused = False
        self.log_message.emit("info", "Resumed.")


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self._logger = logging.getLogger(__name__)
        self._version = read_version()
        self.worker = None
        self._is_paused = False

        self.setWindowTitle(APP_TITLE)
        self.setWindowIcon(QIcon(get_running_path(ICON_FILE)))
        self.setMinimumSize(700, 550)
        self.resize(900, 700)

        self._build_ui()

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        header_layout = QHBoxLayout()
        header_layout.addWidget(
            QLabel(
                "Write, read, and verify disk sectors. "
                "Stage JSONs are saved next to this app."
            )
        )
        header_layout.addStretch()
        if self._version:
            header_layout.addWidget(QLabel(f"v{self._version}"))
        main_layout.addLayout(header_layout)

        path_layout = QHBoxLayout()
        path_layout.addWidget(QLabel("Target Path:"))
        self.path_input = QLineEdit()
        self.path_input.setPlaceholderText("Select the target disk/folder...")
        path_layout.addWidget(self.path_input)
        self.browse_btn = QPushButton("Browse")
        self.browse_btn.clicked.connect(self._browse)
        path_layout.addWidget(self.browse_btn)
        main_layout.addLayout(path_layout)

        settings_layout = QHBoxLayout()
        settings_layout.addWidget(QLabel("Chunk Size (MB):"))
        self.chunk_spin = QSpinBox()
        self.chunk_spin.setRange(1, 10240)
        self.chunk_spin.setValue(100)
        self.chunk_spin.setSingleStep(10)
        settings_layout.addWidget(self.chunk_spin)
        settings_layout.addSpacing(16)

        self.start_btn = QPushButton("Start")
        self.start_btn.clicked.connect(self._on_start)
        settings_layout.addWidget(self.start_btn)

        self.pause_btn = QPushButton("Pause")
        self.pause_btn.setEnabled(False)
        self.pause_btn.clicked.connect(self._on_pause)
        settings_layout.addWidget(self.pause_btn)

        settings_layout.addStretch()
        main_layout.addLayout(settings_layout)

        progress_layout = QHBoxLayout()
        progress_layout.addWidget(QLabel("Progress:"))
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        progress_layout.addWidget(self.progress_bar, stretch=1)
        self.stage_label = QLabel("Idle")
        progress_layout.addWidget(self.stage_label)
        main_layout.addLayout(progress_layout)

        console_header = QHBoxLayout()
        console_header.addWidget(QLabel("Console"))
        console_header.addStretch()
        clear_btn = QPushButton("Clear")
        clear_btn.clicked.connect(self._on_clear_console)
        console_header.addWidget(clear_btn)
        main_layout.addLayout(console_header)

        self.console = QPlainTextEdit()
        self.console.setReadOnly(True)
        self.console.setMaximumBlockCount(5000)
        main_layout.addWidget(self.console, stretch=1)

    # --------------------------------------------------------------- browse/start
    def _browse(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Target Folder")
        if folder:
            self.path_input.setText(folder)

    def _on_clear_console(self):
        self.console.clear()

    def _on_start(self):
        if self.worker is not None and self.worker.isRunning():
            return

        disk_path = self.path_input.text().strip()
        if not disk_path or not os.path.isdir(disk_path):
            QMessageBox.warning(
                self, "Invalid Path", "Please select a valid disk/folder path."
            )
            return

        try:
            check_app_root_writable()
        except OSError as exc:
            QMessageBox.critical(self, "Cannot Write JSON Files", str(exc))
            return

        if self.worker is not None:
            self.worker.deleteLater()
            self.worker = None

        self._is_paused = False
        self.pause_btn.setText("Pause")
        self.console.clear()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setValue(0)
        self.stage_label.setText("Starting...")

        self.worker = StageWorker(disk_path, self.chunk_spin.value(), parent=None)
        self.worker.log_message.connect(self._on_log)
        self.worker.progress_changed.connect(self._on_progress)
        self.worker.stage_changed.connect(self._on_stage_changed)
        self.worker.work_finished.connect(self._on_work_finished)
        self.worker.finished.connect(self._on_worker_finished)

        self._set_running_state(True)
        self.worker.start()

    # --------------------------------------------------------------- pause
    def _on_pause(self):
        if self.worker is None or not self.worker.isRunning():
            return

        if not self._is_paused:
            self.worker.pause()
            self.pause_btn.setText("Resume")
            self._is_paused = True
        else:
            self.worker.resume()
            self.pause_btn.setText("Pause")
            self._is_paused = False

    # --------------------------------------------------------------- slots
    @Slot(str, str)
    def _on_log(self, level, message):
        logger_method = getattr(self._logger, level, self._logger.info)
        logger_method(message)
        self.console.appendPlainText(message)

    @Slot(int, int)
    def _on_progress(self, current, total):
        if total <= 0:
            return
        if self.progress_bar.minimum() != 0 or self.progress_bar.maximum() != total:
            self.progress_bar.setRange(0, total)
        self.progress_bar.setValue(current)

    @Slot(str)
    def _on_stage_changed(self, text):
        self.stage_label.setText(text)

    @Slot(object)
    def _on_work_finished(self, results):
        self.pause_btn.setEnabled(False)
        self._is_paused = False
        self.pause_btn.setText("Pause")

        error = results.get("error")
        if error:
            self.stage_label.setText("Failed")
            QMessageBox.critical(
                self,
                "Run Failed",
                f"The run did not complete:\n\n{error}",
            )
            return

        self.stage_label.setText("Complete")
        summary = (
            f"Stage 1: {results['total']} chunks, "
            f"{results['ghost_count']} ghost(s)\n"
            f"Stage 2: {results['total']} entries, "
            f"{results['read_failure_count']} read failure(s), "
            f"{results['hash_mismatch_count']} hash mismatch(es)\n\n"
            f"stage1.json:\n{results['stage1_path']}\n\n"
            f"stage2.json:\n{results['stage2_path']}"
        )
        QMessageBox.information(self, "Run Complete", summary)

    @Slot()
    def _on_worker_finished(self):
        if self.worker is not None:
            self.worker.deleteLater()
            self.worker = None
        self._set_running_state(False)

    # --------------------------------------------------------------- ui state
    def _set_running_state(self, running):
        self.path_input.setEnabled(not running)
        self.browse_btn.setEnabled(not running)
        self.chunk_spin.setEnabled(not running)
        self.start_btn.setEnabled(not running)
        self.pause_btn.setEnabled(running)

    def closeEvent(self, event):
        if self.worker is not None and self.worker.isRunning():
            QMessageBox.warning(
                self,
                "Run in Progress",
                "A stage is still running (or paused). Wait for it to finish "
                "before closing.",
            )
            event.ignore()
            return
        event.accept()


def set_windows_app_id():
    if sys.platform != "win32":
        return
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(APP_ID)
    except (AttributeError, OSError):
        pass


def main():
    configure_logger()
    set_windows_app_id()
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(get_running_path(ICON_FILE)))
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
