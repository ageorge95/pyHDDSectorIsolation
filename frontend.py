import math
import os
from PySide6.QtCore import Qt, Slot, QTimer
from PySide6.QtGui import QColor, QPainter, QBrush, QPen, QIcon
from PySide6.QtWidgets import (
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QDoubleSpinBox,
    QProgressBar,
    QFileDialog,
    QMessageBox,
    QSizePolicy,
)
from backend import SectorWorker

COLORS = {
    "white": QColor(220, 220, 220),
    "yellow": QColor(255, 220, 50),
    "green": QColor(50, 200, 50),
    "red": QColor(220, 50, 50),
}

def get_running_path(relative_path):
    if '_internal' in os.listdir():
        return os.path.join('_internal', relative_path)
    else:
        return relative_path

class SectorGridWidget(QWidget):
    """Custom widget that draws a grid of colored squares representing chunks."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.total = 0
        self.statuses = []  # list of str: "white", "yellow", "green", "red"
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.setMinimumSize(200, 200)

        # Throttled repaint: schedule at most one repaint per interval
        self._repaint_pending = False
        self._repaint_timer = QTimer(self)
        self._repaint_timer.setSingleShot(True)
        self._repaint_timer.setInterval(50)  # repaint at most every 50ms
        self._repaint_timer.timeout.connect(self._do_repaint)

    def set_total(self, total):
        self.total = total
        self.statuses = ["white"] * total
        self._schedule_repaint()

    def set_chunk_status(self, index, status):
        if 0 <= index < len(self.statuses):
            self.statuses[index] = status

    def set_chunk_status_batch(self, updates):
        """Apply a batch of (index, status) updates and schedule a single repaint."""
        for index, status in updates:
            if 0 <= index < len(self.statuses):
                self.statuses[index] = status
        self._schedule_repaint()

    def clear(self):
        self.total = 0
        self.statuses = []
        self._schedule_repaint()

    def _schedule_repaint(self):
        if not self._repaint_pending:
            self._repaint_pending = True
            self._repaint_timer.start()

    def _do_repaint(self):
        self._repaint_pending = False
        self.update()

    def paintEvent(self, event):
        if self.total == 0:
            return

        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing, False)

        w = self.width()
        h = self.height()

        # Calculate grid dimensions
        # We want cols/rows ratio to roughly match widget aspect ratio
        aspect = w / max(h, 1)
        cols = max(1, int(math.ceil(math.sqrt(self.total * aspect))))
        rows = max(1, int(math.ceil(self.total / cols)))

        # Square size (with 1px gap)
        gap = 1
        sq_w = max(1, (w - gap) / cols - gap)
        sq_h = max(1, (h - gap) / rows - gap)
        sq_size = max(1, min(sq_w, sq_h))

        # Recalculate cols based on actual square size to center the grid
        actual_cols = max(1, int((w - gap) / (sq_size + gap)))
        actual_rows = max(1, int(math.ceil(self.total / actual_cols)))

        # Centering offsets
        total_grid_w = actual_cols * (sq_size + gap) + gap
        total_grid_h = actual_rows * (sq_size + gap) + gap
        offset_x = max(0, (w - total_grid_w) / 2)
        offset_y = max(0, (h - total_grid_h) / 2)

        painter.setPen(QPen(Qt.NoPen))

        for i in range(self.total):
            col = i % actual_cols
            row = i // actual_cols

            x = offset_x + gap + col * (sq_size + gap)
            y = offset_y + gap + row * (sq_size + gap)

            color = COLORS.get(self.statuses[i], COLORS["white"])
            painter.setBrush(QBrush(color))
            painter.drawRect(int(x), int(y), int(sq_size), int(sq_size))

        painter.end()

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("HDD Sector Isolator")
        self.setWindowTitle("HDD Sector Isolator v" + open(get_running_path('version.txt')).read())
        self.setWindowIcon(QIcon(get_running_path('icon.ico')))
        self.setMinimumSize(700, 500)
        self.resize(900, 650)

        self.worker = None
        self._is_paused = False

        self._build_ui()
        self._try_load_session()

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        # -------------------------------------------- Settings row: path
        path_layout = QHBoxLayout()
        path_layout.addWidget(QLabel("Disk Path:"))
        self.path_input = QLineEdit()
        self.path_input.setPlaceholderText("Select target disk/folder...")
        self.path_input.textChanged.connect(self._on_settings_changed)
        path_layout.addWidget(self.path_input)
        self.browse_btn = QPushButton("Browse")
        self.browse_btn.clicked.connect(self._browse)
        path_layout.addWidget(self.browse_btn)
        main_layout.addLayout(path_layout)

        # -------------------------------------------- Settings row: chunk size + threshold
        settings_layout = QHBoxLayout()

        settings_layout.addWidget(QLabel("Chunk Size (MB):"))
        self.chunk_spin = QDoubleSpinBox()
        self.chunk_spin.setRange(1, 10240)
        self.chunk_spin.setValue(40)
        self.chunk_spin.setDecimals(1)
        self.chunk_spin.setSingleStep(10)
        self.chunk_spin.valueChanged.connect(self._on_settings_changed)
        settings_layout.addWidget(self.chunk_spin)

        settings_layout.addSpacing(20)

        settings_layout.addWidget(QLabel("Bad Threshold (s):"))
        self.threshold_spin = QDoubleSpinBox()
        self.threshold_spin.setRange(0.1, 60.0)
        self.threshold_spin.setValue(1.0)
        self.threshold_spin.setDecimals(2)
        self.threshold_spin.setSingleStep(0.1)
        settings_layout.addWidget(self.threshold_spin)

        settings_layout.addStretch()
        main_layout.addLayout(settings_layout)

        # -------------------------------------------- Buttons row
        btn_layout = QHBoxLayout()

        self.start_btn = QPushButton("Start")
        self.start_btn.clicked.connect(self._on_start)
        btn_layout.addWidget(self.start_btn)

        self.pause_btn = QPushButton("Pause")
        self.pause_btn.setEnabled(False)
        self.pause_btn.clicked.connect(self._on_pause)
        btn_layout.addWidget(self.pause_btn)

        self.stop_btn = QPushButton("Stop")
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self._on_stop)
        btn_layout.addWidget(self.stop_btn)

        self.new_session_btn = QPushButton("New Session")
        self.new_session_btn.clicked.connect(self._on_new_session)
        btn_layout.addWidget(self.new_session_btn)

        btn_layout.addStretch()
        main_layout.addLayout(btn_layout)

        # -------------------------------------------- Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        main_layout.addWidget(self.progress_bar)

        # -------------------------------------------- Sector grid
        self.grid_widget = SectorGridWidget()
        main_layout.addWidget(self.grid_widget, stretch=1)

    # ----------------------------------------------------------- session load
    def _try_load_session(self):
        state = SectorWorker.load_state()
        if state is None:
            return

        # Check if the session is already fully complete
        chunks = state.get("chunks", [])
        all_done = all(c["status"] in ("green", "red") for c in chunks) and len(chunks) > 0
        if all_done:
            # Session was completed — just show results, don't auto-resume
            self.path_input.setText(state["disk_path"])
            self.chunk_spin.setValue(state["chunk_size_mb"])
            self.threshold_spin.setValue(state["threshold_s"])
            self.grid_widget.set_total(state["total_chunks"])
            for chunk in chunks:
                self.grid_widget.set_chunk_status(chunk["index"], chunk["status"])
            self.grid_widget._schedule_repaint()
            total_work = state["total_chunks"] * 2
            self.progress_bar.setMaximum(total_work)
            self.progress_bar.setValue(total_work)
            print(f"Loaded completed session ({len(chunks)} chunks). Click 'New Session' to start fresh.")
            return

        reply = QMessageBox.question(
            self,
            "Resume Session",
            f"A previous session was found:\n"
            f"  Path: {state['disk_path']}\n"
            f"  Chunks: {state['total_chunks']} × {state['chunk_size_mb']:.1f} MB\n"
            f"  Phase: {state['current_phase']}, at chunk {state['current_chunk_index']}\n\n"
            f"Do you want to resume?",
            QMessageBox.Yes | QMessageBox.No,
        )

        if reply == QMessageBox.Yes:
            self.path_input.setText(state["disk_path"])
            self.chunk_spin.setValue(state["chunk_size_mb"])
            self.threshold_spin.setValue(state["threshold_s"])

            self.worker = SectorWorker.from_state(state, parent=None)
            self._setup_worker_signals()

            # Restore grid
            self.grid_widget.set_total(state["total_chunks"])
            for chunk in state["chunks"]:
                self.grid_widget.set_chunk_status(chunk["index"], chunk["status"])
            self.grid_widget._schedule_repaint()

            # Restore progress bar
            total_work = state["total_chunks"] * 2
            self.progress_bar.setMaximum(total_work)
            # Calculate completed work
            if state["current_phase"] == 1:
                completed = state["current_chunk_index"]
            else:
                completed = state["total_chunks"] + state["current_chunk_index"]
            self.progress_bar.setValue(completed)

            self._set_running_state(True)
            self.worker.start()
        else:
            SectorWorker.clear_state()

    # ----------------------------------------------------------- preview grid
    def _on_settings_changed(self):
        """Recalculate and preview the grid when settings change (only when not running)."""
        if self.worker is not None:
            return

        disk_path = self.path_input.text().strip()
        if not disk_path or not os.path.isdir(disk_path):
            self.grid_widget.clear()
            self.progress_bar.setValue(0)
            self.progress_bar.setMaximum(100)
            return

        try:
            from shutil import disk_usage
            free = disk_usage(disk_path).free
            chunk_bytes = int(self.chunk_spin.value() * 1024 * 1024)
            total = max(1, int(free // chunk_bytes))
            self.grid_widget.set_total(total)
            self.progress_bar.setMaximum(total * 2)
            self.progress_bar.setValue(0)
        except Exception:
            self.grid_widget.clear()

    # ----------------------------------------------------------- browse
    def _browse(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Target Folder")
        if folder:
            self.path_input.setText(folder)

    # ----------------------------------------------------------- start
    def _on_start(self):
        disk_path = self.path_input.text().strip()
        if not disk_path or not os.path.isdir(disk_path):
            QMessageBox.warning(self, "Invalid Path", "Please select a valid disk/folder path.")
            return

        chunk_mb = self.chunk_spin.value()
        threshold = self.threshold_spin.value()

        self.worker = SectorWorker(disk_path, chunk_mb, threshold, parent=None)
        self._setup_worker_signals()

        # Grid will be set up once we know total_chunks (after worker starts)
        self._set_running_state(True)
        self.worker.start()

    def _setup_worker_signals(self):
        self.worker.chunk_status_batch.connect(self._on_chunk_status_batch)
        self.worker.progress_changed.connect(self._on_progress)
        self.worker.log_message.connect(self._on_log)
        self.worker.work_finished.connect(self._on_finished)

    # ----------------------------------------------------------- pause
    def _on_pause(self):
        if self.worker is None:
            return

        if not self._is_paused:
            self.worker.pause()
            self.pause_btn.setText("Resume")
            self._is_paused = True
        else:
            self.worker.resume()
            self.pause_btn.setText("Pause")
            self._is_paused = False

    # ----------------------------------------------------------- stop
    def _on_stop(self):
        if self.worker is None:
            return

        self.worker.stop()
        # Worker will finish its current chunk, save state, and emit work_finished

    # ----------------------------------------------------------- new session
    def _on_new_session(self):
        if self.worker is not None and self.worker.isRunning():
            reply = QMessageBox.question(
                self,
                "New Session",
                "A session is currently running. Stop it and start a new session?",
                QMessageBox.Yes | QMessageBox.No,
            )
            if reply == QMessageBox.No:
                return
            self.worker.stop()
            self.worker.wait()

        SectorWorker.clear_state()
        self.worker = None
        self._is_paused = False

        self._set_running_state(False)
        self.grid_widget.clear()
        self.progress_bar.setValue(0)
        self.progress_bar.setMaximum(100)

        # Trigger preview recalculation
        self._on_settings_changed()

    # ----------------------------------------------------------- slots
    @Slot(list)
    def _on_chunk_status_batch(self, updates):
        # Ensure grid total is set (first signal from a fresh worker)
        if self.worker and self.grid_widget.total == 0 and self.worker.total_chunks > 0:
            self.grid_widget.set_total(self.worker.total_chunks)
            self.progress_bar.setMaximum(self.worker.total_chunks * 2)

        self.grid_widget.set_chunk_status_batch(updates)

    @Slot(int, int)
    def _on_progress(self, current, total):
        if self.progress_bar.maximum() != total:
            self.progress_bar.setMaximum(total)
        self.progress_bar.setValue(current)

    @Slot(str)
    def _on_log(self, message):
        print(message)

    @Slot()
    def _on_finished(self):
        self._set_running_state(False)
        self._is_paused = False
        self.pause_btn.setText("Pause")
        print("Worker finished.")

    # ----------------------------------------------------------- ui state
    def _set_running_state(self, running):
        """Enable/disable controls based on whether a session is active."""
        self.path_input.setEnabled(not running)
        self.browse_btn.setEnabled(not running)
        self.chunk_spin.setEnabled(not running)
        self.threshold_spin.setEnabled(not running)
        self.start_btn.setEnabled(not running)

        self.pause_btn.setEnabled(running)
        self.stop_btn.setEnabled(running)

    # ----------------------------------------------------------- close event
    def closeEvent(self, event):
        if self.worker is not None and self.worker.isRunning():
            self.worker.stop()
            self.worker.wait(5000)
        event.accept()