import ctypes
import json
import logging
import math
import os
import sys

from PySide6.QtCore import QTimer, Qt, Slot
from PySide6.QtGui import QColor, QIcon, QImage, QPainter
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QButtonGroup,
    QCheckBox,
    QDoubleSpinBox,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QToolTip,
    QVBoxLayout,
    QWidget,
)

from ag95 import configure_logger

APP_TITLE = "Sector Performance Analyzer"
APP_ID = "AG95.pyHDDSectorIsolation.module2"
ICON_FILE = "icon.ico"
VERSION_FILE = "version.txt"
STAGE1_FILE = "stage1.json"
STAGE2_FILE = "stage2.json"

RED = QColor(220, 50, 50)
GREEN = QColor(50, 200, 50)
VIOLET = QColor(150, 50, 200)
GRAY = QColor(220, 220, 220)
RED_RGB = RED.rgb()
GREEN_RGB = GREEN.rgb()
VIOLET_RGB = VIOLET.rgb()
GRAY_RGB = GRAY.rgb()


class StageDataError(Exception):
    """Raised when stage1.json/stage2.json cannot be used."""


def get_running_path(relative_path):
    base_path = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)


def get_app_root():
    """Return the folder that contains the exe (or module2.py when run from source)."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


def read_version():
    try:
        with open(get_running_path(VERSION_FILE), "r", encoding="utf-8") as file_object:
            return file_object.read().strip()
    except OSError:
        return ""


def _is_number(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _is_ghost_status(value):
    return isinstance(value, int) and not isinstance(value, bool) and value in (0, 1)


def _is_safe_filename(value):
    if not isinstance(value, str) or not value:
        return False
    if value in (".", "..") or os.path.isabs(value):
        return False
    if value != os.path.basename(value):
        return False
    if any(character in value for character in '<>:"/\\|?*'):
        return False
    if any(ord(character) < 32 for character in value):
        return False
    return True


def validate_stage1_entries(entries):
    if not isinstance(entries, list) or not entries:
        raise StageDataError("stage1.json contains no entries.")
    seen = set()
    required = {"file", "write_time_s", "write_speed_mbps", "ghost_status"}
    for index, entry in enumerate(entries, start=1):
        where = f"stage1 entry {index}"
        if not isinstance(entry, dict):
            raise StageDataError(f"{where} is not an object.")
        if not required.issubset(entry):
            missing = ", ".join(sorted(required - set(entry)))
            raise StageDataError(f"{where} is missing required key(s): {missing}.")
        filename = entry["file"]
        if not _is_safe_filename(filename):
            raise StageDataError(f"{where} has an invalid 'file' value.")
        if filename in seen:
            raise StageDataError(f"{where} duplicates file name {filename}.")
        seen.add(filename)

        write_time = entry["write_time_s"]
        write_speed = entry["write_speed_mbps"]
        ghost_status = entry["ghost_status"]
        if not _is_number(write_time) or not _is_number(write_speed):
            raise StageDataError(f"{where} has non-numeric write data.")
        if not _is_ghost_status(ghost_status):
            raise StageDataError(f"{where} has an invalid 'ghost_status'.")
        if ghost_status == 1:
            if write_time != -1 or write_speed != -1:
                raise StageDataError(
                    f"{where} is a ghost but its write data is not -1."
                )
        elif write_time < 0 or write_speed < 0:
            raise StageDataError(
                f"{where} is not a ghost but its write data is negative."
            )
    return entries


def validate_stage2_entries(entries, stage1_entries):
    if not isinstance(entries, list):
        raise StageDataError("stage2.json does not contain an entries list.")
    if len(entries) != len(stage1_entries):
        raise StageDataError(
            f"stage2.json has {len(entries)} entries but stage1.json has "
            f"{len(stage1_entries)}."
        )

    required = {
        "file",
        "read_time_s",
        "read_speed_mbps",
        "ghost_status",
        "hash_ok",
    }
    for index, (stage2_entry, stage1_entry) in enumerate(
        zip(entries, stage1_entries), start=1
    ):
        where = f"stage2 entry {index}"
        if not isinstance(stage2_entry, dict):
            raise StageDataError(f"{where} is not an object.")
        if not required.issubset(stage2_entry):
            missing = ", ".join(sorted(required - set(stage2_entry)))
            raise StageDataError(f"{where} is missing required key(s): {missing}.")
        if stage2_entry["file"] != stage1_entry["file"]:
            raise StageDataError(
                f"{where} refers to {stage2_entry['file']!r} but stage1 entry "
                f"{index} is {stage1_entry['file']!r}."
            )

        ghost_status = stage2_entry["ghost_status"]
        if not _is_ghost_status(ghost_status):
            raise StageDataError(f"{where} has an invalid 'ghost_status'.")
        if ghost_status != stage1_entry["ghost_status"]:
            raise StageDataError(
                f"{where} has ghost_status {ghost_status} but stage1 entry "
                f"{index} has {stage1_entry['ghost_status']}."
            )

        read_time = stage2_entry["read_time_s"]
        read_speed = stage2_entry["read_speed_mbps"]
        hash_ok = stage2_entry["hash_ok"]
        if not _is_number(read_time) or not _is_number(read_speed):
            raise StageDataError(f"{where} has non-numeric read data.")

        if ghost_status == 1:
            if read_time != -1 or read_speed != -1:
                raise StageDataError(
                    f"{where} is a ghost but its read data is not -1."
                )
            if hash_ok is not None:
                raise StageDataError(
                    f"{where} is a ghost but 'hash_ok' is not null."
                )
        elif read_time == -1:
            if read_speed != -1:
                raise StageDataError(
                    f"{where} has a read failure but its speed is not -1."
                )
            if hash_ok is not False:
                raise StageDataError(
                    f"{where} has a read failure but 'hash_ok' is not false."
                )
        else:
            if read_time < 0 or read_speed < 0:
                raise StageDataError(f"{where} has inconsistent read data.")
            if not isinstance(hash_ok, bool):
                raise StageDataError(
                    f"{where} must have a boolean 'hash_ok' value."
                )
    return entries


def load_stage_data(app_root):
    if not os.path.isdir(app_root):
        raise StageDataError(f"App folder is not a folder:\n{app_root}")

    stage1_path = os.path.join(app_root, STAGE1_FILE)
    stage2_path = os.path.join(app_root, STAGE2_FILE)
    missing = [
        path for path in (stage1_path, stage2_path) if not os.path.isfile(path)
    ]
    if missing:
        raise StageDataError(
            "Missing required stage data in the app folder:\n"
            + "\n".join(missing)
        )

    def _load(path):
        try:
            with open(path, "r", encoding="utf-8") as file_object:
                data = json.load(file_object)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise StageDataError(f"Invalid JSON in {path}:\n{exc}") from exc
        except OSError as exc:
            raise StageDataError(f"Could not read {path}:\n{exc}") from exc
        if not isinstance(data, dict) or "entries" not in data:
            raise StageDataError(f"{path} does not contain an 'entries' list.")
        if not isinstance(data["entries"], list):
            raise StageDataError(f"{path} has a non-list 'entries' value.")
        return data["entries"]

    stage1_entries = validate_stage1_entries(_load(stage1_path))
    stage2_entries = validate_stage2_entries(_load(stage2_path), stage1_entries)
    return stage1_entries, stage2_entries, stage1_path, stage2_path


def classify_entries(
    stage1_entries,
    stage2_entries,
    min_write_speed,
    max_write_time,
    min_read_speed,
    max_read_time,
    ghost_bad,
    read_failure_bad,
    hash_mismatch_bad,
):
    bad_files = []
    ghost_count = 0

    for stage1_entry, stage2_entry in zip(stage1_entries, stage2_entries):
        filename = stage1_entry["file"]
        is_bad = False

        if stage1_entry["ghost_status"] == 1:
            ghost_count += 1
            is_bad = bool(ghost_bad)
        else:
            if (
                stage1_entry["write_time_s"] >= 0
                and stage1_entry["write_speed_mbps"] >= 0
            ):
                if stage1_entry["write_speed_mbps"] < min_write_speed:
                    is_bad = True
                if stage1_entry["write_time_s"] > max_write_time:
                    is_bad = True

            if stage2_entry["read_time_s"] < 0:
                if read_failure_bad:
                    is_bad = True
            else:
                if (
                    stage2_entry["read_speed_mbps"] >= 0
                    and stage2_entry["read_speed_mbps"] < min_read_speed
                ):
                    is_bad = True
                if stage2_entry["read_time_s"] > max_read_time:
                    is_bad = True
                if stage2_entry["hash_ok"] is False and hash_mismatch_bad:
                    is_bad = True

        if is_bad:
            bad_files.append(filename)

    total = len(stage1_entries)
    bad_count = len(bad_files)
    good_count = total - bad_count
    return total, good_count, bad_count, ghost_count, bad_files


def speed_range(stage1_entries, stage2_entries, view):
    values = []
    if view == "write":
        for entry in stage1_entries:
            if entry["ghost_status"] == 0 and entry["write_speed_mbps"] >= 0:
                values.append(entry["write_speed_mbps"])
    else:
        for entry in stage2_entries:
            if (
                entry["ghost_status"] == 0
                and entry["read_time_s"] >= 0
                and entry["read_speed_mbps"] >= 0
            ):
                values.append(entry["read_speed_mbps"])

    if not values:
        return None, None
    return min(values), max(values)


def speed_color(speed, min_speed, max_speed):
    if min_speed is None or max_speed is None:
        return GRAY
    if max_speed <= min_speed:
        return GREEN

    position = (speed - min_speed) / (max_speed - min_speed)
    position = max(0.0, min(1.0, position))
    red = round(RED.red() + (GREEN.red() - RED.red()) * position)
    green = round(RED.green() + (GREEN.green() - RED.green()) * position)
    blue = round(RED.blue() + (GREEN.blue() - RED.blue()) * position)
    return QColor(red, green, blue)


def format_time(value):
    if value == -1:
        return "-1"
    text = f"{value:.4f}".rstrip("0").rstrip(".")
    return text if text else "0"


def format_speed(value):
    if value == -1:
        return "-1"
    return f"{value:.1f}"


def tooltip_for(index, stage1_entries, stage2_entries):
    stage1_entry = stage1_entries[index]
    stage2_entry = stage2_entries[index]
    filename = stage1_entry["file"]
    stem, _extension = os.path.splitext(filename)
    try:
        file_index = int(stem)
    except ValueError:
        file_index = "?"

    ghost = stage1_entry["ghost_status"] == 1
    if ghost:
        hash_text = "Not checked"
    elif stage2_entry["read_time_s"] < 0:
        hash_text = "Failed"
    elif stage2_entry["hash_ok"] is True:
        hash_text = "OK"
    elif stage2_entry["hash_ok"] is False:
        hash_text = "Mismatch"
    else:
        hash_text = "Not checked"

    return "\n".join(
        [
            f"File: {filename} ({file_index})",
            f"Write: {format_time(stage1_entry['write_time_s'])} s, "
            f"{format_speed(stage1_entry['write_speed_mbps'])} MB/s",
            f"Read:  {format_time(stage2_entry['read_time_s'])} s, "
            f"{format_speed(stage2_entry['read_speed_mbps'])} MB/s",
            f"Hash:  {hash_text}",
            f"Ghost: {'Yes' if ghost else 'No'}",
        ]
    )


def _next_bad_target(sectors_dir, filename):
    base_name = "BAD_" + filename
    candidate = os.path.join(sectors_dir, base_name)
    if not os.path.exists(candidate):
        return candidate

    stem, extension = os.path.splitext(base_name)
    suffix = 2
    while True:
        candidate = os.path.join(sectors_dir, f"{stem}_{suffix}{extension}")
        if not os.path.exists(candidate):
            return candidate
        suffix += 1


def _find_existing_bad_variant(sectors_dir, filename):
    base_name = "BAD_" + filename
    candidate = os.path.join(sectors_dir, base_name)
    if os.path.exists(candidate):
        return candidate

    stem, extension = os.path.splitext(base_name)
    for suffix in range(2, 10000):
        candidate = os.path.join(sectors_dir, f"{stem}_{suffix}{extension}")
        if os.path.exists(candidate):
            return candidate
    return None


def rename_bad_files(sectors_dir, bad_files):
    renamed = 0
    already = 0
    failures = []

    for filename in bad_files:
        source = os.path.join(sectors_dir, filename)

        if filename.startswith("BAD_"):
            if os.path.isfile(source):
                already += 1
            else:
                failures.append((filename, "File not found."))
            continue

        if not os.path.isfile(source):
            if _find_existing_bad_variant(sectors_dir, filename):
                already += 1
            else:
                failures.append((filename, "File not found."))
            continue

        target = _next_bad_target(sectors_dir, filename)
        try:
            os.rename(source, target)
            renamed += 1
        except OSError as exc:
            failures.append((filename, str(exc)))

    return {"renamed": renamed, "already": already, "failures": failures}


class SectorGridWidget(QWidget):
    """One pixel per file, scaled to the widget with nearest-neighbour drawing."""

    def __init__(self, parent=None):
        super().__init__(parent)

        self.total = 0
        self._stage1_entries = []
        self._stage2_entries = []
        self._view = "write"
        self._colors = []
        self._img = None
        self._img_cols = 0
        self._hover_index = None

        self.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding
        )
        self.setMinimumSize(300, 300)
        self.setMouseTracking(True)

        self._repaint_pending = False
        self._repaint_timer = QTimer(self)
        self._repaint_timer.setSingleShot(True)
        self._repaint_timer.setInterval(50)
        self._repaint_timer.timeout.connect(self._do_repaint)

    def set_data(self, stage1_entries, stage2_entries, view):
        self._stage1_entries = stage1_entries
        self._stage2_entries = stage2_entries
        self._view = view
        self.total = len(stage1_entries)
        self._hover_index = None
        QToolTip.hideText()

        min_speed, max_speed = speed_range(
            stage1_entries, stage2_entries, view
        )
        colors = []
        for index, stage1_entry in enumerate(stage1_entries):
            if stage1_entry["ghost_status"] == 1:
                colors.append(VIOLET_RGB)
                continue

            if view == "write":
                speed = stage1_entry["write_speed_mbps"]
                has_data = (
                    stage1_entry["write_time_s"] >= 0 and speed >= 0
                )
            else:
                stage2_entry = stage2_entries[index]
                speed = stage2_entry["read_speed_mbps"]
                has_data = (
                    stage2_entry["read_time_s"] >= 0 and speed >= 0
                )

            if not has_data:
                colors.append(GRAY_RGB)
            else:
                colors.append(
                    speed_color(speed, min_speed, max_speed).rgb()
                )

        self._colors = colors
        self._rebuild_image()
        self._schedule_repaint()

    def clear(self):
        self.total = 0
        self._stage1_entries = []
        self._stage2_entries = []
        self._colors = []
        self._img = None
        self._img_cols = 0
        self._hover_index = None
        QToolTip.hideText()
        self._schedule_repaint()

    def _rebuild_image(self):
        if self.total == 0:
            self._img = None
            self._img_cols = 0
            return

        width = max(1, self.width())
        height = max(1, self.height())
        aspect = width / height
        cols = max(1, int(math.ceil(math.sqrt(self.total * aspect))))
        rows = max(1, int(math.ceil(self.total / cols)))

        self._img_cols = cols
        self._img = QImage(cols, rows, QImage.Format.Format_ARGB32)
        self._img.fill(GRAY_RGB)
        for index, color in enumerate(self._colors):
            self._set_pixel(index, color)

    def _set_pixel(self, index, color):
        if self._img is None or self._img_cols == 0:
            return
        col = index % self._img_cols
        row = index // self._img_cols
        if 0 <= row < self._img.height():
            self._img.setPixel(col, row, color)

    def _schedule_repaint(self):
        if not self._repaint_pending:
            self._repaint_pending = True
            self._repaint_timer.start()

    def _do_repaint(self):
        self._repaint_pending = False
        self.update()

    def _index_at(self, position):
        if self._img is None or self.total == 0 or self._img_cols <= 0:
            return None
        if not self.rect().contains(position):
            return None

        width = self.width()
        height = self.height()
        if width <= 0 or height <= 0:
            return None

        x = max(0, min(width - 1, position.x()))
        y = max(0, min(height - 1, position.y()))
        col = min(self._img_cols - 1, int(x * self._img_cols / width))
        row = min(self._img.height() - 1, int(y * self._img.height() / height))
        index = row * self._img_cols + col
        if 0 <= index < self.total:
            return index
        return None

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self.total > 0:
            self._rebuild_image()
            self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        if self.total == 0 or self._img is None:
            painter.fillRect(self.rect(), self.palette().window())
            painter.end()
            return

        painter.setRenderHint(
            QPainter.RenderHint.SmoothPixmapTransform, False
        )
        painter.drawImage(self.rect(), self._img)
        painter.end()

    def mouseMoveEvent(self, event):
        index = self._index_at(event.position().toPoint())
        if index != self._hover_index:
            self._hover_index = index
            if index is None:
                QToolTip.hideText()
            else:
                QToolTip.showText(
                    event.globalPosition().toPoint(),
                    tooltip_for(index, self._stage1_entries, self._stage2_entries),
                    self,
                )
        super().mouseMoveEvent(event)

    def leaveEvent(self, event):
        self._hover_index = None
        QToolTip.hideText()
        super().leaveEvent(event)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self._logger = logging.getLogger(__name__)
        self._version = read_version()
        self._stage1_entries = []
        self._stage2_entries = []
        self._bad_files = []
        self._sectors_dir = None
        self._loaded = False
        self._current_view = "write"

        self.setWindowTitle(APP_TITLE)
        self.setWindowIcon(QIcon(get_running_path(ICON_FILE)))
        self.setMinimumSize(800, 650)
        self.resize(1050, 850)

        self._build_ui()
        self._set_analysis_enabled(False)

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        header_layout = QHBoxLayout()
        header_layout.addWidget(
            QLabel(
                "Stage JSONs load from this app folder; "
                "Target Path is used for BAD renames."
            )
        )
        header_layout.addStretch()
        if self._version:
            header_layout.addWidget(QLabel(f"v{self._version}"))
        main_layout.addLayout(header_layout)

        path_layout = QHBoxLayout()
        path_layout.addWidget(QLabel("Target Path:"))
        self.path_input = QLineEdit()
        self.path_input.setPlaceholderText(
            "Select the target disk/folder containing sectors..."
        )
        path_layout.addWidget(self.path_input)
        self.browse_btn = QPushButton("Browse")
        self.browse_btn.clicked.connect(self._browse)
        path_layout.addWidget(self.browse_btn)
        self.open_btn = QPushButton("Open")
        self.open_btn.setToolTip(
            "Load stage1.json and stage2.json from the app folder"
        )
        self.open_btn.clicked.connect(self._open_target)
        path_layout.addWidget(self.open_btn)
        main_layout.addLayout(path_layout)

        view_layout = QHBoxLayout()
        view_layout.addWidget(QLabel("View:"))
        self.write_btn = QPushButton("Write")
        self.write_btn.setCheckable(True)
        self.read_btn = QPushButton("Read")
        self.read_btn.setCheckable(True)
        self.view_group = QButtonGroup(self)
        self.view_group.setExclusive(True)
        self.view_group.addButton(self.write_btn)
        self.view_group.addButton(self.read_btn)
        self.write_btn.setChecked(True)
        self.write_btn.clicked.connect(lambda: self._set_view("write"))
        self.read_btn.clicked.connect(lambda: self._set_view("read"))
        view_layout.addWidget(self.write_btn)
        view_layout.addWidget(self.read_btn)
        view_layout.addStretch()
        main_layout.addLayout(view_layout)

        splitter = QSplitter(Qt.Orientation.Vertical)
        self.grid_widget = SectorGridWidget()
        splitter.addWidget(self.grid_widget)

        bottom_widget = QWidget()
        bottom_layout = QVBoxLayout(bottom_widget)
        bottom_layout.setContentsMargins(0, 0, 0, 0)

        thresholds_group = QGroupBox("Classification Thresholds")
        thresholds_layout = QGridLayout(thresholds_group)
        thresholds_layout.addWidget(QLabel("Min Write Speed (MB/s):"), 0, 0)
        self.min_write_speed_spin = self._make_speed_spin(1.0)
        thresholds_layout.addWidget(self.min_write_speed_spin, 0, 1)
        thresholds_layout.addWidget(QLabel("Max Write Time (s):"), 0, 2)
        self.max_write_time_spin = self._make_time_spin(30.0)
        thresholds_layout.addWidget(self.max_write_time_spin, 0, 3)
        thresholds_layout.addWidget(QLabel("Min Read Speed (MB/s):"), 1, 0)
        self.min_read_speed_spin = self._make_speed_spin(1.0)
        thresholds_layout.addWidget(self.min_read_speed_spin, 1, 1)
        thresholds_layout.addWidget(QLabel("Max Read Time (s):"), 1, 2)
        self.max_read_time_spin = self._make_time_spin(30.0)
        thresholds_layout.addWidget(self.max_read_time_spin, 1, 3)

        self.ghost_bad_checkbox = QCheckBox("Ghost Files → BAD")
        self.ghost_bad_checkbox.setChecked(True)
        thresholds_layout.addWidget(self.ghost_bad_checkbox, 2, 0, 1, 2)
        self.read_failure_bad_checkbox = QCheckBox("Read Failures → BAD")
        self.read_failure_bad_checkbox.setChecked(True)
        thresholds_layout.addWidget(self.read_failure_bad_checkbox, 2, 2)
        self.hash_mismatch_bad_checkbox = QCheckBox("Hash Mismatches → BAD")
        self.hash_mismatch_bad_checkbox.setChecked(True)
        thresholds_layout.addWidget(self.hash_mismatch_bad_checkbox, 2, 3)
        thresholds_layout.setColumnStretch(1, 1)
        thresholds_layout.setColumnStretch(3, 1)
        bottom_layout.addWidget(thresholds_group)

        self.counts_label = QLabel("Total: 0 | GOOD: 0 | BAD: 0 | Ghosts: 0")
        font = self.counts_label.font()
        font.setBold(True)
        self.counts_label.setFont(font)
        bottom_layout.addWidget(self.counts_label)

        bottom_layout.addWidget(QLabel("BAD files (read-only review list):"))
        self.bad_list = QListWidget()
        self.bad_list.setSelectionMode(
            QAbstractItemView.SelectionMode.SingleSelection
        )
        self.bad_list.setEditTriggers(
            QAbstractItemView.EditTrigger.NoEditTriggers
        )
        self.bad_list.setAlternatingRowColors(True)
        bottom_layout.addWidget(self.bad_list, stretch=1)

        button_layout = QHBoxLayout()
        self.status_label = QLabel("No data loaded.")
        button_layout.addWidget(self.status_label)
        button_layout.addStretch()
        self.confirm_btn = QPushButton("Confirm")
        self.confirm_btn.clicked.connect(self._on_confirm)
        button_layout.addWidget(self.confirm_btn)
        bottom_layout.addLayout(button_layout)

        splitter.addWidget(bottom_widget)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        main_layout.addWidget(splitter, stretch=1)

        self.min_write_speed_spin.valueChanged.connect(self._recompute)
        self.max_write_time_spin.valueChanged.connect(self._recompute)
        self.min_read_speed_spin.valueChanged.connect(self._recompute)
        self.max_read_time_spin.valueChanged.connect(self._recompute)
        self.ghost_bad_checkbox.toggled.connect(self._recompute)
        self.read_failure_bad_checkbox.toggled.connect(self._recompute)
        self.hash_mismatch_bad_checkbox.toggled.connect(self._recompute)

    @staticmethod
    def _make_speed_spin(value):
        spin = QDoubleSpinBox()
        spin.setRange(0.0, 100000.0)
        spin.setDecimals(1)
        spin.setSingleStep(0.1)
        spin.setValue(value)
        return spin

    @staticmethod
    def _make_time_spin(value):
        spin = QDoubleSpinBox()
        spin.setRange(0.01, 86400.0)
        spin.setDecimals(2)
        spin.setSingleStep(1.0)
        spin.setValue(value)
        return spin

    # --------------------------------------------------------------- browse/open
    def _browse(self):
        folder = QFileDialog.getExistingDirectory(self, "Select Target Folder")
        if folder:
            self.path_input.setText(folder)

    def _open_target(self):
        target_path = self.path_input.text().strip()
        if not target_path or not os.path.isdir(target_path):
            self._clear_loaded_data()
            self._set_analysis_enabled(False)
            self.status_label.setText("Load failed.")
            QMessageBox.critical(
                self,
                "Cannot Open",
                f"Target path is not a valid folder:\n{target_path}\n\n"
                "Select the target folder/disk that contains the sectors folder.",
            )
            return

        try:
            stage1_entries, stage2_entries, stage1_path, stage2_path = (
                load_stage_data(get_app_root())
            )
        except StageDataError as exc:
            self._clear_loaded_data()
            self._set_analysis_enabled(False)
            self.status_label.setText("Load failed.")
            QMessageBox.critical(self, "Cannot Open", str(exc))
            return

        self._stage1_entries = stage1_entries
        self._stage2_entries = stage2_entries
        self._sectors_dir = os.path.join(target_path, "sectors")
        self._loaded = True
        self._set_analysis_enabled(True)
        self._recompute()
        self._refresh_grid()

        status = (
            f"Loaded {len(stage1_entries)} files from {get_app_root()}; "
            f"target: {target_path}"
        )
        if not os.path.isdir(self._sectors_dir):
            status += " (sectors folder not found)"
        self.status_label.setText(status)
        self._logger.info(
            "Loaded %s and %s (%d entries); target=%s",
            stage1_path,
            stage2_path,
            len(stage1_entries),
            target_path,
        )

    def _clear_loaded_data(self):
        self._loaded = False
        self._stage1_entries = []
        self._stage2_entries = []
        self._bad_files = []
        self._sectors_dir = None
        self.grid_widget.clear()
        self.bad_list.clear()
        self.counts_label.setText("Total: 0 | GOOD: 0 | BAD: 0 | Ghosts: 0")

    def _set_analysis_enabled(self, enabled):
        self.write_btn.setEnabled(enabled)
        self.read_btn.setEnabled(enabled)
        self.min_write_speed_spin.setEnabled(enabled)
        self.max_write_time_spin.setEnabled(enabled)
        self.min_read_speed_spin.setEnabled(enabled)
        self.max_read_time_spin.setEnabled(enabled)
        self.ghost_bad_checkbox.setEnabled(enabled)
        self.read_failure_bad_checkbox.setEnabled(enabled)
        self.hash_mismatch_bad_checkbox.setEnabled(enabled)
        self.grid_widget.setEnabled(enabled)
        self.bad_list.setEnabled(enabled)
        self.confirm_btn.setEnabled(enabled)

    # --------------------------------------------------------------- views
    def _set_view(self, view):
        self._current_view = view
        self.write_btn.setChecked(view == "write")
        self.read_btn.setChecked(view == "read")
        if self._loaded:
            self._refresh_grid()

    def _refresh_grid(self):
        self.grid_widget.set_data(
            self._stage1_entries, self._stage2_entries, self._current_view
        )

    # --------------------------------------------------------------- classification
    @Slot()
    def _recompute(self):
        if not self._loaded:
            return

        (
            total,
            good,
            bad,
            ghosts,
            self._bad_files,
        ) = classify_entries(
            self._stage1_entries,
            self._stage2_entries,
            self.min_write_speed_spin.value(),
            self.max_write_time_spin.value(),
            self.min_read_speed_spin.value(),
            self.max_read_time_spin.value(),
            self.ghost_bad_checkbox.isChecked(),
            self.read_failure_bad_checkbox.isChecked(),
            self.hash_mismatch_bad_checkbox.isChecked(),
        )

        self.counts_label.setText(
            f"Total: {total} | GOOD: {good} | BAD: {bad} | Ghosts: {ghosts}"
        )
        self.bad_list.setUpdatesEnabled(False)
        self.bad_list.clear()
        if self._bad_files:
            self.bad_list.addItems(self._bad_files)
        self.bad_list.setUpdatesEnabled(True)

    # --------------------------------------------------------------- confirm
    def _on_confirm(self):
        if not self._loaded:
            return

        self._recompute()
        if not self._bad_files:
            QMessageBox.information(
                self, "Nothing to Rename", "Nothing to BAD rename"
            )
            QApplication.quit()
            return

        total_bad = len(self._bad_files)
        result = rename_bad_files(self._sectors_dir, self._bad_files)
        summary_lines = [
            f"Renamed {result['renamed']} of {total_bad} files."
        ]
        if result["already"]:
            summary_lines.append(f"Already renamed: {result['already']}.")
        if result["failures"]:
            summary_lines.append(f"Failed: {len(result['failures'])}.")
            summary_lines.append("")
            for filename, error in result["failures"][:20]:
                summary_lines.append(f"{filename}: {error}")
            if len(result["failures"]) > 20:
                summary_lines.append("...")
            self.status_label.setText(
                f"Rename incomplete: {len(result['failures'])} failure(s)."
            )
            QMessageBox.warning(
                self, "Rename Summary", "\n".join(summary_lines)
            )
            return

        self.status_label.setText(
            f"Renamed {result['renamed']} file(s). Closing..."
        )
        QMessageBox.information(
            self, "Rename Summary", "\n".join(summary_lines)
        )
        QApplication.quit()


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
