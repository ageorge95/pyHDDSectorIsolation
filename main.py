import ctypes
import sys

from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication

from frontend import MainWindow, get_running_path
from ag95 import configure_logger

APP_ID = 'AG95.pyHDDSectorIsolation'
ICON_FILE = 'icon.ico'


def set_windows_app_id():
    """Give the app its own Windows taskbar identity."""
    if sys.platform != 'win32':
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


if __name__ == '__main__':
    main()
