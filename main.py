import sys
from PySide6.QtWidgets import QApplication
from frontend import MainWindow
from ag95 import configure_logger

def main():
    configure_logger()
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    main()