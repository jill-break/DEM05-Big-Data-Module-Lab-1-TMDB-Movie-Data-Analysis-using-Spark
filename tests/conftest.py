import sys
import socketserver

# Monkeypatch for Windows: PySpark 3.5.0 tries to access UnixStreamServer even on Windows
if sys.platform == 'win32' and not hasattr(socketserver, 'UnixStreamServer'):
    class UnixStreamServer(socketserver.TCPServer):
        pass
    socketserver.UnixStreamServer = UnixStreamServer

import pytest
import os

# Forces Spark to use the virtual environment's Python
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
