"""Сборка ПанельСтрима.exe (один файл, без консоли): python build.py -> dist/ПанельСтрима.exe"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent
subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "--upgrade", "pyinstaller"])
subprocess.check_call([
    sys.executable, "-m", "PyInstaller", "--noconfirm", "--onefile", "--noconsole",
    "--name", "ПанельСтрима",
    "--icon", str(ROOT / "icon.ico"),
    "--add-data", f"{ROOT / 'index.html'};.",
    "--distpath", str(ROOT / "dist"), "--workpath", str(ROOT / "build"), "--specpath", str(ROOT / "build"),
    str(ROOT / "server.py"),
])
print("\nГотово:", ROOT / "dist" / "ПанельСтрима.exe")
