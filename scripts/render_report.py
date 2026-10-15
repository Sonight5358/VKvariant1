"""Compatibility entry point for the current compact, two-session report."""
import runpy
import sys
from pathlib import Path

if len(sys.argv) != 1:
    raise SystemExit('Use render_standalone_report.py without arguments; it reads main, repeat1 and review1.')
runpy.run_path(str(Path(__file__).with_name('render_standalone_report.py')), run_name='__main__')
