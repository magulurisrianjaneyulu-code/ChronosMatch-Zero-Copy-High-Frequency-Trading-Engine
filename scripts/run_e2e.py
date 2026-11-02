import multiprocessing
import os
import sys
import time
import uvicorn

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)


def start_server():
    uvicorn.run("server:app", host="127.0.0.1", port=8000, log_level="warning")


if __name__ == "__main__":
    from scripts.test_live_server import run_tests

    proc = multiprocessing.Process(target=start_server, daemon=True)
    proc.start()
    time.sleep(2.0)

    try:
        success = run_tests()
        if not success:
            sys.exit(1)
    finally:
        proc.terminate()
        proc.join(timeout=2.0)
