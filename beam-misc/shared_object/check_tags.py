import argparse
import time
from datetime import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tag bucketization demo")
    parser.add_argument(
        "--max_stale_sec",
        "-m",
        type=int,
        default=5,
        help="Maximum second of staleness.",
    )
    args = parser.parse_args()
    max_stale_sec = args.max_stale_sec

    while True:
        current_ts = datetime.now().timestamp()
        print(
            f"current {int(current_ts)}, tag {current_ts - (current_ts % max_stale_sec)}, substraction {current_ts % max_stale_sec}"
        )
        time.sleep(1)
