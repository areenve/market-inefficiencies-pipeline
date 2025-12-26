import subprocess

def main():
    subprocess.run(["python", "scripts/detect_events.py", "--lookback-min", "720", "--threshold-bps", "6", "--persistence-ms", "600"], check=True)
    subprocess.run(["python", "scripts/backtest.py", "--lookback-min", "720"], check=True)

if __name__ == "__main__":
    main()


