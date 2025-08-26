import argparse
import csv
import gzip
import os
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Iterable, Tuple


def _rand_name(rnd: random.Random) -> str:
	first = rnd.choice(["John", "Jane", "Alex", "Olivia", "Liam", "Emma", "Noah", "Ava", "Mason", "Sophia"]) 
	last = rnd.choice(["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]) 
	return f"{first} {last}"


def _rand_email(rnd: random.Random, name: str) -> str:
	user = "".join(ch for ch in name.lower() if ch.isalnum())
	domain = rnd.choice(["example.com", "mail.com", "test.org", "sample.net"]) 
	num = rnd.randint(1, 9999)
	return f"{user}{num}@{domain}"


def _rand_amount(rnd: random.Random) -> float:
	return round(rnd.uniform(1.0, 10000.0), 2)


def _rand_date(rnd: random.Random) -> str:
	start = datetime.now(timezone.utc) - timedelta(days=3650)
	delta = timedelta(seconds=rnd.randint(0, 3650 * 24 * 3600))
	return (start + delta).strftime("%Y-%m-%d")


def _generate_rows(num_rows: int, rnd: random.Random) -> Iterable[Tuple[int, str, str, float, str]]:
	for i in range(1, num_rows + 1):
		name = _rand_name(rnd)
		yield (
			i,
			name,
			_echo_safe(_rand_email(rnd, name)),
			_rand_amount(rnd),
			_rand_date(rnd),
		)


def _echo_safe(s: str) -> str:
	# Avoid problematic characters for CSV tests; keep it simple
	return s.replace("\n", " ").replace("\r", " ")


def generate_csv(output_path: str, num_rows: int, delimiter: str = ",", include_header: bool = True, seed: int | None = None) -> None:
	os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
	rnd = random.Random(seed)

	open_fn = gzip.open if output_path.endswith(".gz") else open
	mode = "wt" if output_path.endswith(".gz") else "w"

	with open_fn(output_path, mode, encoding="utf-8", newline="") as f:  # type: ignore[arg-type]
		writer = csv.writer(f, delimiter=delimiter, lineterminator="\n")
		if include_header:
			writer.writerow(["id", "name", "email", "amount", "date"])
		for row in _generate_rows(num_rows, rnd):
			writer.writerow(row)


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Generate a large CSV file with synthetic data")
	parser.add_argument("--output", "-o", required=True, help="Output CSV path (use .gz for gzip)")
	parser.add_argument("--rows", "-r", type=int, required=True, help="Number of rows to generate")
	parser.add_argument("--delimiter", "-d", default=",", help="CSV delimiter (default ',')")
	parser.add_argument("--no-header", action="store_true", help="Do not write header row")
	parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	generate_csv(
		output_path=args.output,
		num_rows=args.rows,
		delimiter=args.delimiter,
		include_header=not args.no_header,
		seed=args.seed,
	)


if __name__ == "__main__":
	main()
