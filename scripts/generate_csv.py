#!/usr/bin/env python3
"""
Utility to generate sample CSV files compatible with the application's CSV validator.
The produced CSV has headers: id,name,value
"""
import argparse
import csv
import os
import random
import sys
from typing import List


DEFAULT_NAMES: List[str] = [
	"Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy",
	"Mallory", "Niaj", "Olivia", "Peggy", "Sybil", "Trent", "Victor", "Wendy"
]


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description="Generate a CSV file with columns: id,name,value"
	)
	parser.add_argument(
		"-o", "--out",
		dest="output_path",
		help="Output CSV file path",
		default="sample.csv"
	)
	parser.add_argument(
		"-n", "--rows",
		dest="num_rows",
		type=int,
		help="Number of data rows to generate",
		default=1000
	)
	parser.add_argument(
		"--min",
		dest="min_value",
		type=int,
		help="Minimum value for the 'value' column",
		default=1
	)
	parser.add_argument(
		"--max",
		dest="max_value",
		type=int,
		help="Maximum value for the 'value' column",
		default=1000
	)
	parser.add_argument(
		"--names",
		dest="names",
		nargs="*",
		help="Override list of names to sample from (space-separated)",
		default=None
	)
	parser.add_argument(
		"--seed",
		dest="seed",
		type=int,
		help="Random seed for reproducibility",
		default=None
	)
	return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
	if args.num_rows < 0:
		raise ValueError("--rows must be >= 0")
	if args.min_value > args.max_value:
		raise ValueError("--min cannot be greater than --max")
	if args.names is not None and len(args.names) == 0:
		raise ValueError("--names provided but empty")


def main() -> int:
	args = parse_args()
	try:
		validate_args(args)
	except Exception as exc:
		print(f"Invalid arguments: {exc}", file=sys.stderr)
		return 2

	if args.seed is not None:
		random.seed(args.seed)

	names: List[str] = args.names if args.names else DEFAULT_NAMES

	output_dir = os.path.dirname(os.path.abspath(args.output_path)) or "."
	os.makedirs(output_dir, exist_ok=True)

	with open(args.output_path, "w", newline="", encoding="utf-8") as fh:
		writer = csv.writer(fh)
		writer.writerow(["id", "name", "value"])  # required headers for validator
		for i in range(1, args.num_rows + 1):
			writer.writerow([
				i,
				random.choice(names),
				random.randint(args.min_value, args.max_value),
			])

	print(f"CSV generated: {args.output_path} ({args.num_rows} rows)")
	return 0


if __name__ == "__main__":
	sys.exit(main()) 