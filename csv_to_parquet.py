import argparse
from pathlib import Path
import dask.dataframe as dd


def csv_to_parquet(
    csv_path: Path,
    output_dir: Path,
    blocksize: str = "256MB"
):
    """
    Convert a CSV file to a Parquet dataset using Dask.
    """

    print(f"Reading CSV: {csv_path}")
    print(f"Writing Parquet to: {output_dir}")
    print(f"Blocksize: {blocksize}")

    # Read CSV lazily in parallel
    ddf = dd.read_csv(
        csv_path,
        blocksize=blocksize,
        assume_missing=True
    )

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write Parquet dataset
    ddf.to_parquet(
        output_dir,
        engine="pyarrow",
        compression="snappy",
        write_index=False
    )

    print("Conversion complete.")


def main():
    parser = argparse.ArgumentParser(
        description="Convert large CSV files to Parquet using Dask"
    )
    parser.add_argument("csv", type=Path, help="Path to input CSV file")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("parquet_output"),
        help="Output directory for Parquet dataset"
    )
    parser.add_argument(
        "--blocksize",
        type=str,
        default="256MB",
        help="Dask blocksize (e.g. 128MB, 256MB, 512MB)"
    )

    args = parser.parse_args()

    if not args.csv.exists():
        raise FileNotFoundError(f"CSV not found: {args.csv}")

    csv_to_parquet(args.csv, args.out, args.blocksize)


if __name__ == "__main__":
    main()
