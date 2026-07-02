import argparse
import polars as pl
from pathlib import Path


def parquet_glob(path: str) -> str:
    p = Path(path)
    if p.is_file():
        return str(p)
    return str(p / "**" / "*.parquet")


def main():
    parser = argparse.ArgumentParser(
        description="Merge bus/area metadata into branch parquet files using Polars."
    )

    parser.add_argument("--branches", required=True, help="Path to branch parquet file/folder")
    parser.add_argument("--metadata", required=True, help="Path to bus metadata parquet file/folder")
    parser.add_argument("--output", default="merged_output.parquet", help="Output parquet file")

    parser.add_argument("--from-col", default="from_bus", help="Branch FROM bus column")
    parser.add_argument("--to-col", default="to_bus", help="Branch TO bus column")
    parser.add_argument("--meta-key", default="bus_id", help="Metadata bus ID column")

    parser.add_argument(
        "--meta-cols",
        nargs="+",
        default=["area", "area_name"],
        help="Metadata columns to add, e.g. area area_name zone",
    )

    args = parser.parse_args()

    print("Scanning branch parquet...")
    branches = pl.scan_parquet(parquet_glob(args.branches))

    print("Scanning metadata parquet...")
    metadata = pl.scan_parquet(parquet_glob(args.metadata))

    print("Branch schema:")
    print(branches.collect_schema())

    print("Metadata schema:")
    print(metadata.collect_schema())

    from_meta = metadata.select(
        [pl.col(args.meta_key).cast(pl.Int64).alias(args.from_col)]
        + [
            pl.col(c).alias(f"from_{c}")
            for c in args.meta_cols
        ]
    ).drop_nulls([args.from_col]).unique(subset=[args.from_col])

    to_meta = metadata.select(
        [pl.col(args.meta_key).cast(pl.Int64).alias(args.to_col)]
        + [
            pl.col(c).alias(f"to_{c}")
            for c in args.meta_cols
        ]
    ).drop_nulls([args.to_col]).unique(subset=[args.to_col])

    print("Joining metadata...")

    merged = (
        branches
        .with_columns([
            pl.col(args.from_col).cast(pl.Int64),
            pl.col(args.to_col).cast(pl.Int64),
        ])
        .join(from_meta, on=args.from_col, how="left")
        .join(to_meta, on=args.to_col, how="left")
    )

    print("Writing output parquet...")
    merged.sink_parquet(args.output)

    print("Done.")
    print(f"Output written to: {args.output}")


if __name__ == "__main__":
    main()
