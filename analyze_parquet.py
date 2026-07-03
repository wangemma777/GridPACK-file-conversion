import polars as pl
from pathlib import Path


def load_dataset(parquet_dir: Path) -> pl.LazyFrame:
    """
    Lazily load a Parquet dataset using Polars.
    """
    return pl.scan_parquet(parquet_dir)


def example_analysis(lf: pl.LazyFrame):
    """
    Example transformations — customize this later.
    """
    return (
        lf
        .select(pl.all())
        .limit(10)
    )

def main(parquet_dir: Path):
    if not parquet_dir.exists():
        raise FileNotFoundError("Parquet directory not found")

    lf = load_dataset(parquet_dir)

    print("Schema:")
    print(lf.schema)

    print("\nSample rows:")
    df = example_analysis(lf).collect()
    print(df)


if __name__ == "__main__":
    main(Path("ca_parquet"))
