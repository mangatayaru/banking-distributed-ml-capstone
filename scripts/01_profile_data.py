
from pathlib import Path
import json
import pandas as pd

# Project root is the parent of the scripts folder
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = PROJECT_ROOT / "data" / "bank.csv"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

def main():
    print("=" * 60)
    print("BANKING DATASET PROFILE")
    print("=" * 60)

    # Verify that the dataset exists
    if not DATA_FILE.exists():
        print(f"\nERROR: Dataset not found:\n{DATA_FILE}")
        print("\nExpected structure:")
        print("project_folder/")
        print("  data/")
        print("    bank.csv")
        return

    # Load the dataset
    df = pd.read_csv(DATA_FILE, sep=None, engine="python")

    print(f"\nDataset path: {DATA_FILE}")
    print(f"Number of rows: {df.shape[0]}")
    print(f"Number of columns: {df.shape[1]}")

    print("\nColumn names:")
    print(df.columns.tolist())

    print("\nData types:")
    print(df.dtypes.astype(str).to_dict())

    print("\nMissing values by column:")
    missing = df.isnull().sum()
    print(missing.to_string())

    print("\nTarget distribution:")
    if "y" in df.columns:
        print(df["y"].value_counts(dropna=False).to_string())
        print("\nTarget distribution (%):")
        print(
            (df["y"].value_counts(normalize=True) * 100)
            .round(2)
            .to_string()
        )
    else:
        print("Target column 'y' was not found.")

    # Save reproducible outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    profile = {
        "rows": int(df.shape[0]),
        "columns": int(df.shape[1]),
        "column_names": df.columns.tolist(),
        "missing_values_total": int(df.isnull().sum().sum()),
        "missing_values_by_column": {
            str(k): int(v) for k, v in missing.items()
        },
        "target_distribution": (
            {str(k): int(v) for k, v in
             df["y"].value_counts(dropna=False).items()}
            if "y" in df.columns else {}
        ),
    }

    output_file = OUTPUT_DIR / "data_profile.json"
    output_file.write_text(
        json.dumps(profile, indent=2),
        encoding="utf-8"
    )

    print(f"\nProfile saved to: {output_file}")
    print("\nDataset profiling completed successfully.")

if __name__ == "__main__":
    main()
