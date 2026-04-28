
import pandas as pd
import os

def process_year(year):
    path = f"data/raw/{year}/records.csv"
    if not os.path.exists(path):
        print(f"Missing file for {year}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return pd.DataFrame()

    if "fecha_adjudicacion" not in df.columns:
        return pd.DataFrame()

    df["fecha_adjudicacion"] = pd.to_datetime(df["fecha_adjudicacion"], errors="coerce")
    df["anio"] = df["fecha_adjudicacion"].dt.year
    df["monto"] = pd.to_numeric(df.get("monto", 0), errors="coerce").fillna(0)

    return df

def process_all(years):
    all_data = []
    for y in years:
        df = process_year(y)
        if not df.empty:
            all_data.append(df)

    if not all_data:
        print("No data processed")
        return

    final = pd.concat(all_data)

    result = final.groupby("anio").agg(
        cantidad=("monto", "count"),
        monto_total=("monto", "sum")
    ).reset_index()

    os.makedirs("data/processed", exist_ok=True)
    result.to_csv("data/processed/evolucion_anual.csv", index=False)

    print("Procesamiento completo")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", nargs="+", required=True)
    args = parser.parse_args()
    process_all(args.years)
