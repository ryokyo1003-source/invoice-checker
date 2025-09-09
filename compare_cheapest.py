#!/usr/bin/env python3
import os, pandas as pd, numpy as np
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
DEFAULT_TAX_RATE = float(os.getenv("DEFAULT_TAX_RATE", "0.10"))

INPUT = Path("output/all_lines_combined.csv")
MAP   = Path("map_dictionary.csv")
OUT_DIR = Path("output")
OUT_DIR.mkdir(exist_ok=True)

def load_map():
    if MAP.exists():
        m = pd.read_csv(MAP)
        m["raw_name"] = m["raw_name"].astype(str).str.strip()
        return m
    return pd.DataFrame(columns=["raw_name","normalized_name","unit_hint","pack_size_hint"])

def normalize(df, m):
    df["normalized_name"] = df["raw_desc"].fillna("").astype(str).str.strip()
    if not m.empty:
        repl = dict(zip(m["raw_name"], m["normalized_name"]))
        df["normalized_name"] = df["normalized_name"].map(lambda x: repl.get(x, x))
    return df

def coerce_numbers(df):
    def to_num(x):
        if pd.isna(x):
            return np.nan
        s = str(x).replace(",", "").replace("¥","").strip()
        try:
            return float(s)
        except:
            return np.nan
    for col in ["qty","unit_price","amount","tax_rate"]:
        df[col] = df[col].map(to_num)
    df["amount"]      = df["amount"].fillna(df["qty"] * df["unit_price"])
    df["unit_price"]  = df["unit_price"].fillna(df["amount"] / df["qty"])
    df["tax_rate"]    = df["tax_rate"].fillna(DEFAULT_TAX_RATE)
    return df

def main():
    if not INPUT.exists():
        print("Run process_invoices_textract.py first.")
        return
    df = pd.read_csv(INPUT)
    m  = load_map()
    df = normalize(df, m)
    df = coerce_numbers(df)

    # とりあえず金額/数量で実効単価（税別/包装換算の高度化は次段）
    df["effective_unit_price"] = df["amount"] / df["qty"]

    agg = (df.dropna(subset=["normalized_name","effective_unit_price"])
             .sort_values(by=["normalized_name","effective_unit_price"])
             .groupby("normalized_name")
             .first()
             .reset_index())
    agg = agg[["normalized_name","supplier","effective_unit_price"]]
    agg = agg.rename(columns={"supplier":"cheapest_supplier",
                              "effective_unit_price":"cheapest_unit_price"})
    agg.to_csv(OUT_DIR / "cheapest_by_item.csv", index=False)
    print("Wrote:", OUT_DIR / "cheapest_by_item.csv")

if __name__ == "__main__":
    main()
