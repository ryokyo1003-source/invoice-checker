#!/usr/bin/env python3
import os, sys
import pandas as pd
import boto3
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-1")
textract = boto3.client("textract", region_name=AWS_REGION)

INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Textractの項目名ゆらぎを吸収する簡易マップ
FIELD_MAPPINGS = {
    "qty":        ["Quantity", "数量", "数", "Qty"],
    "unit_price": ["UnitPrice", "単価", "単価(税抜)", "単価(税込)"],
    "amount":     ["Amount", "金額", "合計", "金額(税抜)", "金額(税込)"],
    "desc":       ["Item", "Description", "品名", "商品名", "規格", "品名・規格"],
    "unit":       ["UnitCode", "単位"],
    "taxrate":    ["TaxRate", "消費税率", "Tax %"],
}

def choose_field(fields, names):
    for cand in names:
        if cand in fields:
            return fields[cand]
    return None

def parse_expense(doc_bytes):
    resp = textract.analyze_expense(Document={"Bytes": doc_bytes})
    rows = []
    for doc in resp.get("ExpenseDocuments", []):
        header = {}
        for field in doc.get("SummaryFields", []):
            t = (field.get("Type") or {}).get("Text")
            v = (field.get("ValueDetection") or {}).get("Text")
            if t and v:
                header[t] = v

        vendor   = header.get("VENDOR_NAME") or header.get("SUPPLIER_NAME") or header.get("RECEIVER_NAME")
        inv_date = header.get("INVOICE_RECEIPT_DATE") or header.get("INVOICE_DATE")

        for group in doc.get("LineItemGroups", []):
            for item in group.get("LineItems", []):
                fields = {}
                for f in item.get("LineItemExpenseFields", []):
                    t = (f.get("Type") or {}).get("Text")
                    v = (f.get("ValueDetection") or {}).get("Text")
                    if t and v:
                        fields[t] = v
                rows.append({
                    "supplier":   vendor,
                    "invoice_date": inv_date,
                    "raw_desc":   choose_field(fields, FIELD_MAPPINGS["desc"]),
                    "qty":        choose_field(fields, FIELD_MAPPINGS["qty"]),
                    "unit":       choose_field(fields, FIELD_MAPPINGS["unit"]),
                    "unit_price": choose_field(fields, FIELD_MAPPINGS["unit_price"]),
                    "amount":     choose_field(fields, FIELD_MAPPINGS["amount"]),
                    "tax_rate":   choose_field(fields, FIELD_MAPPINGS["taxrate"]),
                })
    return rows

def main():
    files = []
    for ext in ("*.pdf", "*.jpg", "*.jpeg", "*.png"):
        files += list(INPUT_DIR.glob(ext))
    if not files:
        print("Put invoice images/PDFs into input/ then run again.")
        sys.exit(0)

    all_rows = []
    for p in files:
        with open(p, "rb") as f:
            b = f.read()
        rows = parse_expense(b)
        pd.DataFrame(rows).to_csv(OUTPUT_DIR / f"lines_{p.stem}.csv", index=False)
        for r in rows:
            r["source_file"] = p.name
        all_rows.extend(rows)

    if all_rows:
        pd.DataFrame(all_rows).to_csv(OUTPUT_DIR / "all_lines_combined.csv", index=False)
        print("Wrote:", OUTPUT_DIR / "all_lines_combined.csv")

if __name__ == "__main__":
    main()
