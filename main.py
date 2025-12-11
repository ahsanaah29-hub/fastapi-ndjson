import nest_asyncio
nest_asyncio.apply()

from fastapi import FastAPI, UploadFile, File
import uvicorn
import json, os, threading

app = FastAPI()

# ---------------------------
# Robust voucher extractor (handles the structure in your DayBook.json)
# ---------------------------
def _get_case_insensitive(d: dict, *keys):
    """Return value for first matching key in d (case-insensitive / variations)."""
    for k in keys:
        if not k:
            continue
        # exact
        if k in d:
            return d[k]
        # uppercase version
        ku = k.upper()
        if ku in d:
            return d[ku]
        # lowercase
        kl = k.lower()
        if kl in d:
            return d[kl]
    return None

def extract_vouchers(json_data):
    messages = json_data.get("tallymessage", [])
    rows = []

    for v in messages:
        # voucher-level wrapper: sometimes values are directly inside v (your file)
        # Build base fields
        base = {
            "date": _get_case_insensitive(v, "date"),
            "voucher_number": _get_case_insensitive(v, "vouchernumber", "voucherkey"),
            "voucher_type": _get_case_insensitive(v, "vouchertypename", "vchtype"),
            "narration": _get_case_insensitive(v, "narration"),
            "party": _get_case_insensitive(v, "partyname", "partyledgername"),
            "guid": _get_case_insensitive(v, "guid")
        }

        ledger_blocks = []

        # Common places ledger entries may appear at voucher-level
        for key in ("accountingallocations", "ledgerentries", "allledgerentries", "ALLLEDGERENTRIES.LIST", "allledgerentries_list"):
            val = _get_case_insensitive(v, key)
            if isinstance(val, list):
                ledger_blocks.extend(val)

        # Also check inside inventory/item entries (allinventoryentries)
        inv_items = _get_case_insensitive(v, "allinventoryentries", "allinventoryentries_list")
        if isinstance(inv_items, list):
            for item in inv_items:
                for key in ("accountingallocations", "allledgerentries", "ledgerentries"):
                    val = _get_case_insensitive(item, key)
                    if isinstance(val, list):
                        ledger_blocks.extend(val)

                # some items have batchallocations -> accountingallocations
                batch_allocs = _get_case_insensitive(item, "batchallocations")
                if isinstance(batch_allocs, list):
                    for b in batch_allocs:
                        val = _get_case_insensitive(b, "accountingallocations")
                        if isinstance(val, list):
                            ledger_blocks.extend(val)

        # Also try nested structures like 'voucher' wrapper inside message item
        inner_voucher = _get_case_insensitive(v, "voucher")
        if isinstance(inner_voucher, dict):
            for key in ("accountingallocations", "ledgerentries", "allledgerentries", "allledgerentries_list"):
                val = _get_case_insensitive(inner_voucher, key)
                if isinstance(val, list):
                    ledger_blocks.extend(val)
            inv_items2 = _get_case_insensitive(inner_voucher, "allinventoryentries")
            if isinstance(inv_items2, list):
                for item in inv_items2:
                    val = _get_case_insensitive(item, "accountingallocations")
                    if isinstance(val, list):
                        ledger_blocks.extend(val)

        # If nothing found, try 'ledgerentries' plural again (defensive)
        if not ledger_blocks:
            maybe = _get_case_insensitive(v, "ledgerentries")
            if isinstance(maybe, list):
                ledger_blocks.extend(maybe)

        # Build rows — one per ledger entry
        for entry in ledger_blocks:
            if not isinstance(entry, dict):
                continue
            ledger_name = _get_case_insensitive(entry, "ledgername", "LEDGERNAME", "ledger")
            amount_raw = _get_case_insensitive(entry, "amount", "AMOUNT", "value")
            # try to normalize amount to number when possible
            amount = None
            if isinstance(amount_raw, (int, float)):
                amount = float(amount_raw)
            elif isinstance(amount_raw, str):
                try:
                    # remove commas/spaces
                    amount = float(amount_raw.replace(",", "").strip())
                except:
                    # maybe negative in parentheses or other formats — keep string
                    try:
                        amount = float(amount_raw.replace("(", "-").replace(")", "").replace(",", "").strip())
                    except:
                        amount = None

            row = {
                "date": base["date"],
                "voucher_number": base["voucher_number"],
                "voucher_type": base["voucher_type"],
                "narration": base["narration"],
                "party": base["party"],
                "ledger_name": ledger_name,
                "amount": amount if amount is not None else amount_raw,
                "guid": base.get("guid")
            }
            rows.append(row)

    return rows

# ---------------------------
# NDJSON output folder
# ---------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # absolute folder where this .py file is located
NDJSON_FOLDER = os.path.join(BASE_DIR, "ndjson_output")
os.makedirs(NDJSON_FOLDER, exist_ok=True)

# ---------------------------
# API Endpoint
# ---------------------------
@app.post("/convert-daybook-ndjson")
async def convert_daybook_ndjson(file: UploadFile = File(...)):
    try:
        raw = await file.read()
        json_data = json.loads(raw)

        rows = extract_vouchers(json_data)

        if not rows:
            return {"status": "error", "message": "No voucher rows extracted"}

        out_file = file.filename.replace(".json", "") + ".ndjson"
        out_path = os.path.join(NDJSON_FOLDER, out_file)

        # Write NDJSON file
        with open(out_path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        return {
            "status": "success",
            "rows_created": len(rows),
            "ndjson_file": out_path
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

# ---------------------------
# Start server on Port 8001
# ---------------------------


