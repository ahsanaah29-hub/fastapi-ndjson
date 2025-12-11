import nest_asyncio
nest_asyncio.apply()

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
import uvicorn
import json, os, threading

app = FastAPI()

# ---------------------------
# Case-insensitive key fetch
# ---------------------------
def _get_case_insensitive(d: dict, *keys):
    for k in keys:
        if not k:
            continue
        if k in d:
            return d[k]
        if k.upper() in d:
            return d[k.upper()]
        if k.lower() in d:
            return d[k.lower()]
    return None

# ---------------------------
# Voucher extraction
# ---------------------------
def extract_vouchers(json_data):
    messages = json_data.get("tallymessage", [])
    rows = []

    for v in messages:
        base = {
            "date": _get_case_insensitive(v, "date"),
            "voucher_number": _get_case_insensitive(v, "vouchernumber", "voucherkey"),
            "voucher_type": _get_case_insensitive(v, "vouchertypename", "vchtype"),
            "narration": _get_case_insensitive(v, "narration"),
            "party": _get_case_insensitive(v, "partyname", "partyledgername"),
            "guid": _get_case_insensitive(v, "guid")
        }

        ledger_blocks = []

        for key in ("accountingallocations", "ledgerentries", "allledgerentries", "ALLLEDGERENTRIES.LIST", "allledgerentries_list"):
            val = _get_case_insensitive(v, key)
            if isinstance(val, list):
                ledger_blocks.extend(val)

        inv_items = _get_case_insensitive(v, "allinventoryentries", "allinventoryentries_list")
        if isinstance(inv_items, list):
            for item in inv_items:
                for key in ("accountingallocations", "allledgerentries", "ledgerentries"):
                    val = _get_case_insensitive(item, key)
                    if isinstance(val, list):
                        ledger_blocks.extend(val)

                batch_allocs = _get_case_insensitive(item, "batchallocations")
                if isinstance(batch_allocs, list):
                    for b in batch_allocs:
                        val = _get_case_insensitive(b, "accountingallocations")
                        if isinstance(val, list):
                            ledger_blocks.extend(val)

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

        if not ledger_blocks:
            maybe = _get_case_insensitive(v, "ledgerentries")
            if isinstance(maybe, list):
                ledger_blocks.extend(maybe)

        for entry in ledger_blocks:
            if not isinstance(entry, dict):
                continue
            ledger_name = _get_case_insensitive(entry, "ledgername", "LEDGERNAME", "ledger")
            amount_raw = _get_case_insensitive(entry, "amount", "AMOUNT", "value")

            amount = None
            if isinstance(amount_raw, (int, float)):
                amount = float(amount_raw)
            elif isinstance(amount_raw, str):
                try:
                    amount = float(amount_raw.replace(",", "").strip())
                except:
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
# FIXED STORAGE PATH (Railway compatible)
# ---------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
NDJSON_FOLDER = os.path.join(BASE_DIR, "ndjson_output")
os.makedirs(NDJSON_FOLDER, exist_ok=True)

# ---------------------------
# Upload & Convert Endpoint
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

        with open(out_path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

        return {
            "status": "success",
            "rows_created": len(rows),
            "ndjson_file": out_file,
            "download_url": f"/download-ndjson?filename={out_file}"
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}

# ---------------------------
# Download Endpoint
# ---------------------------
@app.get("/download-ndjson")
def download_ndjson(filename: str):
    file_path = os.path.join(NDJSON_FOLDER, filename)

    if not os.path.exists(file_path):
        return {"status": "error", "message": "File not found"}

    return FileResponse(file_path, filename=filename, media_type="application/octet-stream")
