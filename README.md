# Missing folders dashboard

Grafana dashboard that shows which scan identifiers listed in a Google Sheet have no matching folder
on an SMB share. A FastAPI service does the comparison; the Grafana Infinity plugin polls it every
30 minutes.

## How the data is pulled

Every request re-reads both sources — nothing is cached, so a panel always shows the current state.

**SMB share** — `smbclient` opens a session against `NAS_HOST` with `NAS_USERNAME` / `NAS_PASSWORD`

code example:
```python
import smbclient
smbclient.register_session(HOST, username=USERNAME, password=PASSWORD)
files = smbclient.listdir(f"\\\\{HOST}\\{SHARE}")
print(len(files))
for f in files:
    print(f)
```


**Google Sheet** — the service account key comes in as `GOOGLE_CREDS`, a base64 blob that is decoded
and passed to `Credentials.from_service_account_info`. The scope is restricted to
`spreadsheets.readonly`, so the credential cannot write. `gspread` opens `SHEET_URL`, takes the first
tab, and reads column `SHEET_COLUMN` top to bottom.

code example:
```python
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDS), scopes=SCOPES)
client = gspread.authorize(creds)
sheet_url = "https://docs.google.com/spreadsheets/d/1YvhfZ8k3xafsN1ijtOl1uESG4w48-2jVnnWQM-s_OQM/edit?gid=0#gid=0"
spreadsheet = client.open_by_url(sheet_url)
worksheet = spreadsheet.sheet1  # Gets the first tab
data = worksheet.col_values(1)
print(len(data))
```

**Comparison** — both sides are filtered to entries matching `\d{8}_\d{4}` (e.g. `20010907_1247`), which
drops the column header, stray notes, and anything on the share that is not a scan folder. Sheet values
are then deduplicated, and every one with no folder of the same name is reported as missing.

## Run

1. Fill in `.env` (see `.env.example`). For the key: `base64 -w0 secret.keys.json` is what you download from google cloud.
2. `docker compose up --build`
3. Grafana on http://localhost:3000 (`admin` / `$GRAFANA_ADMIN_PASSWORD`), dashboard **Missing Folders**.
   The API is also reachable directly on http://localhost:8000.

## Endpoints

| Endpoint                | Response                                                                             |
| ----------------------- | ------------------------------------------------------------------------------------ |
| `/missing-folders`      | counts (`expected_count`, `present_count`, `missing_count`), `missing`, `checked_at`  |
| `/missing-folders/list` | one row per missing folder, for the table panel                                      |
| `/health`               | liveness check                                                                       |
