# Run Order (Publisher -> Subscriber -> Dashboard)

## 1) Install dependencies
```bash
pip install -r requirements.txt
```

## 2) Set Google credentials (PowerShell)
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\mspaw\Downloads\BigDataProj\admin-key.json"
```

## 3) Optional model path
If you already saved an H2O model, set:
```powershell
$env:H2O_MODEL_PATH="C:\path\to\your\saved\h2o_model"
```

If no model path is set, `sub.py` will train a fallback model from `combined_ridership_2025_full.xlsx`.

## 4) Start real-time publisher
```bash
python pub.py
```

## 5) Start real-time subscriber + prediction snapshot
```bash
python sub.py
```

This continuously writes `dashboard_state.json` and also publishes dashboard snapshots to Pub/Sub topic `transit-dashboard-data`.

## 6) Start map dashboard
```bash
streamlit run dashboard.py
```
