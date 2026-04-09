# Dashboard Ufficio Acquisti – Fondazione Telethon ETS

Dashboard KPI per il monitoraggio delle attività dell'ufficio acquisti: saving & ordini, tempi di attraversamento, non conformità, analisi fornitori.

---

## Stack

| Layer | Tecnologia |
|-------|-----------|
| Frontend | React 18 + Vite + Tailwind CSS + Recharts |
| Backend | FastAPI (Python 3.12) |
| Database | Supabase (PostgreSQL) |
| Deploy | Railway (2 servizi: backend + frontend) |

---

## Setup iniziale

### 1. Supabase — crea le tabelle

1. Apri il tuo progetto su [supabase.com](https://supabase.com)
2. Vai in **SQL Editor → New query**
3. Incolla il contenuto di `supabase/schema.sql` ed esegui
4. Vai in **Settings → API** e copia:
   - **Project URL** → `SUPABASE_URL`
   - **anon/public key** → (non usata direttamente)
   - **service_role key** → `SUPABASE_SERVICE_KEY`

### 2. GitHub — crea il repo

```bash
git init
git add .
git commit -m "init: dashboard ufficio acquisti"
git remote add origin https://github.com/TUO_USERNAME/ua-dashboard.git
git push -u origin main
```

### 3. Railway — deploy backend

1. Vai su [railway.app](https://railway.app) → **New Project → Deploy from GitHub repo**
2. Seleziona il tuo repo, scegli la cartella **`backend`** come root
3. Railway rileva automaticamente il Dockerfile
4. Aggiungi le variabili d'ambiente in **Settings → Variables**:
   ```
   SUPABASE_URL=https://xxxx.supabase.co
   SUPABASE_SERVICE_KEY=eyJ...
   ALLOWED_ORIGINS=https://TUO_FRONTEND.railway.app
   ```
5. Copia l'URL pubblico del backend (es. `https://ua-backend.railway.app`)

### 4. Railway — deploy frontend

1. **New Service → GitHub repo** (stesso repo)
2. Root directory: **`frontend`**
3. Aggiungi variabile:
   ```
   VITE_API_URL=https://ua-backend.railway.app
   ```
4. Railway esegue il build e serve il frontend

---

## Sviluppo locale

```bash
# Backend
cd backend
cp .env.example .env          # compila con le tue credenziali Supabase
pip install -r requirements.txt
uvicorn main:app --reload

# Frontend (in altro terminale)
cd frontend
cp .env.example .env.local    # compila VITE_API_URL=http://localhost:8000
npm install
npm run dev
```

Apri http://localhost:5173

---

## Caricamento dati mensile

1. Apri la dashboard → sezione **Carica Dati**
2. Carica i tre file Excel:
   - **Saving**: estratto Alyante con foglio `Final saving 2025`
   - **Tempi**: file `Tempo_attraversamento_ordini_YYYY_finale.xlsx`
   - **NC**: file `NonConformita_Ricerca_Semplificato.xlsx`
3. I dati vengono aggiunti senza sovrascrivere quelli esistenti
4. Per correggere un upload errato: usa il cestino nello storico caricamenti (elimina a cascata tutti i dati dell'upload)

---

## Struttura del progetto

```
ua-dashboard/
├── backend/
│   ├── main.py              # FastAPI — tutti gli endpoint
│   ├── requirements.txt
│   ├── Dockerfile
│   └── .env.example
├── frontend/
│   ├── src/
│   │   ├── pages/
│   │   │   ├── Riepilogo.jsx      # Homepage con KPI headline
│   │   │   ├── Saving.jsx         # Saving & ordini (tutti i breakdown)
│   │   │   ├── Tempi.jsx          # Tempi attraversamento
│   │   │   ├── NonConformita.jsx  # NC per tipo/fornitore/trend
│   │   │   ├── Fornitori.jsx      # Pareto + top fornitori
│   │   │   └── Upload.jsx         # Caricamento file + storico
│   │   ├── components/
│   │   │   ├── Layout.jsx         # Sidebar + navigazione
│   │   │   └── UI.jsx             # KpiCard, FilterBar, DataTable...
│   │   ├── hooks/useKpi.js        # Custom hook per fetch dati
│   │   └── utils/
│   │       ├── api.js             # Tutti i fetch verso il backend
│   │       └── fmt.js             # Formatter: fmtEur, fmtPct, colori brand
│   ├── Dockerfile
│   ├── nginx.conf
│   └── package.json
└── supabase/
    └── schema.sql             # Tabelle, indici, views aggregate
```
