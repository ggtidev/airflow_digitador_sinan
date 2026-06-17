# Airflow Digitador SINAN — Project Guide for AI Agents

## Project Overview

Orquestração ETL utilizando **Apache Airflow 2.8.4 + Docker** para extrair dados do REDCap, processar notificações do SINAN (Sistema de Informação de Agravos de Notificação), e persistir informações no PostgreSQL. Inclui automação RPA (Robotic Process Automation) para digitar notificações diretamente no sistema SINAN desktop.

**Stack:** Apache Airflow 2.8.4 | Docker Compose | PostgreSQL 15/16 | Python 3.13 | FastAPI | Flask | Alembic | PyAutoGUI | OpenCV

**Repository:** `https://github.com/ggtidev/airflow_digitador_sinan`

---

## Architecture

```
REDCap API ──► 1_etl_redcap_sinan/redcap.py ──► PostgreSQL (pg_redcap)
                                                      │
                                                      ▼
                                            2_digitador_sinan_api/
                                              carga_violencia.py
                                              (Transform + Load)
                                                      │
                                                      ▼
                                            PostgreSQL (sinan_api)
                                            Tables: sistemas_alvo,
                                                    agravos,
                                                    rpa_notificacoes,
                                                    rpa_notificacao_detalhes
                                                      │
                                                      ▼
                                            FastAPI (main.py)
                                            GET /notificacoes
                                            PATCH /{id}/status
                                                      │
                                                      ▼
                                            3_sinan_rpa/
                                              main.py → api_client.py
                                              → agravosscripts/violencia.py
                                              (PyAutoGUI → SINAN Desktop)
```

---

## Directory Structure

```
C:\airflow-docker\
├── .env                          # Credenciais REDCap, DB, Airflow admin
├── docker-compose.yaml           # Production: 6 containers
├── config/airflow.cfg            # Airflow configuration
├── dags/
│   ├── dag_etl_redcap_sinan.py   # DAG: ETL REDCap manual
│   ├── dag_digitador_sinan_api.py# DAG: Migration branch + carga
│   ├── dag_digitador_sinan.py    # DAG: Pipeline completo (ETL→API→RPA)
│   └── dag_teste_minimo.py       # DAG: Diagnóstico mínimo
├── projects/sinan/
│   ├── 1_etl_redcap_sinan/       # Stage 1: REDCap extract
│   │   └── redcap.py
│   ├── 2_digitador_sinan_api/    # Stage 2: API + carga scripts
│   │   ├── main.py               # FastAPI server
│   │   ├── carga_violencia.py    # Load script (violência)
│   │   ├── carga_sifilis.py      # Load script (sífilis)
│   │   ├── models/models.py      # ORM models
│   │   ├── services/             # REDCap query services per agravo
│   │   ├── migrations/           # Alembic migrations
│   │   └── alembic.ini
│   ├── 3_sinan_rpa/              # Stage 3: RPA automation
│   │   ├── main.py               # Orchestrator
│   │   ├── api_client.py         # HTTP client for FastAPI
│   │   ├── utils.py              # PyAutoGUI + OpenCV core
│   │   ├── agravosscripts/       # Per-agravo automation scripts
│   │   │   └── violencia.py      # 1271 lines, main RPA logic
│   │   └── imagens/              # PNG templates for screen recognition
│   └── Documentacao/
│       └── guia_deploy_producao.md
├── scripts/
│   ├── airflow-init.sh           # Container init: migrate + create user
│   └── create_user.sh
└── test_dag_logic.py             # Branch logic unit test
```

---

## DAGs — Detailed

### 1. `dag_etl_redcap_sinan.py` (DAG: `etl_redcap_sinan`)
- **Schedule:** None (manual trigger)
- **Tasks:** `inicio` → `etl_redcap` → `fim`
- **Logic:** Valida env vars, executa `python redcap.py`
- **Retries:** 3, delay 2min, timeout 1h

### 2. `dag_digitador_sinan_api.py` (DAG: `digitador_sinan_api`)
- **Schedule:** None
- **BranchPythonOperator:** `verificar_estado_banco` decide:
  - `gerar_migration` (tabela não existe, sem migrations)
  - `aplicar_migration` (tabela não existe, migrations presentes)
  - `executar_carga_violencia` (tabela já existe)
- **Flow:** `inicio → branch → (gerar→aplicar→carga) | (aplicar→carga) | (carga) → fim`
- **DBs:** Conector (`host.docker.internal:5432/pg_redcap`), API (`postgres_sinan:5432/sinan_api`)

### 3. `dag_digitador_sinan.py` (DAG: `digitador_sinan_pipeline`)
- **Schedule:** None
- **Control Variable:** `SINAN_MODO_EXECUCAO` (FULL | ETL | API | RPA)
- **Tasks:**
  1. `etl_redcap` → `python redcap.py`
  2. `carga_api` → `python carga_violencia.py`
  3. `rpa_sinan` → `python main.py` (RPA)
- **Retries:** 3, delay 2min, timeout 2h

### 4. `dag_teste_minimo.py` (DAG: `dag_teste_minimo`)
- Diagnostic only: single `EmptyOperator`, no schedule.

---

## Environment Variables

### Root `.env` (Airflow containers)
| Variable | Value | Purpose |
|----------|-------|---------|
| `REDCAP_API` | `https://redcap.recife.pe.gov.br/api/` | REDCap API endpoint |
| `REDCAP_TOKEN` | `15A28A0BA2CFF32380E87F2003FDA610` | REDCap project token |
| `DB_HOST` | `host.docker.internal` | Conector DB host |
| `DB_PORT` | `5432` | Conector DB port |
| `DB_NAME` | `pg_redcap` | Conector DB name |
| `DB_USER` | `postgres` | Conector DB user |
| `DB_PASSWORD` | `root` | Conector DB password |

### API `.env` (`2_digitador_sinan_api/.env`)
| Variable | Value | Purpose |
|----------|-------|---------|
| `CONECTOR_DB_*` | postgres/root@localhost:5432/pg_redcap | Source DB (REDCap data) |
| `API_DB_*` | postgres/postgres@localhost:5433/sinan_api | Target DB (notifications) |

### RPA `.env` (`3_sinan_rpa/.env`)
| Variable | Value | Purpose |
|----------|-------|---------|
| `USUARIO_LOGIN` | `usuario1` | RPA user profile |
| `USUARIO1_USERNAME` | `ADMINISTRADOR` | SINAN system login |
| `USUARIO1_PASSWORD` | `RODOVALHO21` | SINAN system password |
| `API_URL` | `http://localhost:8000` | FastAPI endpoint |

---

## Database Models (`models/models.py`)

| Table | Key Columns |
|-------|-------------|
| `sistemas_alvo` | `id` (PK), `nome`, `descricao` |
| `agravos` | `id` (PK), `sistema_alvo_id` (FK), `nome`, `descricao` |
| `rpa_notificacoes` | `id` (PK), `num_notificacao` (varchar 7), `record`, `status`, `erro_pergunta`, `agravo_id` (FK) |
| `rpa_notificacao_detalhes` | `id` (PK), `rpa_notificacao_id` (FK), `field_name`, `value` (text) |

### Alembic Migrations
1. `5901fc0d0f15` — Criar todas as tabelas (initial)
2. `f04da2dc26a5` — Informações do dia 19-11-2025 (empty)
3. `a1b2c3d4e5f6` — Add coluna `erro_pergunta` em `rpa_notificacoes`

---

## API Endpoints

### FastAPI (`main.py` — port 8000)
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/notificacoes` | Retorna notificações pendentes formatadas para RPA |
| `PATCH` | `/notificacoes/{num_notificacao}` | Atualiza status (`concluido` / `erro_digitacao`) |
| `GET` | `/notificacoes/{num_notificacao}/status` | Retorna status atual |

### Flask (`api_backend.py` — port 5000, alternativa)
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/notificacoes` | Retorna todas notificações via SQLAlchemy |

---

## RPA Automation (`3_sinan_rpa/`)

### Flow
1. `main.py` → `api_client.buscar_filas()` → `GET /notificacoes`
2. Dispatch to agravo script (currently only `VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA`)
3. `violencia.py` → `executar_violencia(item, ...)`
   - `abrir_sinan()` → Win key, type "sinan", Enter
   - `login(usuario, senha)` → Type credentials
   - `selecionar_agravo("%VIOLENC%")` → Menu navigation
   - `preencher_bloco_notificacao()` → Fields P01–P31 (~50 fields)
   - `preencher_bloco_investigacao()` → Fields P34–P70 (~100+ fields)
   - Click "salvar", handle dialogs
4. Error handling: screenshots → `registrar_erro()` via API

### Key RPA technologies
- **PyAutoGUI:** Keyboard/mouse automation, screen resolution detection
- **OpenCV (cv2):** Template matching for button/dialog recognition
- **MSS:** Fast screen capture for image recognition
- **Error detection:** 18+ error template images scanned per action

### Supported agravos (RPA scripts)
| Agravo | Script | Status |
|--------|--------|--------|
| Violência | `violencia.py` | **Implemented** (1271 lines) |
| Sífilis | `sifilis.py` | Scaffold |
| Toxoplasmose | `toxoplasmose.py` | Scaffold |
| Toxoplasmose Gestacional | `toxoplasmose_gestacional.py` | Scaffold |
| Varicela | `varicela.py` | Scaffold |
| Zika | `zika.py` | Scaffold |
| Creutzfeldt-Jakob | `creutzfeldt_jakob.py` | Scaffold |

---

## Docker Compose Services (`docker-compose.yaml`)

| Service | Image | Purpose | Port |
|---------|-------|---------|------|
| `airflow-postgres` | postgres:15 | Airflow metadata DB | — |
| `airflow-redis` | redis:7 | Celery backend | — |
| `airflow-init` | apache/airflow:2.8.4 | DB migrate + user create | — |
| `airflow-webserver` | apache/airflow:2.8.4 | Airflow UI | 8081 |
| `airflow-scheduler` | apache/airflow:2.8.4 | Task scheduler | — |
| `postgres_sinan` | postgres:16 | SINAN notification DB | 5433 |

**Network:** `airflow-net` (external, name: `prod`)
**Executor:** LocalExecutor
**Secret Key:** `b7c8a6e4f1d9a0c23e4f9b1d7e3a2c8f`

---

## Development Workflow

### Local Setup
```bash
git clone https://github.com/ggtidev/airflow_digitador_sinan
cd airflow_digitador_sinan
git submodule update --init --remote
python -m venv .venv
# Edit .env with credentials
docker network create prod
docker compose up -d
```

### Update Flow
```bash
git pull origin main
cd projects/sinan && git pull origin airflow && cd ../..
docker compose restart
```

### Container Init
```bash
docker compose up airflow-init   # Runs airflow-init.sh
# Creates admin user (admin/admin)
```

---

## Testing

```bash
python test_dag_logic.py   # Tests branch decision logic
```

The test mocks DB connection and glob results to verify three branch outcomes:
`gerar_migration`, `aplicar_migration`, `executar_carga_violencia`.

No formal pytest suite is configured yet.

---

## Security Considerations

1. **REDCap token** `15A28A0BA2CFF32380E87F2003FDA610` — hardcoded in multiple files (`services/redcap_violencia.py`, `utils/utils.py`)
2. **SINAN credentials** `ADMINISTRADOR / RODOVALHO21` — in `credenciais.txt` and RPA `.env`
3. **External API auth** `sevs_user:YrQKvg82DKfLooYT` — in `utils.py` for CNES lookup
4. **Patient PII** — `response_1757590003626.json` contains real names, CPF, addresses, phone numbers
5. **`.env` files at multiple levels** — root, API, RPA, ETL all have separate `.env` files

---

## Docker Compose Variants (Historical)

| File | Notes |
|------|-------|
| `docker-compose.yaml` | Current production-ready (6 services, external network) |
| `docker-compose-19-01-2026.yaml` | Previous version with inline init & hardcoded secret |
| `docker-compose-18-01-2026.yaml` | Earlier version |
| `docker-compose(modelo).yaml` | Official Apache Airflow template |
| `OLDdocker-compose copy.yaml` | Original simple version (Windows 11) |
