---
name: etl-pipeline
description: Use when working with REDCap ETL, carga scripts, or data transformation in 1_etl_redcap_sinan/ or 2_digitador_sinan_api/carga_*.py files. Covers extraction from REDCap API, transformation of EAV data, loading to PostgreSQL via carga scripts.
---

# ETL Pipeline

## Stage 1 — REDCap Extraction (`1_etl_redcap_sinan/redcap.py`)

1. **Extract:** `POST` to `REDCAP_API` with `token`, `content=record`, `format=json`, `type=eav`
2. **Load:** Creates table `redcap_respostas` (record, field_name, value) if not exists, `TRUNCATE`, bulk insert all rows
3. **Connection:** `DB_HOST` (default `host.docker.internal`), port `5432`, database `pg_redcap`

## Stage 2 — Carga Scripts (`2_digitador_sinan_api/carga_*.py`)

Each carga script follows the same pattern:
1. Connect to Conector DB (source: `redcap_respostas`)
2. Query records filtered by agravo prefix (e.g., `vio_%`, `sif_%`)
3. Group by `record`, normalize field names (strip prefix)
4. Apply business validation rules:
   - Municipio must be "RECIFE"
   - Unidade notificadora: if != 1 or 7 → force to 7
   - Data encerramento >= data notificacao
   - Validate 5+ mandatory fields
5. Seed `SistemaAlvo` ("SINAN NET") and `Agravo` (e.g., "VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA")
6. Get SINAN number: `GET https://vigilanciaemsaude.recife.pe.gov.br/margem-sinan/numero_sinan`
7. Insert into `rpa_notificacoes` and `rpa_notificacao_detalhes`

## Carga Scripts Available

| Script | Agravo Prefix | Agravo Name |
|--------|---------------|-------------|
| `carga_violencia.py` | `vio` | VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA |
| `carga_sifilis.py` | `sif` | SIFILIS |
| `carga_toxoplasmose.py` | `tox` | TOXOPLASMOSE |
| `carga_toxoplasmose_gestacional.py` | `toxg` | TOXOPLASMOSE_GESTACIONAL |
| `carga_varicela.py` | `var` | VARICELA |
| `carga_zika.py` | `zik` | ZIKA |
| `carga_creutzfeldt_jakob.py` | `cjd` | CREUTZFELDT_JAKOB |

## Services per Agravo (`2_digitador_sinan_api/services/`)

Each `redcap_<agravo>.py` contains:
- SQL queries to extract from `redcap_respostas`
- Field name mapping/conversion functions
- Data formatting for the notification structure

## Utility Functions (`utils/utils.py`)

- `formatar_data(value)` — Converts REDCap date strings
- `formatar_sexo(value)` — Maps (1=M, 2=F)
- `formatar_deficiencia(value)` — Maps REDCap codes
- `FIELD_LABELS` dict — Maps field names to display labels
