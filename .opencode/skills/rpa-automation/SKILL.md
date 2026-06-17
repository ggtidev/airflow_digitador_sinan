---
name: rpa-automation
description: Use when working with RPA automation scripts in 3_sinan_rpa/, including PyAutoGUI, OpenCV template matching, SINAN desktop navigation, error handling, and screen recognition. Covers main.py, api_client.py, utils.py, and agravosscripts/*.py.
---

# RPA Automation

## Architecture

```
main.py → api_client.buscar_filas() → GET /notificacoes (FastAPI)
   │
   └──→ dispatch to agravo script by name
        │
        └──→ violencia.py (implemented)
             ├── abrir_sinan() — Win key + type "sinan" + Enter
             ├── login(usuario, senha) — type credentials
             ├── selecionar_agravo("%VIOLENC%")
             ├── preencher_bloco_notificacao() — P01–P31 (~50 fields)
             ├── preencher_bloco_investigacao() — P34–P70 (~100+ fields)
             └── salvar() + handle dialogs
```

## Core Utilities (`utils.py`)

### Image Recognition (dual approach)
1. **Primary:** OpenCV (`cv2.matchTemplate`) + MSS (fast screen capture)
2. **Fallback:** PyAutoGUI (`pyautogui.locateOnScreen`)

### Error Detection
- Scans 18+ error template images (`erro-01-atencao.png` through `erro-18-notificacao_ja_cadastrada.jpg`)
- On match: `fechar_tela_erro()` sequence (ESC → SAIR → NAO → click fixo → ENTER×2)

### CNES Resolution
- `get_cnes(unidade)`: queries `https://vigilanciaemsaude.recife.pe.gov.br/api-sinan/unidade`
- Auth: basic auth (`sevs_user:YrQKvg82DKfLooYT`)
- 10 special units use fixed CNES `6468918`

### Helper Functions
- `aguardar(t)` — Sleep with logging
- `digitar(texto)` — Type with delay, logging
- `clicar_no_centro(regiao)` — Click center of matched region
- `rolar_para_baixo()` — Scroll down 3 notches
- `monitorar_recursos()` — CPU/RAM logging via psutil

## API Client (`api_client.py`)

| Function | Method | Endpoint |
|----------|--------|----------|
| `buscar_filas()` | GET | `{API_URL}/notificacoes` |
| `atualizar_status(num, status)` | PATCH | `{API_URL}/notificacoes/{num}` |
| `registrar_erro(num, erro_pergunta)` | PATCH | `{API_URL}/notificacoes/{num}` |

## Agravo Scripts (`agravosscripts/`)

### violencia.py (1271 lines — fully implemented)
- `executar_violencia(item, reaproveitar_sessao, tem_proxima)`
- Business rules:
  - P06 (Unidade Notificadora): only 1 or 7
  - P07/P08: CNES lookup by unit code
  - P13–P16: gender/gestation/schooling conditional on age
  - P35–P37: marital status, sexual orientation, gender identity (age >= 10)
  - P38–P39: disability conditional sub-fields
  - P56–P57: violence type + means mapped from REDCap to SINAN codes
- Error handling: per-field `_pergunta_digitando` tracking, screenshot on error, error registration via API

### Other scripts (scaffolds)
`sifilis.py`, `toxoplasmose.py`, `toxoplasmose_gestacional.py`, `varicela.py`, `zika.py`, `creutzfeldt_jakob.py` — basic structure only.

## Error Flow
1. PyAutoGUI exception or timeout → screenshot saved to `erros/`
2. `fechar_tela_erro()` → close all error popups
3. `registrar_erro()` → PATCH API with `{"status": "erro_digitacao", "erro_pergunta": "..."}`
4. Log to `rpa_log.txt`
5. Continue to next notification

## Field Mapping (REDCap → SINAN)

REDCap fields follow pattern `{agravo}_{field}` (e.g., `vio_p01_natureza`).
The SINAN desktop expects specific field codes (P01–P70+).
Mapping is hardcoded per agravo script via `PERGUNTAS_MAPEADAS` dicts.
