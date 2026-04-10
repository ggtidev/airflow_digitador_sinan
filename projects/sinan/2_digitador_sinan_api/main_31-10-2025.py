from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from services.redcap_violencia import get_redcap_filas
from services.update_status import atualizar_status, obter_status

description = """
## 🚀 Digitador SINAN API

Esta API fornece dados formatados sobre notificações, com objetivo de integração direta com sistemas RPA.

## 📌 Configuração Inicial (Passo a Passo)

**1. Clone o projeto:**
```bash
git clone <url_do_projeto>
cd digitador_sinan_api
```

**2. Crie um ambiente virtual (opcional, mas recomendado):**
```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
.\venv\Scripts\activate   # Windows
```

**3. Instale as dependências:**
```bash
pip install -r requirements.txt
```

**4. Configure as variáveis de ambiente:**
Copie o arquivo `.env.example` para `.env` e preencha com seus dados:
```bash
cp .env.example .env
```

**5. Inicie o banco de dados com Docker Compose:**
```bash
docker-compose up -d
```

**6. Execute as migrações para criar as tabelas:**
```bash
alembic upgrade head
```

**7. Execute a carga inicial dos dados:**
```bash
python carga_violencia.py
```

**8. Inicie a API:**
```bash
uvicorn main:app --reload
```

Acesse a documentação interativa: `http://localhost:8000/docs`

## 🔗 Endpoints Disponíveis

- `/notificacoes`: Lista notificações pendentes.
- `/notificacoes/{num_notificacao}` *(PATCH)*: Atualiza status da notificação.
- `/notificacoes/{num_notificacao}/status`: Consulta status da notificação.

## 📝 Estrutura do Response
```json
[
  {
    "agravo": "VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA",
    "num_notificacao": "1234567",
    "notificacao": { ... },
    "investigacao": { ... },
    "outros": { ... }
  },
  ...
]
```

Para mais detalhes, consulte a documentação completa no projeto.

---
**Autor:** JVsVieira 🎯
"""

app = FastAPI(title="Digitador SINAN API",
              description=description,
              version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/notificacoes", summary="Listar notificações pendentes", tags=["Notificações Gerais"])
def listar_notificacoes():
    """
    Retorna uma lista formatada das notificações diretamente para integração com sistemas RPA.

    - **agravo**: Tipo de agravo da notificação.
    - **num_notificacao**: Número único de identificação da notificação.
    - **notificacao**: Informações gerais da notificação.
    - **investigacao**: Dados específicos de investigação.
    - **outros**: Informações adicionais não categorizadas.
    """
    return get_redcap_filas()

@app.patch("/notificacoes/{num_notificacao}", summary="Atualizar status da notificação", tags=["Notificações Gerais"])
def patch_status_violencia(num_notificacao: str):
    return atualizar_status(num_notificacao)
  
@app.get("/notificacoes/{num_notificacao}/status", summary="Obter status da notificação", tags=["Notificações Gerais"])
def get_status_violencia(num_notificacao: str):
    return obter_status(num_notificacao)

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Digitador SINAN API",
        version="1.0.0",
        description=description,
        routes=app.routes,
    )
    openapi_schema["info"]["x-logo"] = {
        "url": "https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png"
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi