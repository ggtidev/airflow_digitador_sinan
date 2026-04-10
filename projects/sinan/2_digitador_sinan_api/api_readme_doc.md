# Documentação API - Digitador SINAN

Esta documentação contém instruções detalhadas para configurar, executar e realizar manutenção no projeto da API do Digitador SINAN.

---

## 🛠️ Pré-Requisitos

- Docker e Docker Compose instalados.
- Python 3.9+ instalado.

---

## 📂 Estrutura do Projeto

```
Digitador_SINAN_API
├── migrations
├── models
│   └── models.py
├── services
│   └── redcap_violencia.py
├── carga_violencia.py
├── database.py
├── main.py
├── requirements.txt
├── alembic.ini
├── .env
└── docker-compose.yml
```

---

## ⚙️ Configurando o Ambiente

### 1. Docker Compose

Execute o Docker Compose para subir o banco PostgreSQL:

```bash
docker-compose up -d
```

### 2. Instalar Dependências

```bash
pip install -r requirements.txt
```

---

## 📄 Configuração `.env`

Crie o arquivo `.env` com as seguintes variáveis:

```env
# Conexão Banco API
API_DB_USER=postgres
API_DB_PASSWORD=postgres
API_DB_HOST=localhost
API_DB_PORT=5433
API_DB_NAME=sinan_api

# Conexão Banco REDCAP (Conector)
REDCAP_DB_USER=postgres
REDCAP_DB_PASSWORD=postgres
REDCAP_DB_HOST=localhost
REDCAP_DB_PORT=5432
REDCAP_DB_NAME=pg_redcap
```

---

## 🚀 Alembic (Migrations)

O Alembic gera e executa as migrações do banco de dados automaticamente:

### Gerar migration

```bash
alembic revision --autogenerate -m "Descrição"
```

### Executar migration

```bash
alembic upgrade head
```

**Nota:** A configuração do Alembic está em `alembic.ini` e `migrations/env.py`. As URLs dos bancos são geradas a partir do `.env`.

---

## 📌 Carga Inicial (Conector → API)

Execute o script de carga para importar os dados de violência do banco Redcap para o banco da API:

```bash
python carga_violencia.py
```

Este script cria registros nas tabelas `rpa_violencia` e `rpa_violencia_detalhes`.

---

## 🌐 Executando a API

### Rodar Localmente (com Uvicorn e Swagger)

```bash
uvicorn main:app --reload
```

Acesse o Swagger (documentação interativa):

```
http://localhost:8000/docs
```

---

## 🔎 Endpoint `/violencia`

Retorna as notificações de violência formatadas para o RPA.

```bash
GET /violencia
```

### Formato do Response

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

**Observação:**

- `num_notificacao` é um identificador único da notificação.
- Os campos dentro de `notificacao` e `investigacao` estão mapeados diretamente do banco.

---

## 📌 Conexões (database.py)

Gerencia conexões com os bancos:

```python
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("API_DB_HOST"),
        port=os.getenv("API_DB_PORT"),
        dbname=os.getenv("API_DB_NAME"),
        user=os.getenv("API_DB_USER"),
        password=os.getenv("API_DB_PASSWORD")
    )
```

---

## ⚙️ Serviços (services/redcap\_violencia.py)

Responsável por:

- Consultar dados formatados para o RPA.
- Realizar mapeamento dos campos (Redcap → RPA).

---

## 📦 Docker Compose (`docker-compose.yml`)

Banco PostgreSQL do projeto:

```yaml
version: '3.8'

services:
  postgres_sinan:
    image: postgres:16
    container_name: postgres_sinan
    restart: always
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: sinan_api
    ports:
      - "5433:5432"
    volumes:
      - sinan_pgdata:/var/lib/postgresql/data

volumes:
  sinan_pgdata:
```

---

## 🗃️ Modelos de Dados

**Tabela:** `rpa_violencia`

| Coluna           | Tipo       | Descrição                     |
| ---------------- | ---------- | ----------------------------- |
| id               | integer    | Chave primária                |
| num\_notificacao | varchar(7) | Número único da notificação   |
| record           | string     | Identificador original Redcap |
| status           | string     | Status da carga               |

**Tabela:** `rpa_violencia_detalhes`

| Coluna             | Tipo    | Descrição                           |
| ------------------ | ------- | ----------------------------------- |
| id                 | integer | Chave primária                      |
| rpa\_violencia\_id | integer | Chave estrangeira (`rpa_violencia`) |
| field\_name        | string  | Nome do campo                       |
| value              | text    | Valor do campo                      |
| status             | string  | Status da carga                     |

---

## 🧑‍💻 Manutenção e Evolução

- **Adicionar novas tabelas:** Utilize Alembic.
- **Modificar serviços:** `services/redcap_violencia.py`.
- **Atualizar conexões:** `.env`.

---

📌 **Importante:** Sempre execute migrações após alterações nos modelos:

```bash
alembic revision --autogenerate -m "alteração XYZ"
alembic upgrade head
```

---

Autor: JVsVieira 🎯

