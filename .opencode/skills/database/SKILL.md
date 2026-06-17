---
name: database
description: Use when working with PostgreSQL databases, Alembic migrations, ORM models, or SQL queries. Covers models/models.py, migrations/, alembic.ini, and database.py.
---

# Database Operations

## Connection Configuration

### Two Database Pattern
This project uses **two PostgreSQL databases**:

| DB | Container | Port | Purpose | Connection |
|----|-----------|------|---------|------------|
| `pg_redcap` | External / `host.docker.internal` | 5432 | Source: REDCap raw data (`redcap_respostas`) | `CONECTOR_DB_*` env vars |
| `sinan_api` | `postgres_sinan` (postgres:16) | 5433 | Target: processed notifications (`rpa_notificacoes`, etc.) | `API_DB_*` env vars |

## ORM Models (`models/models.py`)

```python
class SistemaAlvo(Base):
    __tablename__ = "sistemas_alvo"
    id = Column(Integer, primary_key=True)
    nome = Column(String(100), nullable=False)
    descricao = Column(Text)

class Agravos(Base):
    __tablename__ = "agravos"
    id = Column(Integer, primary_key=True)
    sistema_alvo_id = Column(Integer, ForeignKey("sistemas_alvo.id"))
    nome = Column(String(100), nullable=False)
    descricao = Column(Text)

class RpaNotificacao(Base):
    __tablename__ = "rpa_notificacoes"
    id = Column(Integer, primary_key=True)
    num_notificacao = Column(String(7), unique=True)
    record = Column(String(100))
    status = Column(String(50), default="pendente")
    erro_pergunta = Column(Text, nullable=True)
    agravo_id = Column(Integer, ForeignKey("agravos.id"))

class RpaNotificacaoDetalhe(Base):
    __tablename__ = "rpa_notificacao_detalhes"
    id = Column(Integer, primary_key=True)
    rpa_notificacao_id = Column(Integer, ForeignKey("rpa_notificacoes.id"))
    field_name = Column(String(100))
    value = Column(Text)
```

## Alembic Migrations

### Migration Chain
```
5901fc0d0f15 (initial: todas tabelas)
    ↓
f04da2dc26a5 (empty: informações 19-11-2025)
    ↓
a1b2c3d4e5f6 (add erro_pergunta column)
```

### Migration Commands
```bash
cd projects/sinan/2_digitador_sinan_api
alembic upgrade head      # Apply all migrations
alembic downgrade -1      # Rollback last migration
alembic revision --autogenerate -m "descrição"  # Create new migration
alembic history           # Show migration history
```

### Migration Configuration (`alembic.ini`)
- `sqlalchemy.url` configured per environment via `env.py`
- `env.py` loads `.env` and sets `target_metadata = Base.metadata`

## Database Connection (`database.py`)
- Uses `psycopg2` for direct PostgreSQL connections
- Connection string format: `postgresql://user:password@host:port/dbname`
- Separate connections for Conector DB and API DB

## Schema Evolution Notes
- `rpa_notificacoes.erro_pergunta` added in migration `a1b2c3d4e5f6` (2026-03-31)
- This field stores the pergunta (question) that caused the RPA error, enabling targeted retry
