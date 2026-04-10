# 🚀 Guia de Deploy — Airflow Digitador SINAN (Produção)

> **Projeto:** `airflow_digitador_sinan`  
> **Repositório:** https://github.com/ggtidev/airflow_digitador_sinan  
> **Branch de produção:** `airflow`  
> **Stack:** Apache Airflow 2.8.4 + Docker + PostgreSQL + Python

---

## 📋 Pré-requisitos no Servidor

O servidor de produção precisa ter instalado:
- ✅ **Git**
- ✅ **Docker** (`docker --version`)
- ✅ **Docker Compose** (`docker compose version`)
- ✅ Acesso à internet (para puxar imagens Docker e clonar o repositório)

---

## PASSO 1 — Conectar ao Servidor (SSH)

```bash
ssh usuario@ip-do-servidor
# Exemplo:
ssh admin@192.168.1.100
```

> [!NOTE]
> Se você não sabe o IP/usuário do servidor, pergunte ao administrador de infra da GTIC.

---

## PASSO 2 — Clonar o Repositório (Primeira vez)

> Só faz isso na **primeira vez**. Se já estiver clonado, pule para o **Passo 3**.

```bash
# Ir para o diretório onde vai ficar o projeto
cd /opt

# Clonar o repositório principal
git clone https://github.com/ggtidev/airflow_digitador_sinan.git airflow-docker

# Entrar na pasta
cd airflow-docker

# Clonar o submodulo sinan (projects/sinan)
git submodule update --init --recursive
```

---

## PASSO 3 — Atualizar o Código (Deploy de nova versão) ⭐

> Este é o passo que você vai usar **toda vez** que quiser atualizar a produção.

```bash
# ① Entrar na pasta do projeto
cd /opt/airflow-docker

# ② Atualizar o repositório principal (branch main)
git pull origin main

# ③ Entrar no submodulo e atualizar (branch airflow)
cd projects/sinan
git pull origin airflow

# ④ Voltar para a raiz do projeto
cd /opt/airflow-docker
```

---

## PASSO 4 — Configurar o Arquivo `.env`

> [!IMPORTANT]
> O arquivo `.env` contém senhas e credenciais sensíveis. Ele **NÃO está no GitHub** (está no .gitignore). Você precisa criá-lo manualmente no servidor.

```bash
# Ver o modelo de .env disponível
cat .env.example

# Criar/editar o .env de produção
nano .env
```

### Variáveis essenciais no `.env`:

```env
# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__WEBSERVER__SECRET_KEY=sua-chave-secreta-aqui

# Banco de dados Airflow
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow

# Credenciais REDCap / SINAN (verifique com o .env local)
REDCAP_API_URL=...
REDCAP_API_TOKEN=...
SINAN_DB_HOST=...
SINAN_DB_PORT=...
SINAN_DB_USER=...
SINAN_DB_PASSWORD=...
```

> [!TIP]
> Copie o `.env` do seu computador local para o servidor com:
> ```bash
> scp c:\airflow-docker\.env usuario@ip-do-servidor:/opt/airflow-docker/.env
> ```

---

## PASSO 5 — Criar a Rede Docker

> Só precisa fazer isso **uma vez**.

```bash
docker network create prod
```

---

## PASSO 6 — Subir os Containers

```bash
# Na raiz do projeto (/opt/airflow-docker)

# ① Subir tudo em background
docker compose up -d

# ② Verificar se os containers subiram corretamente
docker compose ps
```

### O que deve aparecer (todos `healthy` ou `running`):

| Container | Status | Porta |
|---|---|---|
| `airflow-postgres` | healthy | interno |
| `airflow-webserver` | running | 8081 |
| `airflow-scheduler` | running | interno |
| `postgres_sinan` | running | 5433 |

---

## PASSO 7 — Executar a Migração do Banco (se houver)

> Quando há novas migrações Alembic (como a `add_erro_pergunta`), execute:

```bash
# Entrar no container da API
docker exec -it airflow-webserver bash

# Dentro do container, ir para a pasta da API
cd /opt/projects/sinan/2_digitador_sinan_api

# Rodar a migração
flask db upgrade

# Sair do container
exit
```

---

## PASSO 8 — Verificar os Logs

```bash
# Ver logs de todos os containers
docker compose logs -f

# Ver logs de um container específico
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
```

---

## PASSO 9 — Acessar o Airflow no Navegador

Abra no navegador:
```
http://ip-do-servidor:8081
```

> Usuário/senha padrão: `admin` / `admin` (ou conforme configurado no script `airflow-init.sh`)

---

## 🔄 Fluxo Resumido — Deploy de Atualização

```bash
# No servidor de produção:
cd /opt/airflow-docker

git pull origin main
cd projects/sinan && git pull origin airflow && cd ..

docker compose restart
```

> [!WARNING]
> O `docker compose restart` reinicia os containers sem recriar. Se você alterar o `docker-compose.yaml`, use:
> ```bash
> docker compose up -d --force-recreate
> ```

---

## 🛑 Comandos Úteis de Manutenção

```bash
# Parar tudo
docker compose down

# Parar e remover volumes (⚠️ apaga os dados do banco!)
docker compose down -v

# Ver containers rodando
docker ps

# Reiniciar apenas um container
docker compose restart airflow-scheduler

# Verificar uso de recursos
docker stats
```

---

## ❓ Dúvidas Frequentes

**Q: Onde ficam os logs das DAGs?**  
A: Na pasta `./logs/` que é mapeada no volume do container.

**Q: Como saber se a DAG executou com erro?**  
A: Via interface web do Airflow (`http://ip:8081`) ou nos logs: `docker compose logs airflow-scheduler`

**Q: O que fazer se um container não sobe?**  
A: `docker compose logs nome-do-container` para ver o erro detalhado.
