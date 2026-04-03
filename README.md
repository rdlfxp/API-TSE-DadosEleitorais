# API MeuCandidato (iOS Analytics)
[![CI](https://github.com/rdlfxp/API-TSE-DadosEleitorais/actions/workflows/ci.yml/badge.svg)](https://github.com/rdlfxp/API-TSE-DadosEleitorais/actions/workflows/ci.yml)

Projeto limpo para backend mobile-first: FastAPI + analytics em JSON para app iOS (Swift/SwiftUI).

## 1) Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Dados

Organizacao recomendada:

- `data/raw/<ano>/...csv`: arquivos brutos do TSE (entrada)
- `data/curated/analytics.csv`: artefato legado/publicado junto do snapshot
- `data/curated/quality_report.json`: relatorio de qualidade da consolidacao

Por padrao, a API carrega apenas `data/curated/analytics.parquet`.

Você pode configurar via `.env`:

```env
ANALYTICS_ENGINE=duckdb
ANALYTICS_DATA_PATH=data/curated/analytics.parquet
ANALYTICS_SEPARATOR=,
ANALYTICS_ENCODING=utf-8
RATE_LIMIT_ENABLED=true
RATE_LIMIT_WINDOW_SECONDS=60
RATE_LIMIT_MAX_REQUESTS_PER_IP=120
```

Para volume alto (produção), prefira `analytics.parquet` com DuckDB:

```env
ANALYTICS_ENGINE=duckdb
ANALYTICS_DATA_PATH=data/curated/analytics.parquet
PREFER_PARQUET_IF_AVAILABLE=true
```

Se o arquivo local não existir, a API pode baixar automaticamente do Cloudflare R2 na inicialização:

```env
R2_ACCOUNT_ID=seu_account_id
R2_ACCESS_KEY_ID=seu_access_key
R2_SECRET_ACCESS_KEY=sua_secret_key
R2_BUCKET=tse-curated
R2_OBJECT_KEY_PARQUET=latest/analytics.parquet
R2_CONNECT_TIMEOUT_SECONDS=5
R2_READ_TIMEOUT_SECONDS=30
```

Com `PREFER_PARQUET_IF_AVAILABLE=true`, o bootstrap usa apenas `latest/analytics.parquet` para a API.

## Compatibilidade de parâmetros de ano

Os endpoints de analytics aceitam tanto `ano` quanto `year`.
Se ambos forem enviados com valores diferentes, a API retorna `400`.

## Contrato de identidade

Nos endpoints que retornam dados de candidato, os campos de identidade seguem esta regra:

- `candidate_id`: identificador de consulta da rota.
- `source_id`: origem do registro eleitoral retornado.
- `nr_cpf_candidato`: CPF do candidato quando disponivel. No endpoint de historico, ele e a chave preferencial de consulta e expansao do historico.
- `person_id`: identidade estavel da pessoa, derivada do CPF do candidato quando disponivel.
- `canonical_candidate_id`: identidade canonica interna da API. Hoje espelha `person_id` e nunca volta a ser `candidate_id`.

Observacoes:

- `SQ_CANDIDATO` e `NR_CANDIDATO` nao sao usados como identidade historica da pessoa.
- No `vote-history`, a API nao expande mais historico por nome completo, nome de urna ou data de nascimento.
- O historico multi-ano/multicargo so e expandido quando existe `NR_CPF_CANDIDATO`.
- Quando o candidato nao e encontrado no recorte consultado, `source_id`, `canonical_candidate_id` e `person_id` ficam como `null`.

## 3) Run

```bash
uvicorn app.main:app --reload
```

### 3.1) Profiling de custo por endpoint

Para medir RAM e CPU endpoint por endpoint, rode:

```bash
python3 scripts/profile_endpoint_costs.py
```

O script sobe um servidor local, executa cada rota de forma isolada e mostra:

- `duration_ms`
- `rss_before_mb`
- `rss_peak_mb`
- `rss_after_mb`
- `cpu_peak_pct`

Se a API já estiver rodando, use `--base-url` e informe `--pid`.

## 4) Normalizacao multi-ano (2000-2024)

### 4.1) Fluxo recomendado (auto descoberta em `data/raw`)

Estruture os arquivos:

```text
data/raw/
  2014/
    consulta_cand_2014_*.csv
    votacao_candidato_munzona_2014_*.csv
  2018/
    consulta_cand_2018_*.csv
    votacao_candidato_munzona_2018_*.csv
  2022/
    consulta_cand_2022_*.csv
    votacao_candidato_munzona_2022_*.csv
```

Gere a base consolidada:

```bash
python3 scripts/normalize.py \
  --raw-dir data/raw \
  --years 2014 2018 2022 \
  --votacao-pattern '*votacao_candidato*munzona*.csv' \
  --consulta-pattern '*consulta_cand*.csv' '*consulta_vagas*.csv' \
  --exclude-pattern '*.DS_Store' '*classificado*.csv' \
  --output data/curated/analytics.csv \
  --report data/curated/quality_report.json
```

Parquet opcional (melhor performance em volume alto):

```bash
pip install pyarrow
python3 scripts/normalize.py \
  --raw-dir data/raw \
  --years 2000 2002 2004 2006 2008 2010 2012 2014 2016 2018 2020 2022 2024 \
  --output data/curated/analytics.parquet \
  --report data/curated/quality_report.json
```

Para integrar outras variacoes de planilha de votacao, adicione mais padroes:

```bash
python3 scripts/normalize.py \
  --raw-dir data/raw \
  --years 2000 2002 2004 2006 2008 2010 2012 2014 2016 2018 2020 2022 2024 \
  --votacao-pattern '*votacao_candidato*munzona*.csv' '*votacao_candidato*.csv' \
  --consulta-pattern '*consulta_cand*.csv' '*consulta_vagas*.csv'
```

### 4.2) Fluxo manual (arquivos explicitos)

Use quando quiser controlar exatamente quais CSVs entram:

```bash
python3 scripts/normalize.py \
  --votacao /caminho/votacao_candidato_munzona_2014_*.csv /caminho/votacao_candidato_munzona_2018_*.csv /caminho/votacao_candidato_munzona_2022_*.csv \
  --consulta /caminho/consulta_cand_2014_*.csv /caminho/consulta_cand_2018_*.csv /caminho/consulta_cand_2022_*.csv \
  --output data/curated/analytics.csv \
  --report data/curated/quality_report.json
```

Notas:
- O script assume padrao TSE (`sep=';'`, `encoding='latin1'`).
- Se `consulta_cand` nao for enviada, ainda gera arquivo normalizado com colunas disponiveis.
- A API passa a funcionar para qualquer ano/cargo disponivel no arquivo final.
- O relatorio JSON traz checagens por ano: colunas faltantes, nulos criticos, duplicidade e total de votos.

Quality gate (bloqueio por qualidade):

```bash
python3 scripts/normalize.py \
  --raw-dir data/raw \
  --years 2018 2022 \
  --output data/curated/analytics.csv \
  --report data/curated/quality_report.json \
  --quality-gate \
  --max-duplicate-rows 0 \
  --max-negative-votes 0 \
  --max-required-null-rate 0.02
```

Se reprovar no gate, o script encerra com codigo `2` e nao grava o arquivo de saida.

## 5) Endpoints para iOS

- `GET /health`
- `GET /metrics` (observabilidade)
- `GET /v1/analytics/filtros` (`200`, `503`)
- `GET /v1/analytics/overview?ano=2022&uf=SP&cargo=Deputado%20Estadual` (`200`, `422`, `503`)
- `GET /v1/analytics/top-candidatos?ano=2022&uf=SP&cargo=Deputado%20Estadual&top_n=20` (`200`, `422`, `503`)
- `GET /v1/analytics/candidatos/search?q=candidato&ano=2022&uf=SP&cargo=Deputado%20Estadual&page=1&page_size=20` (`200`, `422`, `503`)
- `GET /v1/analytics/distribuicao?group_by=genero&ano=2022&uf=SP` (`200`, `400`, `422`, `503`)
- `GET /v1/analytics/distribuicao?group_by=status&ano=2022&uf=SP&cargo=Senador` (`200`, `400`, `422`, `503`)
- `GET /v1/analytics/cor-raca-comparativo?ano=2022&uf=SP` (`200`, `422`, `503`)
- `GET /v1/analytics/ocupacao-genero?ano=2022&uf=SP&somente_eleitos=false` (`200`, `422`, `503`)
- `GET /v1/analytics/idade?ano=2022&uf=SP&cargo=Deputado%20Estadual` (`200`, `422`, `503`)
- `GET /v1/analytics/serie-temporal?metric=votos_nominais&uf=SP&cargo=Deputado%20Estadual` (`200`, `400`, `422`, `503`)
- `GET /v1/analytics/ranking?group_by=partido&metric=votos_nominais&ano=2022&uf=SP&top_n=10` (`200`, `400`, `422`, `503`)
- `GET /v1/analytics/mapa-uf?metric=votos_nominais&ano=2022&cargo=Deputado%20Estadual` (`200`, `400`, `422`, `503`)
- `GET /v1/analytics/vagas-oficiais?ano=2024&uf=SP&group_by=cargo` (`200`, `400`, `422`, `503`)
- `GET /v1/analytics/polarizacao?uf=SP&ano_governador=2022&ano_municipal=2024` (`200`, `422`, `503`)

`group_by` aceitos em `/distribuicao`: `status`, `genero`, `instrucao`, `cor_raca`, `estado_civil`, `ocupacao`, `cargo`, `uf`.
`metric` aceitos em `/serie-temporal`, `/ranking`, `/mapa-uf`: `votos_nominais`, `candidatos`, `eleitos`, `registros`.
`group_by` aceitos em `/ranking`: `candidato`, `partido`, `cargo`, `uf`.
`group_by` aceitos em `/vagas-oficiais`: `cargo`, `uf`, `municipio`.

### Contrato de erro (padrao)

Todas as rotas de analytics retornam erro no formato:

```json
{
  "message": "descricao do erro"
}
```

Casos comuns:
- `400`: regra de negocio invalida (ex.: `group_by` nao suportado)
- `422`: parametros de consulta invalidos
- `503`: base analytics indisponivel

## 6) Contrato `/v1` (exemplos)

Base path: `/v1/analytics`

### `GET /filtros`

Resposta `200`:

```json
{
  "anos": [2018, 2022],
  "ufs": ["RJ", "SP"],
  "cargos": ["Deputado Estadual", "Senador"]
}
```

Erro `503`:

```json
{
  "message": "Base analytics indisponivel. Ajuste ANALYTICS_DATA_PATH ou coloque o arquivo em data/curated/analytics.parquet (ou .csv)."
}
```

### `GET /overview?ano=2022&uf=SP&cargo=Deputado%20Estadual`

Resposta `200`:

```json
{
  "total_registros": 1924,
  "total_candidatos": 1924,
  "total_eleitos": 94,
  "total_votos_nominais": 22791543
}
```

Erro `422`:

```json
{
  "message": "Parametros de consulta invalidos."
}
```

### `GET /top-candidatos?ano=2022&uf=SP&cargo=Deputado%20Estadual&top_n=3`

Resposta `200`:

```json
{
  "top_n": 3,
  "items": [
    {
      "candidato": "Nome Candidato",
      "partido": "ABC",
      "cargo": "Deputado Estadual",
      "uf": "SP",
      "votos": 123456,
      "situacao": "ELEITO"
    }
  ]
}
```

### `GET /polarizacao?uf=SP&ano_governador=2022&ano_municipal=2024`

Resposta `200`:

```json
{
  "federal": [
    {
      "uf": "SP",
      "partido": "PL",
      "espectro": "direita",
      "votos": 1000000,
      "status": "ELEITO",
      "eleito": true,
      "ano": 2022,
      "turno": 2
    }
  ],
  "municipal_brasil": [
    {
      "uf": "SP",
      "espectro": "esquerda",
      "partido_representativo": "PT",
      "total_prefeitos": 645,
      "ano": 2024
    }
  ],
  "municipal_uf": [
    {
      "uf": "SP",
      "municipio": "SAO PAULO",
      "partido": "PT",
      "espectro": "esquerda",
      "votos": 500000,
      "status": "ELEITO",
      "eleito": true,
      "ano": 2024,
      "turno": 2
    }
  ]
}
```

Erro `422` (ex.: `top_n > 100`):

```json
{
  "message": "Parametros de consulta invalidos."
}
```

### `GET /distribuicao?group_by=genero&ano=2022&uf=SP`

Resposta `200`:

```json
{
  "group_by": "genero",
  "items": [
    { "label": "MASCULINO", "value": 1200, "percentage": 62.37 },
    { "label": "FEMININO", "value": 724, "percentage": 37.63 }
  ]
}
```

Erro `400`:

```json
{
  "message": "group_by invalido ou coluna ausente no dataset"
}
```

### `GET /ocupacao-genero?ano=2022&uf=SP`

Resposta `200`:

```json
{
  "items": [
    { "ocupacao": "EMPRESARIO", "masculino": 21000, "feminino": 8300 },
    { "ocupacao": "SERVIDOR PUBLICO MUNICIPAL", "masculino": 14000, "feminino": 15600 }
  ]
}
```

### `GET /idade?ano=2022&uf=SP&cargo=Deputado%20Estadual`

Resposta `200`:

```json
{
  "media": 47.21,
  "mediana": 47.0,
  "minimo": 21.0,
  "maximo": 82.0,
  "desvio_padrao": 11.08,
  "bins": [
    { "label": "18-29", "value": 120, "percentage": 6.24 },
    { "label": "30-39", "value": 430, "percentage": 22.35 },
    { "label": "40-49", "value": 610, "percentage": 31.7 },
    { "label": "50-59", "value": 470, "percentage": 24.43 },
    { "label": "60-69", "value": 240, "percentage": 12.47 },
    { "label": "70+", "value": 54, "percentage": 2.81 }
  ]
}
```

### `GET /vagas-oficiais?ano=2024&uf=SP&group_by=cargo`

Query params:
- `group_by` (default `cargo`): `cargo`, `uf`, `municipio`
- `ano` (opcional)
- `uf` (opcional, 2 chars)
- `cargo` (opcional)
- `municipio` (opcional)

Resposta `200`:

```json
{
  "group_by": "cargo",
  "total_vagas_oficiais": 4865,
  "items": [
    { "ano": 2024, "uf": "SP", "municipio": null, "cargo": "Vereador", "vagas_oficiais": 4243 },
    { "ano": 2024, "uf": "SP", "municipio": null, "cargo": "Prefeito", "vagas_oficiais": 622 }
  ]
}
```

Erro `400`:

```json
{
  "message": "group_by invalido, coluna ausente no dataset ou combinacao sem vagas oficiais (ex.: group_by=municipio para cargo nao municipal)."
}
```

## 7) Ajustes importantes para Mobile iOS

- Retornar JSON enxuto (sem HTML de gráfico).
- Contratos estáveis e tipados para `Codable`.
- Filtros por `ano`, `uf`, `cargo` no backend.
- Limite de payload com `top_n` (máximo 100).
- Endpoint de saúde para observabilidade no app.

## 8) OpenAPI versionado

Arquivo versionado do contrato:
- `docs/openapi.v1.json`

Regenerar apos mudancas de endpoint/schema:

```bash
./.venv/bin/python scripts/export_openapi.py
```

## 9) CI automatizada (GitHub Actions)

Workflow: `.github/workflows/ci.yml`

Executa em todo `push` e `pull_request`:
- roda lint (`ruff check .`)
- instala dependencias
- roda `pytest -q`
- regenera `docs/openapi.v1.json` e falha se o arquivo versionado estiver desatualizado

Rodar localmente antes do push:

```bash
./.venv/bin/ruff check .
./.venv/bin/python -m pytest -q
```

Quando a pipeline falhar por contrato OpenAPI:

```bash
./.venv/bin/python scripts/export_openapi.py
git add docs/openapi.v1.json
git commit -m "chore: update openapi contract"
```

No GitHub: aba `Actions` > workflow `CI` > abrir o job `test-and-contract`.

## 10) Carga agendada e snapshots

Workflow agendado: `.github/workflows/data-refresh.yml`

O que ele faz:
- baixa fontes públicas configuradas via secret `TSE_SOURCES_JSON` (preferencial)
- fallback para `config/tse_sources.json` (se existir)
- roda normalizacao de `data/raw` para `data/curated`
- gera `manifest.json` de auditoria da carga
- publica snapshot em `data/releases/YYYYMMDD`
- publica `latest/*` e `snapshots/YYYYMMDD/*` no Cloudflare R2 (quando secrets R2 existem)
- sobe artifacts da execucao no GitHub Actions

Agendamento semanal:
- cron atual: `0 9 * * 1` (toda segunda-feira, 09:00 UTC)
- para runs agendados, `publish_to_r2` fica ativo automaticamente
- anos padrão de normalização: `2000 2002 2004 2006 2008 2010 2012 2014 2016 2018 2020 2022 2024`

Configuração de fontes remotas:
- em `Settings > Secrets and variables > Actions > Secrets`, crie `TSE_SOURCES_JSON`
- valor do secret: JSON no mesmo formato do arquivo de exemplo
- copie `config/tse_sources.example.json` para `config/tse_sources.json`
- preencha URLs públicas reais do TSE e paths de destino em `data/raw`
- `config/tse_sources.json` é fallback local (ignorado no git), útil para execução local

Execução manual com multi-ano customizado:
- `Actions` > `Data Refresh` > `Run workflow`
- `normalize_years`: ex. `2000 2002 2004 2006 2008 2010 2012 2014 2016 2018 2020 2022 2024`
- `publish_to_r2`: `true`
- `keep_snapshots`: ex. `7`

### 10.1) Cloudflare R2 (baixo custo, limite inicial 10 GB)

1. Criar bucket no R2:
- Cloudflare Dashboard > `R2 Object Storage` > `Create bucket`
- nome sugerido: `tse-curated`
- região: automática (padrão)

2. Criar chave S3 API para R2:
- R2 > `Manage R2 API tokens` > `Create API token`
- permissão mínima: `Object Read & Write` no bucket `tse-curated`
- salve `Access Key ID` e `Secret Access Key`

3. Coletar o `Account ID`:
- Cloudflare Dashboard > coluna lateral (conta) > `Account ID`

4. Configurar secrets no GitHub:
- GitHub > repo > `Settings` > `Secrets and variables` > `Actions`
- em `Repository secrets`, criar:
  - `R2_ACCOUNT_ID`
  - `R2_ACCESS_KEY_ID`
  - `R2_SECRET_ACCESS_KEY`
  - `R2_BUCKET` (ex.: `tse-curated`)

5. Rodar upload no workflow:
- `Actions` > `Data Refresh` > `Run workflow`
- inputs:
  - `publish_to_r2`: `true`
  - `keep_snapshots`: `7` (ou menos para economizar espaço)

6. Estratégia para ficar no limite de 10 GB:
- mantenha apenas:
  - `latest/analytics.csv` (ou `analytics.parquet`)
  - `latest/manifest.json`
  - `latest/quality_report.json`
  - poucos diretórios em `snapshots/` (ex.: últimos 7)
- prefira Parquet quando possível para reduzir tamanho:
  - `pip install pyarrow`
  - gerar `data/curated/analytics.parquet`
- não publique `data/raw` completo no R2 nesse estágio

Teste local opcional de upload para R2:

```bash
python3 scripts/upload_to_r2.py \
  --account-id "$R2_ACCOUNT_ID" \
  --access-key-id "$R2_ACCESS_KEY_ID" \
  --secret-access-key "$R2_SECRET_ACCESS_KEY" \
  --bucket "$R2_BUCKET" \
  --source-dir data/curated \
  --releases-dir data/releases \
  --keep-snapshots 7
```

Execucao manual:
- GitHub > `Actions` > `Data Refresh` > `Run workflow`

Validacao go/no-go local:

```bash
python3 scripts/go_no_go.py \
  --analytics data/curated/analytics.csv \
  --quality-report data/curated/quality_report.json \
  --manifest data/curated/manifest.json \
  --require-release \
  --max-duplicate-rows 0 \
  --max-negative-votes 0 \
  --max-required-null-rate 0.02
```

Criterios criticos (NO-GO se falhar):
- arquivos curated obrigatorios ausentes
- colunas obrigatorias faltantes
- duplicidade/votos negativos acima do limite
- taxa de nulos acima do limite por coluna obrigatoria
- manifest sem hash/output/fontes

## 11) Deploy (staging/producao)

Arquivos:
- `Dockerfile`
- `docker-compose.yml`

Subir local/staging:

```bash
docker compose up -d --build
```

Validar:

```bash
curl -s http://localhost:8000/health
curl -s http://localhost:8000/metrics
```

Smoke test de cliente (local/staging):

```bash
python3 scripts/smoke_test_api.py --base-url http://localhost:8000
```

Exemplo para staging:

```bash
python3 scripts/smoke_test_api.py --base-url https://seu-staging.exemplo.com
```

Smoke test via GitHub Actions:
- workflow: `.github/workflows/staging-smoke.yml`
- execucao: `Actions` > `Staging Smoke` > `Run workflow`
- preencher `base_url` com a URL de staging
- o job falha automaticamente se algum endpoint crítico falhar

### 11.1) Producao no EasyPanel

Fluxo recomendado para a VPS Hostinger com EasyPanel:

- serviço `App` apontando para o repo GitHub
- build via `Dockerfile`
- domínio público `https://apitse.safeartlabs.com`
- bootstrap de dados pelo Cloudflare R2
- SSL gerenciado pelo EasyPanel/Traefik

Variáveis mínimas de produção no EasyPanel:

```env
ANALYTICS_DATA_PATH=data/curated/analytics.parquet
ANALYTICS_ENCODING=utf-8
ANALYTICS_ENGINE=duckdb
ANALYTICS_SEPARATOR=,
APP_NAME=MeuCandidato Analytics API
APP_VERSION=1.0.0
DEFAULT_TOP_N=20
DUCKDB_CREATE_INDEXES=false
DUCKDB_DATABASE_PATH=/tmp/meucandidato_analytics.duckdb
DUCKDB_MATERIALIZE_TABLE=false
DUCKDB_MEMORY_LIMIT_MB=2048
DUCKDB_THREADS=4
GUNICORN_TIMEOUT=120
GUNICORN_WORKERS=1
MAX_TOP_N=100
PREFER_PARQUET_IF_AVAILABLE=true
R2_ACCOUNT_ID=<cloudflare_account_id>
R2_ACCESS_KEY_ID=<r2_access_key_id>
R2_BUCKET=tse-curated
R2_CONNECT_TIMEOUT_SECONDS=5
R2_ENDPOINT=https://<cloudflare_account_id>.r2.cloudflarestorage.com
R2_OBJECT_KEY_CSV=latest/analytics.csv
R2_OBJECT_KEY_PARQUET=latest/analytics.parquet
R2_READ_TIMEOUT_SECONDS=30
R2_REGION_NAME=auto
R2_SECRET_ACCESS_KEY=<r2_secret_access_key>
RATE_LIMIT_ENABLED=true
RATE_LIMIT_MAX_REQUESTS_PER_IP=120
RATE_LIMIT_WINDOW_SECONDS=60
```

Checklist de smoke test após cada deploy:

1. `GET /health` deve retornar `status=ok` e `data_loaded=true`.
2. `GET /v1/analytics/filtros` deve retornar listas não vazias.
3. `GET /v1/analytics/overview?ano=<ano>` deve responder `200`.
4. `GET /v1/analytics/candidatos?query=ca&page=1&page_size=5` deve responder com `items` e `total`.
5. `GET /v1/analytics/distribuicao?group_by=genero&ano=<ano>` deve responder `200`.

Deploy via GitHub Actions em produção:

- workflow: `.github/workflows/deploy.yml`
- secret recomendado: `PROD_BASE_URL` com `https://apitse.safeartlabs.com`
- o EasyPanel faz auto deploy a partir do GitHub
- o workflow aguarda o rollout e executa `scripts/smoke_test_api.py`

Variáveis recomendadas para bootstrap de dados via R2:
- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET` (`tse-curated`)
- `R2_OBJECT_KEY_CSV` (`latest/analytics.csv`)
- `R2_OBJECT_KEY_PARQUET` (`latest/analytics.parquet`)

## 12) Estrutura final do projeto

```text
API-MeuCandidato/
  .github/workflows/
    ci.yml
    data-refresh.yml
    staging-smoke.yml
  app/
    main.py
    config.py
    schemas.py
    services/
      analytics_service.py
      duckdb_analytics_service.py
  scripts/
    normalize.py
    convert_csv_to_parquet.py
    export_openapi.py
    publish_snapshot.py
    upload_to_r2.py
    smoke_test_api.py
  data/
    curated/
      .gitkeep
    raw/
      .gitkeep
    releases/
  docs/
    openapi.v1.json
  Dockerfile
  docker-compose.yml
  .dockerignore
  .env.example
  .gitignore
  requirements.txt
  README.md
```
