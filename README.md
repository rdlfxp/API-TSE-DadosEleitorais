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
- `data/curated/analytics.csv`: base consolidada consumida pela API (saida padrao)
- `data/curated/quality_report.json`: relatorio de qualidade da consolidacao

Por padrao, a API tenta carregar `data/curated/analytics.csv`.

Você pode configurar via `.env`:

```env
ANALYTICS_DATA_PATH=data/curated/analytics.csv
ANALYTICS_SEPARATOR=,
ANALYTICS_ENCODING=utf-8
```

## 3) Run

```bash
uvicorn app.main:app --reload
```

## 4) Normalizacao multi-ano (2014/2018/2022)

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
  --consulta-pattern '*consulta_cand*.csv' \
  --exclude-pattern '*.DS_Store' '*classificado*.csv' \
  --output data/curated/analytics.csv \
  --report data/curated/quality_report.json
```

Parquet opcional (melhor performance em volume alto):

```bash
pip install pyarrow
python3 scripts/normalize.py \
  --raw-dir data/raw \
  --years 2018 2020 2022 2024 \
  --output data/curated/analytics.parquet \
  --report data/curated/quality_report.json
```

Para integrar outras variacoes de planilha de votacao, adicione mais padroes:

```bash
python3 scripts/normalize.py \
  --raw-dir data/raw \
  --years 2018 2020 2022 2024 \
  --votacao-pattern '*votacao_candidato*munzona*.csv' '*votacao_candidato*.csv' \
  --consulta-pattern '*consulta_cand*.csv'
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
- `GET /v1/analytics/candidatos?query=candidato&ano=2022&uf=SP&cargo=Deputado%20Estadual&page=1&page_size=20` (`200`, `422`, `503`)
- `GET /v1/analytics/distribuicao?group_by=genero&ano=2022&uf=SP` (`200`, `400`, `422`, `503`)
- `GET /v1/analytics/distribuicao?group_by=status&ano=2022&uf=SP&cargo=Senador` (`200`, `400`, `422`, `503`)
- `GET /v1/analytics/idade?ano=2022&uf=SP&cargo=Deputado%20Estadual` (`200`, `422`, `503`)

`group_by` aceitos: `status`, `genero`, `instrucao`, `cor_raca`, `estado_civil`, `ocupacao`, `cargo`, `uf`.

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
- baixa fontes públicas configuradas em `config/tse_sources.json` (se existir)
- roda normalizacao de `data/raw` para `data/curated`
- gera `manifest.json` de auditoria da carga
- publica snapshot em `data/releases/YYYYMMDD`
- sobe artifacts da execucao no GitHub Actions

Configuração de fontes remotas:
- copie `config/tse_sources.example.json` para `config/tse_sources.json`
- preencha URLs públicas reais do TSE e paths de destino em `data/raw`
- o arquivo `config/tse_sources.json` é local (ignorando no git), para evitar hardcode rígido

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
  scripts/
    normalize.py
    export_openapi.py
    publish_snapshot.py
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
