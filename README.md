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

Por padrão, a API tenta carregar `data/analytics.csv`.

Você pode configurar via `.env`:

```env
ANALYTICS_DATA_PATH=data/analytics.csv
ANALYTICS_SEPARATOR=,
ANALYTICS_ENCODING=utf-8
```

## 3) Run

```bash
uvicorn app.main:app --reload
```

## 4) Normalizacao multi-ano (2014/2018/2022)

Use o script para gerar um unico `data/analytics.csv` a partir dos CSVs brutos:

```bash
python3 scripts/normalize.py \
  --votacao /caminho/votacao_candidato_munzona_2014_*.csv /caminho/votacao_candidato_munzona_2018_*.csv /caminho/votacao_candidato_munzona_2022_*.csv \
  --consulta /caminho/consulta_cand_2014_*.csv /caminho/consulta_cand_2018_*.csv /caminho/consulta_cand_2022_*.csv \
  --output data/analytics.csv \
  --report data/quality_report.json
```

Notas:
- O script assume padrao TSE (`sep=';'`, `encoding='latin1'`).
- Se `consulta_cand` nao for enviada, ainda gera arquivo normalizado com colunas disponiveis.
- A API passa a funcionar para qualquer ano/cargo disponivel no arquivo final.
- O relatorio JSON traz checagens por ano: colunas faltantes, nulos criticos, duplicidade e total de votos.

Quality gate (bloqueio por qualidade):

```bash
python3 scripts/normalize.py \
  --votacao /caminho/votacao_2018.csv /caminho/votacao_2022.csv \
  --consulta /caminho/consulta_2018.csv /caminho/consulta_2022.csv \
  --output data/analytics.csv \
  --report data/quality_report.json \
  --quality-gate \
  --max-duplicate-rows 0 \
  --max-negative-votes 0 \
  --max-required-null-rate 0.02
```

Se reprovar no gate, o script encerra com codigo `2` e nao grava o `analytics.csv`.

## 5) Endpoints para iOS

- `GET /health`
- `GET /v1/analytics/filtros` (`200`, `503`)
- `GET /v1/analytics/overview?ano=2022&uf=SP&cargo=Deputado%20Estadual` (`200`, `422`, `503`)
- `GET /v1/analytics/top-candidatos?ano=2022&uf=SP&cargo=Deputado%20Estadual&top_n=20` (`200`, `422`, `503`)
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
  "message": "Base analytics indisponivel. Ajuste ANALYTICS_DATA_PATH ou coloque o arquivo em data/analytics.csv."
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

## 10) Estrutura final do projeto

```text
API-MeuCandidato/
  app/
    main.py
    config.py
    schemas.py
    services/
      analytics_service.py
  scripts/
    normalize.py
    export_openapi.py
  docs/
    openapi.v1.json
  data/
    .gitkeep
  .env.example
  .gitignore
  requirements.txt
  README.md
```
