# API MeuCandidato (iOS Analytics)

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
- `GET /v1/analytics/filtros`
- `GET /v1/analytics/overview?ano=2022&uf=SP&cargo=Deputado%20Estadual`
- `GET /v1/analytics/top-candidatos?ano=2022&uf=SP&cargo=Deputado%20Estadual&top_n=20`
- `GET /v1/analytics/distribuicao?group_by=genero&ano=2022&uf=SP`
- `GET /v1/analytics/distribuicao?group_by=status&ano=2022&uf=SP&cargo=Senador`
- `GET /v1/analytics/idade?ano=2022&uf=SP&cargo=Deputado%20Estadual`

`group_by` aceitos: `status`, `genero`, `instrucao`, `cor_raca`, `estado_civil`, `ocupacao`, `cargo`, `uf`.

## 6) Ajustes importantes para Mobile iOS

- Retornar JSON enxuto (sem HTML de gráfico).
- Contratos estáveis e tipados para `Codable`.
- Filtros por `ano`, `uf`, `cargo` no backend.
- Limite de payload com `top_n` (máximo 100).
- Endpoint de saúde para observabilidade no app.

## 7) Estrutura final do projeto

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
  data/
    .gitkeep
  .env.example
  .gitignore
  requirements.txt
  README.md
```
