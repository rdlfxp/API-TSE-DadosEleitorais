# Performance e cache

## Como a API responde rápido

1. **Grão dos dados.** O `analytics.parquet` publicado tem 1 linha por candidato e turno nos cargos
   estaduais e de deputado (Governador, Senador, Deputado Federal/Estadual/Distrital). Só **Presidente**
   (e Distrital) guarda detalhe por município/zona, usado no mapa. As rotas contam linhas, então manter
   2026 por zona multiplicava eleitos e candidatos. `scripts/merge_curated_year.py` compacta a partição
   nova por padrão (`--no-compact` desliga).
2. **Cache em duas camadas.** L1 em memória (pequeno) + Redis (L2, compartilhado e persistente).
   Todas as rotas de análise passam por `run_analytics_query`.
3. **Chave versionada pelo snapshot:** `"{mtime_ns}:{tamanho}":{rota}:{params}`. Publicar um snapshot novo
   muda a versão e invalida tudo sem `FLUSHALL`. Eleições encerradas ficam 7 dias
   (`ANALYTICS_CLOSED_CACHE_TTL_SECONDS`); `/v1/resultados` fica em 10 s.
4. **Single-flight.** N pedidos simultâneos do mesmo recorte frio executam 1 consulta (lock por chave no
   processo + lock `SET NX EX` no Redis para várias réplicas). Os demais esperam e leem o cache.
5. **Aquecimento.** No start (`CACHE_WARMUP_ENABLED=true`) e após cada publicação (`scripts/warm_cache.py`).
6. **Resiliência.** Redis fora do ar nunca derruba a API: timeout de 1 s e fallback para L1.

## Variáveis (EasyPanel, serviço `api-tse`)

| Variável | Valor |
|---|---|
| `REDIS_ENABLED` | `true` |
| `REDIS_URL` | a URL interna da aba **Credentials** do serviço Redis (`redis://default:SENHA@HOST:6379`) |
| `CACHE_WARMUP_ENABLED` | `true` |
| `DUCKDB_THREADS` | número de vCPUs do VPS (hoje `1`) |

## Publicar um snapshot

1. Data Refresh (GitHub Actions) com `incremental_year=<ano>`: baixa a base do R2, substitui só o ano,
   valida endpoints e publica `latest/` + `snapshots/YYYYMMDD/`.
2. Reiniciar o `api-tse` no EasyPanel (baixa o `latest/` e abre o snapshot novo).
3. Aquecer: o start já faz isso se `CACHE_WARMUP_ENABLED=true`. Manualmente:
   `python scripts/warm_cache.py --base-url https://apitse.safeartlabs.com` (`--no-ufs` só Brasil).
4. Conferir `GET /health/cache`: `snapshot` novo, `warmup.state=done`, `redis.keys` > 0.

## Reverter

Os 7 últimos snapshots ficam em `snapshots/YYYYMMDD/` no R2. Para voltar: copie os 4 arquivos do snapshot
anterior para `latest/` (console do R2) e reinicie o `api-tse`. A versão da chave muda sozinha e o cache
do snapshot ruim deixa de ser usado; o Redis o expira pelo TTL ou por `allkeys-lru`.

## Diagnóstico

- `GET /health/cache`: snapshot ativo, hit-rate (L1+L2 e do Redis), esperas de single-flight, tempo
  médio/máximo por rota, estado do aquecimento.
- Header `X-Memory-Cache: HIT|MISS|BYPASS|STALE_HIT` em cada resposta.
- Medir tudo: `scripts/profile_endpoint_costs.py` e `curl -w "%{time_total}"`.
