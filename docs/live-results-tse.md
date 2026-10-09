# Resultados ao vivo: estrutura TSE

O adaptador em `app/services/live_results/tse_divulgacao.py` consome os arquivos
JSON públicos da divulgação oficial do TSE. Os fixtures em
`tests/fixtures/live_results/` foram baixados dos arquivos oficiais de 2024 e
2026.

## URLs

- 2026 Presidente: `/oficial/ele2026/6257/dados/br/br-c0001-e006257-u.json`
- 2024 Prefeito de São Paulo (código eleitoral 71072): `/oficial/ele2024/619/dados/sp/sp71072-c0011-e000619-u.json`

O caminho é montado a partir do ano/turno, código do cargo e localização. Os
códigos de eleição ficam em `LIVE_RESULTS_ELECTIONS_JSON`, por padrão `6257`
para o primeiro turno geral de 2026 e `619/620` para os turnos municipais de
2024.

## Campos observados

- `dg`/`hg`: data e hora oficiais da última geração;
- `tf` e `and`: estado da totalização;
- `s`: seções (`psi` é o percentual de seções totalizadas);
- `e`: eleitorado, comparecimento e abstenções;
- `v`: votos válidos, brancos e nulos;
- `carg[].agr[].par[].cand[]`: candidatos, partido, votos, percentual e situação.

O TSE entrega números e percentuais como strings, com percentuais usando vírgula.
O adaptador converte somente valores efetivamente presentes; campos ausentes são
omitidos da resposta. A hora sem fuso do arquivo recebe o fuso oficial
`America/Sao_Paulo`.

O endpoint próprio é `GET /v1/resultados`. Ele pagina candidatos ordenados por
votos decrescentes, retorna `404` para recorte não publicado, `503` com
`retryable: true` quando o TSE está indisponível e usa `Cache-Control: public,
max-age=10`, sem stale-if-error.
