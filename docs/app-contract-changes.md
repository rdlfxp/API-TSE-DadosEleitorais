# Mudanças de contrato da API que afetam o app

Registro vivo, para gerar o prompt de ajuste do app no final. Status:
**código** = pronto e testado na API; **publicar** = só vale em produção depois que a base nova for publicada
(reconstrução de 2002/2006/2008, vagas 2018–2024, `electorate_profile.parquet` e fotos) e o `api-tse` reiniciado.

Itens de `docs/pendencias-backend.md` do app: #27, #4, #15, #16, #1, #21, #17, #18, #19, #12, #5, #24, #13, #14, #9.

## 1. Busca de candidatos (`GET /v1/analytics/candidatos/search`) — #4 — código
- `q` casa por nome de registro, **nome de urna** e, se só tiver dígitos, pelo **número exato**. Sem acento e sem caixa.
- Item novo: `nm_urna_candidato` (string ou `null`).
- Sem `uf`/`cargo`/`partido`, exige 4+ caracteres (antes 8) ou um número; abaixo disso devolve lista vazia.
- Mais rápida (índice compacto): busca sem filtro caiu de ~9 s para ~0,3 s.
- **App:** exibir `nm_urna_candidato`; filtrar localmente também por ele e por `numero`; remover qualquer exigência
  de 8 caracteres no campo de busca (o mínimo agora é 4, ou número).

## 2. Histórico e identidade — #27 — código + publicar
- `person_id` é o mesmo em todos os anos da pessoa (Lula: 2002, 2006, 2022 e 2026). `vote-history` traz todos.
- `vote-history` e demais rotas de candidato casam `candidate_id` por **SQ primeiro**, depois número, CPF e nome.
- Itens do histórico de **Presidente** vêm com `state: "BR"` (antes uma UF qualquer).
- **App:** tratar `state == "BR"` como "Brasil"; agrupar trajetória por `person_id`; remover contornos que
  completavam o histórico à mão.

## 3. Eleitos e candidatos contam pessoas — #15, #16, #21 — código
Vale para `/overview`, `/ranking`, `/serie-temporal` e `/mapa-uf` (métricas `eleitos` e `candidatos`).
- `total_eleitos` do overview de uma UF/município sem `cargo` **não inclui o Presidente**.
  Ex.: SP/2022 = 166; São Paulo (cidade)/2024 = 56; Brasil/2022 = 1.627.
- `ranking?metric=eleitos`: vereadores de São Paulo/2024 somam 55 (antes 3.135).
- O rótulo antigo `MÉDIA` (2002/2006/2010) conta como eleito: Câmara = 513 em todos os anos (antes 446 em 2010).
- `total_votos_nominais` **não mudou** (soma das linhas do recorte; mistura cargos/turnos).
- **App:** voltar a exibir `total_eleitos` no detalhe do local; usar o ranking de vereadores/eleitos; remover o aviso
  de "Base parcial" das bancadas de 2002–2010 (a base 2010 agora fecha 513/1.035/24); remover contornos de contagem.

## 4. Vagas oficiais (`/vagas-oficiais`) — #1 — código (+ publicar para o oficial de 2018–2024)
- Anos sem `QT_VAGAS` oficial passam a usar eleitos distintos; cargos de vaga única (Presidente, Governador,
  Prefeito e vices) valem 1. Fim dos `vagas_oficiais: 0` em 2022 e 2024.
- Após publicar, 2022 e os municípios de SP em 2024 batem 100% com o oficial do TSE. Totais nacionais municipais
  (2020/2024) ficam levemente abaixo do oficial (ex.: Prefeito 2024: 5.561 vs 5.599), porque cidades sem nenhuma
  linha de candidato na base não entram. Senador 2018 = 53, igual ao arquivo oficial por UF (MT consta com 1 vaga).
- **App:** habilitar o cálculo da "Meta de votos" (2028) com esses dados e remover o estado "sem dados de vagas"
  para 2022/2024.

## 5. `top-candidatos` — #17 — código
- Novo parâmetro `somente_eleitos` (bool, padrão `false`), filtrado no servidor.
- **App:** enviar `somente_eleitos=true` quando quiser só eleitos e remover o filtro local por `situacao`.

## 6. `ranking` — #18 — código
- `page_size` é sinônimo de `top_n`; valores diferentes dão `400`. Sem nenhum dos dois, o corte continua em 20.
- **App:** continuar enviando `top_n` (nada obrigatório); se usar `page_size`, enviar o mesmo valor.

## 7. `polarizacao` — #19 — código
- `municipal_uf` traz **apenas prefeitos eleitos**.
- Campo novo `municipal_sem_resultado`: `[{uf, municipio, status, ano, turno}]` para eleição anulada/pendente
  (SP/2024: Brejo Alegre, Reginópolis, Tuiuti).
- `municipal_brasil`: contagens e espectro só com eleitos; `total_prefeitos` segue o total oficial.
- **App:** parar de descartar `eleito == false`; adicionar `municipal_sem_resultado` ao modelo `Codable`
  (opcional, com valor padrão `[]`) e, se quiser, exibir "eleição sem resultado" no mapa.

## 8. `vote-distribution?level=municipio` — #12 — código
- Cada item ganha `uf`, `cd_municipio` (código TSE, 5 dígitos) e `cd_municipio_ibge` (7 dígitos).
  Lula/SP/2026: 645 de 645 cidades com IBGE (antes 8 não casavam por nome).
- Cidades homônimas de UFs diferentes (ex.: BOA VISTA em PB e RR) deixam de ser fundidas: a chave agora é
  nome + UF + código.
- Nome `"BRASIL"` (partições antigas do Presidente) é trocado pelo nome real do município.
- `cd_municipio_ibge` é `null` só no exterior (`uf == "ZZ"`). Brasília (DF) e Fernando de Noronha (PE) resolvem.
- Tabela de códigos: `app/data/municipios_tse_ibge.json` (5.571 municípios), gerada por
  `scripts/build_municipios_table.py` a partir do cadastro oficial do TSE.
- **App:** casar a malha do IBGE por `cd_municipio_ibge` em vez de por nome; remover o casamento por nome e o
  contorno dos 8 municípios de SP; tratar `uf == "ZZ"` como exterior (sem polígono).

## 9. `vote-distribution?level=zona` — #5 — código
- Todo item de zona (qualquer cargo) traz `zone_id`, `zone_name`, `municipio`, `uf`, `cd_municipio` e
  `cd_municipio_ibge`. Antes, Presidente/Governador/Senador/Deputados vinham só com `key`, `label`
  ("SÃO PAULO - Zona 372"), `votes` e `vote_share`.
- `key` e `label` **não mudaram**. Atenção: no ramo de cargos estaduais/nacionais o `key` é só o número da zona e se
  repete entre cidades (181 repetidos nas 779 zonas de SP/2026). A identidade única é
  `(cd_municipio_ibge, zone_id)`.
- **App:** parar de extrair o número da zona do texto do `label`; usar `zone_id`, `municipio`, `uf`; usar
  `(cd_municipio_ibge, zone_id)` como identidade em listas que misturam cidades (ex.: `ForEach`).

## 10. Municípios por UF — #24 — código (rota nova)
- `GET /v1/analytics/municipios?uf=SP&ano=2024` devolve `{uf, ano, total, items}`; cada item:
  `{nome, cd_municipio (TSE), cd_municipio_ibge, segundo_turno}`, em ordem alfabética sem acento.
- `segundo_turno`: `true`/`false` conforme a eleição de Prefeito daquele município no `ano` (SP/2024: 18 cidades com
  2º turno); `null` se não houve eleição municipal na UF nesse ano (ex.: `ano=2022`). Sem `ano`, usa o ano municipal
  mais recente da base. Para 2028 o flag só existirá depois do 1º turno; a elegibilidade (>200 mil eleitores) continua
  sendo regra do app.
- `/v1/analytics/filtros` **não mudou** (a lista completa tem ~390 KB e a rota é chamada sempre).
- **App:** carregar os municípios da UF escolhida por esta rota (busca local, "minha cidade"), usar
  `cd_municipio_ibge` para a malha e `segundo_turno` para o seletor de turno; remover tabelas de município embutidas.

## 11. `electorate-profile` — #13 — código + publicar (precisa do arquivo `electorate_profile.parquet`)
- Passa a devolver o perfil do **eleitorado** onde o candidato teve votos (média do perfil de cada município, ou da
  UF quando o cargo não tem detalhe municipal, ponderada pelos votos dele). Antes era o perfil do próprio candidato
  (uma candidata dava `female_share: 100`). Fonte: perfil do eleitorado do TSE (2000–2026).
- Campos novos: `basis` (`"eleitorado"` ou `"indisponivel"`), `unavailable`, `electorate_year` (ano do perfil usado: o da eleição ou o
  mais recente anterior) e `coverage` (% dos votos cobertos), mais `age_bands.under_18` (16–17 anos).
- `unavailable`: lista de dimensões sem dado, que o app deve ocultar: `renda` e `urbano` (sempre), `idade`
  (**2000, 2002, 2004 e 2006 não têm faixa etária no TSE**; gênero e escolaridade existem) e `escolaridade`.
  Fora esses anos, `age_bands` soma ~100% entre quem tem idade informada.
- `basis == "indisponivel"` (sem o arquivo no servidor ou sem dado): gênero e idade vêm zerados e
  `dominant_education` = `"N/A"`. **Não exibir zeros como dado.**
- `dominant_income_band` é sempre `"N/A"` e `urban_concentration` é sempre `null` (o TSE não publica renda nem
  zona urbana/rural no perfil do eleitorado). `urban_concentration` virou opcional.
- **App:** decodificar `basis`/`electorate_year`/`coverage` (opcionais) e `urban_concentration` como `Double?`;
  só mostrar o card quando `basis == "eleitorado"`; mostrar "Perfil do eleitorado de {electorate_year}"; ocultar
  renda e urbano. `age_bands` agora soma ~100% com `under_18`.

## 12. `compare` — campo `retention` — #14 — código
- `retention` = votos desta eleição ÷ votos da eleição **anterior do mesmo cargo e território** (mesma UF; mesmo
  município nos cargos municipais), no mesmo turno, × 100. Lula 2026 vs 2022: 106,48.
- É `null` sem eleição anterior comparável (primeira candidatura ou troca de cargo). Antes dava valores como
  6.798.413,75 (dividia votos de Presidente por votos de Vereador) e usava a eleição mais antiga.
- `deltas` só inclui `retention` quando ao menos 2 candidatos têm valor.
- **App:** `retention` agora é `Double?`; voltar a exibir quando houver valor ("retém X% da votação anterior") e
  omitir quando `null`.

## 13. Fotos de 2006 a 2022 — #9 — código + publicar (precisa do `candidate_photos.parquet` e das imagens no CDN)
- `foto_url` de `GET /v1/candidates/{id}/summary` passa a vir preenchido também para eleições de 2006 a 2022
  (antes só 2024 e 2026). É uma URL https do CDN próprio da API, com cache imutável de 1 ano; a API não serve a imagem.
- Continua opcional: `null` quando o TSE não publicou a foto daquele candidato. O contrato não mudou (mesmo campo e tipo);
  só a descrição ganhou "ou, nos anos antigos, o storage da API".
- 2000 e 2002 não têm pacote de fotos no TSE (404). Sem foto, o app segue usando a foto mais recente do mesmo
  `person_id` e o avatar do partido.
- Tamanho: JPEG com no máximo 360 px (média ~5–6 KB), adequado para lista e resumo; não há URL de tamanho maior.
- **App:** nada a mudar no `Codable`; o `AsyncImage` e o `URLCache` atuais cobrem. Pode remover o contorno que
  reaproveita a foto de outra eleição **depois** que a publicação das fotos estiver no ar (manter até lá).

## Tabela de decodificação (modelos Codable)
| Modelo | Campo novo | Observação |
|---|---|---|
| item da busca | `nm_urna_candidato: String?` | pode ser `null` |
| `PolarizacaoResponse` | `municipal_sem_resultado: [SemResultado]` | opcional, padrão `[]` |
| `SemResultado` | `uf`, `municipio`, `status?`, `ano?`, `turno?` | novo tipo |
| item de `vote-distribution` | `uf?`, `cd_municipio?`, `cd_municipio_ibge?` | níveis município e zona |
| `MunicipiosResponse` | `uf`, `ano?`, `total`, `items[]` | rota nova |
| `MunicipioItem` | `nome`, `cd_municipio`, `cd_municipio_ibge`, `segundo_turno?` | novo tipo |
| `ElectorateProfileResponse` | `basis`, `unavailable: [String]`, `electorate_year?`, `coverage?`; `urban_concentration` agora opcional | novo contrato |
| `AgeBandsProfile` | `under_18` | padrão 0 |
| `CompareCandidateItem` | `retention` agora opcional | `null` sem eleição comparável |
| item de zona | `zone_id?`, `zone_name?`, `municipio?` | agora sempre preenchidos |
| `CandidateSummaryResponse` | `foto_url: String?` | já existia; agora também 2006–2022 |

## Pendências futuras (registradas em 2026-11-20)

### A. Reconstrução 2002/2006/2008 corrigida e publicada em 2026-11-20 17:51Z (`rb4`)
**Estado atual:** `tse-curated/latest/analytics.parquet` (119.917.188 bytes) e `candidate_history.parquet`
(105.035.666 bytes) são a reconstrução corrigida; `candidate_photos.parquet` e `electorate_profile.parquet` já estavam
publicados. A API só lê o R2 na inicialização: **vale depois de reiniciar o `api-tse`**.
Histórico: a primeira reconstrução (`rb3`, 17:11Z) trouxe colunas vazias e foi revertida; a causa estava em
`scripts/merge_curated_year.py` (correção **não commitada**, com teste novo em `tests/test_merge_curated_year.py` e
ajuste em `tests/test_sq_collisions.py`):
- `DT_ELEICAO` vinha em dd/mm/aaaa e o `TRY_CAST ... AS DATE` a zerava (a base de 2026 já tem esse defeito hoje).
- `IDADE` já vinha vazia nos arquivos por ano de 2018–2024; agora é calculada de `DT_NASCIMENTO` e `DT_ELEICAO` (mesma
  regra de `normalize._compute_age`) e `FAIXA_ETARIA` é refeita. Conferido: idade igual à da base antiga em 100% dos
  candidatos que tinham valor (11.794 em 2022; 428.441 em 2024).
- `HISTORICO_CANDIDATURA_ID` do ano alvo não recebia o id sintético de reserva; o último ano da cadeia (2024) ficava com
  92% nulo e fora da ligação de identidades. Agora recebe, e a detecção de SQ repetido inclui as linhas do ano alvo.
Validação (base anterior x nova): mesmas colunas e tipos; nenhuma coluna piorou ≥ 2 pontos de nulos em nenhum ano
(`DS_COR_RACA` 2002–2008 nulo no lugar de `#NE` é equivalente: a API trata os dois como "não informado"). Votos de 2002
Presidente 1º turno = 84.952.512 e Lula 2002/2006 batem com o TSE; `vote-history` do Lula = 2026, 2022, 2006, 2002 (`BR`).
Mudanças esperadas: 2002 e 2006 sem inflação (1,05 bi → 611 mi; 954 mi → 574 mi), 2008 de 424 mi para 212 mi e
dobra de linhas (detalhe por zona; 2020 também), 2018/2022 Deputado Distrital de ~70 mil para ~1,4 mi de votos (DF),
CPF preenchido em 2002/2006/2008/2018/2020/2022, e CPFs com mais de um nome sobem de 0 para 24.492 (6.518 depois de tirar
acento e espaço duplo; é grafia e nome que mudou entre eleições, mesmo CPF).
Voltar atrás: a base anterior está em `scratchpad/rb4/base/`.
Ainda aberto: `DT_ELEICAO` de 2026 segue nula na base (vem de antes); `scripts/upload_to_r2.py` envia como snapshot a
última pasta de `data/releases/` (hoje o teste `20990102`), não a base publicada, e criou `snapshots/20990102` no R2;
`age_stats` com `somente_eleitos` conta linhas, então em anos gerais as linhas do Presidente por município dominam a
distribuição (ex.: 6.405 de ~7.000 em 60–69 anos em 2018), mesma família dos itens #15/#16.

### A2. Anos 2000, 2004, 2010, 2012, 2014 e 2016 reconstruídos e publicados em 2026-11-20 18:26Z (`rb6`)
A base publicada tinha esses seis anos com votos 2x (todos os cargos exceto Presidente em 2010 e 2014), o Deputado
Distrital de 2010 ausente e CPF 100% nulo. Reconstruídos do bruto (`normalize.py` + `merge_curated_year.py` corrigido).
**Estado atual:** `latest/analytics.parquet` = 149.621.451 bytes e `candidate_history.parquet` = 115.897.842 bytes
(6.076.234 linhas). Vale depois de reiniciar o `api-tse`. A base de antes (rb4) está em `scratchpad/rb4/`.
Validação: votos 2000 361,9 mi → 181,0 mi; 2004 410,2 → 205,1; 2010 1.120,8 → 662,3; 2012 450,1 → 225,0; 2014 1.014,3 →
613,4; 2016 452,6 → 226,3. Razões entre cargos coerentes em todos os anos (Federal ÷ Presidente 0,85–0,93; Prefeito 1º
turno 85,0 mi em 2000, 96,0 em 2004, 101,0 em 2008, 104,7 em 2012, 105,7 em 2016, 102,0 em 2020, 112,0 em 2024).
Presidente de 2010 e 2014 bate com o TSE (Dilma 2010 = 47.651.434; válidos 2014 = 104.023.802). Eleitos iguais aos de
antes, exceto Distrital 2010: 0 → 24. Nenhuma coluna ficou com mais nulos; demais anos idênticos.
Detalhes que ficam: 2004 tem 10 pares de linhas com a mesma chave (o TSE grava `SQ_CANDIDATO = 1` como valor genérico em
2004; os votos não se perdem); 124 linhas de 2000/2004 com CPF `00000000000` ou `99999999999` (o ligador de
identidades recusa CPF de dígitos iguais); CPFs com mais de um nome (normalizado) = 50.253, quase todos grafia ou nome
que mudou entre eleições. Efeito no app: totais de votos e votos por candidato desses anos caem à metade.

### B. Fotos (`meucandidato-fotos`): baixar depois de validar o app
Índice publicado (`candidate_photos.parquet`): 2004 = 1.501 (só AC) · 2006 = 18.977 · 2008 = 94.488 (9 UFs) ·
2010 = 22.908 · 2012 = 23.440 (4 UFs) · 2014 = 26.195 · **2016 = 0** · 2018 = 29.119 · 2020 = 139.838 (9 UFs) ·
2022 = 29.266. Completos: só as eleições gerais (2006, 2010, 2014, 2018, 2022). As municipais (2008, 2012, 2016,
2020) estão incompletas, e é nelas que o app mais depende de foto.
- Contagem real no bucket (2026-11-20): 2016, 2024 e 2026 = 0 objetos. Há objetos além do índice nas UFs que foram
  interrompidas (2004: 3.644 contra 1.501 indexadas; 2008: 130.469 contra 94.488; 2012: 37.319 contra 23.440; 2020:
  162.182 contra 139.838); as chaves são fixas, então a retomada só completa e sobrescreve.
- Retomar (UFs com marcador `.uploaded` são puladas), sempre com `--bucket meucandidato-fotos --upload --upload-workers 2`:
  `--years 2016`, `--years 2020`, `--years 2012` (sem TO) e `--years 2008`, depois `--years 2004`.
  Em 2012 BA e 2008 MG a conexão com o R2 caiu (`EndpointConnectionError`); basta repetir.
- Depois: `--index-only --upload --bucket meucandidato-fotos`, e republicar o índice com `scripts/upload_to_r2.py`.
- 2024 e 2026: decisão de 2026-11-20 — também guardar no R2. O app tenta primeiro a fonte atual, depois o R2 e, por
  fim, a foto mais recente da mesma pessoa ou o avatar do partido. Falta definir qual `foto_url` a API devolve
  quando as duas existirem.
- Sem foto no TSE: 2000 e 2002; TO 2012 (zip corrompido no TSE); alguns arquivos com nome corrompido (2020 BA e MG).
- `--upload-workers` (em `scripts/build_candidate_photos.py`) ainda não foi commitado; `ensure_zip` não valida o zip.

### C. Para ver o app funcionando
Definir `PHOTOS_BASE_URL=https://fotos.safeartlabs.com` no EasyPanel e reiniciar o `api-tse` (a base corrigida do
item A já está no R2). Depois: `/health`; busca `lula`; `vote-history` do Lula (esperado 2002, 2006, 2022, 2026, todos
`BR`); `summary?year=2022` com `foto_url`; `electorate-profile` com `basis`; `overview?ano=2002&cargo=Presidente`; um
painel de idade de 2022 e de 2024.

### D. Itens do doc do app sem implementação no backend
#2/#22 (detalhe municipal de Deputado, Senador e Governador em 2026), #6/#25 (locais de votação e rota da zona),
#7/#8 (ao vivo: Presidente por UF/município e mapa por município), #10 (push da Live Activity), #11 (série da
apuração), #20 (espectro por partido), #23 (agenda de eleições) e #26 (candidaturas de 2028).
O #3 (Deputados do Amapá em 2026) depende do TSE publicar os dados.
