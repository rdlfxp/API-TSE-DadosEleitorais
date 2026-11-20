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

## Pendente / fora desta lista
- Publicação da base (ver README, "Reconstruir 2002, 2006 e 2008") e validação contra o TSE.
- Itens ainda abertos no doc do app: #2, #3, #6–#11, #20 (e #25/#26).
