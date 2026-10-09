import pandas as pd

from scripts.normalize import (
    CANONICAL_COLUMNS,
    _finalize_normalized_frames,
    _normalize_votacao_file,
    _prepare_consulta,
    _prepare_historico,
    _prepare_vagas,
)


def _row(**overrides):
    base = {col: pd.NA for col in CANONICAL_COLUMNS}
    base.update(
        {
            "ANO_ELEICAO": 2022,
            "NR_TURNO": 2,
            "SG_UF": "SP",
            "NM_UE": "BRASIL",
            "CD_MUNICIPIO": pd.NA,
            "NR_ZONA": "1",
            "NR_SECAO": "1",
            "CD_CARGO": 1,
            "DS_CARGO": "Presidente",
            "SQ_CANDIDATO": 100,
            "NR_CANDIDATO": 13,
            "NM_CANDIDATO": "Candidato X",
            "NM_URNA_CANDIDATO": "Candidato X",
            "SG_PARTIDO": "PXX",
            "NM_PARTIDO": "Partido X",
            "TP_AGREMIACAO": "PARTIDO ISOLADO",
            "DS_SIT_TOT_TURNO": "ELEITO",
            "QT_VOTOS_NOMINAIS_VALIDOS": 10,
        }
    )
    base.update(overrides)
    return base


def test_finalize_normalized_frames_preserves_multirow_vote_totals():
    frame = pd.DataFrame(
        [
            _row(NR_ZONA="1", NR_SECAO="1", QT_VOTOS_NOMINAIS_VALIDOS=10),
            _row(NR_ZONA="1", NR_SECAO="2", QT_VOTOS_NOMINAIS_VALIDOS=20),
            _row(NR_ZONA="2", NR_SECAO="1", QT_VOTOS_NOMINAIS_VALIDOS=30),
        ]
    )

    finalized = _finalize_normalized_frames([frame])

    assert len(finalized) == 3
    assert int(finalized["QT_VOTOS_NOMINAIS_VALIDOS"].sum()) == 60


def test_finalize_normalized_frames_drops_exact_duplicate_rows_without_losing_votes():
    frame = pd.DataFrame(
        [
            _row(NR_ZONA="1", NR_SECAO="1", QT_VOTOS_NOMINAIS_VALIDOS=10),
            _row(NR_ZONA="1", NR_SECAO="1", QT_VOTOS_NOMINAIS_VALIDOS=10),
            _row(NR_ZONA="1", NR_SECAO="2", QT_VOTOS_NOMINAIS_VALIDOS=20),
        ]
    )

    finalized = _finalize_normalized_frames([frame])

    assert len(finalized) == 2
    assert int(finalized["QT_VOTOS_NOMINAIS_VALIDOS"].sum()) == 30


def test_normalize_votacao_accepts_legacy_qt_votos_nominais_column(tmp_path):
    votacao = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2014,
                "NR_TURNO": 2,
                "SG_UF": "SP",
                "NM_UE": "BRASIL",
                "CD_MUNICIPIO": "3550308",
                "NR_ZONA": "1",
                "NR_SECAO": "1",
                "CD_CARGO": 1,
                "DS_CARGO": "Presidente",
                "SQ_CANDIDATO": 10,
                "NR_CANDIDATO": 45,
                "NM_CANDIDATO": "Candidato X",
                "NM_URNA_CANDIDATO": "Candidato X",
                "SG_PARTIDO": "PXX",
                "NM_PARTIDO": "Partido X",
                "TP_AGREMIACAO": "PARTIDO ISOLADO",
                "DT_ELEICAO": "26/10/2014",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "QT_VOTOS_NOMINAIS": 123,
            }
        ]
    )
    path = tmp_path / "votacao_legacy.csv"
    votacao.to_csv(path, sep=";", index=False, encoding="latin1")

    normalized = _normalize_votacao_file(
        file_path=str(path),
        consulta_df=pd.DataFrame(),
        sep=";",
        encoding="latin1",
        chunk_size=0,
        merge_consulta=False,
    )

    assert len(normalized) == 1
    assert int(normalized["QT_VOTOS_NOMINAIS_VALIDOS"].iloc[0]) == 123


def test_normalize_votacao_merges_consulta_cpf_into_output(tmp_path):
    votacao = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "BRASIL",
                "CD_MUNICIPIO": "3550308",
                "NR_ZONA": "1",
                "NR_SECAO": "1",
                "CD_CARGO": 1,
                "DS_CARGO": "Prefeito",
                "SQ_CANDIDATO": 10,
                "NR_CANDIDATO": 45,
                "NM_CANDIDATO": "Candidato X",
                "NM_URNA_CANDIDATO": "Candidato X",
                "SG_PARTIDO": "PXX",
                "NM_PARTIDO": "Partido X",
                "TP_AGREMIACAO": "PARTIDO ISOLADO",
                "DT_ELEICAO": "06/10/2024",
                "DS_SIT_TOT_TURNO": "ELEITO",
                "QT_VOTOS_NOMINAIS_VALIDOS": 123,
            }
        ]
    )
    consulta = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2024,
                "SQ_CANDIDATO": 10,
                "NR_CANDIDATO": 45,
                "SG_UF": "SP",
                "DS_CARGO": "Prefeito",
                "NR_CPF_CANDIDATO": "12345678901",
                "NM_CANDIDATO": "Candidato X",
                "NM_URNA_CANDIDATO": "Candidato X",
            }
        ]
    )
    path = tmp_path / "votacao_consulta.csv"
    votacao.to_csv(path, sep=";", index=False, encoding="latin1")

    normalized = _normalize_votacao_file(
        file_path=str(path),
        consulta_df=consulta,
        sep=";",
        encoding="latin1",
        chunk_size=0,
        merge_consulta=True,
    )

    assert len(normalized) == 1
    assert "NR_CPF_CANDIDATO" in normalized.columns
    assert str(normalized["NR_CPF_CANDIDATO"].iloc[0]) == "12345678901"


def test_normalize_2026_uses_complement_history_vacancies_and_municipality(tmp_path):
    main = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2026,
                "SQ_CANDIDATO": 26001,
                "NR_CANDIDATO": 40123,
                "NR_CPF_CANDIDATO": -4,
                "SG_UF": "SP",
                "DS_CARGO": "Deputado Estadual",
                "NM_CANDIDATO": "Candidata 2026",
                "NM_URNA_CANDIDATO": "Candidata",
                "DS_GENERO": "NÃO DIVULGÁVEL",
                "DS_COR_RACA": "NÃO DIVULGÁVEL",
                "DS_GRAU_INSTRUCAO": "NÃO DIVULGÁVEL",
            }
        ]
    )
    complement = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2026,
                "SQ_CANDIDATO": 26001,
                "DS_GENERO_FEFC": "FEMININO",
                "DS_COR_RACA_FEFC": "PARDA",
                "NR_IDADE_DATA_POSSE": 42,
            }
        ]
    )
    history = pd.DataFrame(
        [
            {
                "ANO_ELEICAO_ATUAL": 2026,
                "SQ_CANDIDATO_ATUAL": 26001,
                "ANO_ELEICAO": 2022,
                "SQ_CANDIDATO": 22001,
            }
        ]
    )
    vacancies = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2026,
                "SG_UF": "SP",
                "NM_UE": "SÃO PAULO",
                "DS_CARGO": "Deputado Estadual",
                "QT_VAGA": 94,
            }
        ]
    )
    vote = pd.DataFrame(
        [
            {
                "ANO_ELEICAO": 2026,
                "NR_TURNO": 1,
                "SG_UF": "SP",
                "NM_UE": "SÃO PAULO",
                "CD_MUNICIPIO": 71072,
                "NM_MUNICIPIO": "CAMPINAS",
                "NR_ZONA": 1,
                "CD_CARGO": 7,
                "DS_CARGO": "Deputado Estadual",
                "SQ_CANDIDATO": 26001,
                "NR_CANDIDATO": 40123,
                "NM_CANDIDATO": "Candidata 2026",
                "NM_URNA_CANDIDATO": "Candidata",
                "SG_PARTIDO": "PSB",
                "NM_PARTIDO": "Partido Socialista Brasileiro",
                "TP_AGREMIACAO": "PARTIDO ISOLADO",
                "DT_ELEICAO": "04/10/2026",
                "DS_SIT_TOT_TURNO": "SUPLENTE",
                "QT_VOTOS_NOMINAIS_VALIDOS": 1234,
            }
        ]
    )

    paths = {}
    for name, frame in {
        "consulta_cand_2026_BRASIL.csv": main,
        "consulta_cand_complementar_2026_BRASIL.csv": complement,
        "historico_candidatura_2026_BRASIL.csv": history,
        "consulta_vagas_2026_BRASIL.csv": vacancies,
        "votacao_candidato_munzona_2026_BRASIL.csv": vote,
    }.items():
        path = tmp_path / name
        frame.to_csv(path, sep=";", index=False, encoding="latin1")
        paths[name] = str(path)

    consulta = _prepare_consulta(
        [
            paths["consulta_cand_2026_BRASIL.csv"],
            paths["consulta_cand_complementar_2026_BRASIL.csv"],
        ],
        sep=";",
        encoding="latin1",
    )
    historico = _prepare_historico(
        [paths["historico_candidatura_2026_BRASIL.csv"]],
        sep=";",
        encoding="latin1",
    )
    vagas = _prepare_vagas(
        [paths["consulta_vagas_2026_BRASIL.csv"]], sep=";", encoding="latin1"
    )
    normalized = _normalize_votacao_file(
        file_path=paths["votacao_candidato_munzona_2026_BRASIL.csv"],
        consulta_df=consulta,
        historico_df=historico,
        vagas_df=vagas,
        sep=";",
        encoding="latin1",
    )

    row = normalized.iloc[0]
    assert row["NM_UE"] == "CAMPINAS"
    assert row["DS_GENERO"] == "FEMININO"
    assert row["DS_COR_RACA"] == "PARDA"
    assert int(row["IDADE"]) == 42
    assert pd.isna(row["NR_CPF_CANDIDATO"])
    assert row["HISTORICO_CANDIDATURA_ID"] == "tse-history:2026:26001"
    assert int(row["QT_VAGAS"]) == 94
