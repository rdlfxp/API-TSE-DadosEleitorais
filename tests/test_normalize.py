import pandas as pd

from scripts.normalize import CANONICAL_COLUMNS, _finalize_normalized_frames, _normalize_votacao_file


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
