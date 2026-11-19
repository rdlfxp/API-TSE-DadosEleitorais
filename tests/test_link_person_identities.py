from __future__ import annotations

import duckdb
import pandas as pd

from app.services.analytics.duckdb_service import DuckDBAnalyticsService
from scripts import link_person_identities

OFFICIAL = "tse-history:2026:280002542548"


def _row(year: int, sq: int, hid: str, name: str, dob: str | None, cpf: str | None, votes: int = 1000, **extra) -> dict:
    return {
        "ANO_ELEICAO": year,
        "NR_TURNO": 1,
        "SG_UF": "SP",
        "DS_CARGO": "Presidente",
        "SQ_CANDIDATO": sq,
        "NM_CANDIDATO": name,
        "DT_NASCIMENTO": dob,
        "NR_CPF_CANDIDATO": cpf,
        "HISTORICO_CANDIDATURA_ID": hid,
        "QT_VOTOS_NOMINAIS_VALIDOS": votes,
        **extra,
    }


def _write(df: pd.DataFrame, path) -> None:
    conn = duckdb.connect()
    try:
        conn.register("frame", df)
        conn.execute(f"COPY frame TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
    finally:
        conn.close()


def _link(tmp_path, rows: list[dict]) -> dict[tuple[int, int], str]:
    source, destination = tmp_path / "in.parquet", tmp_path / "out.parquet"
    _write(pd.DataFrame(rows), source)
    conn = duckdb.connect()
    try:
        link_person_identities.link_legacy_identities(conn, source, destination)
        out = conn.execute(
            "SELECT ANO_ELEICAO, SQ_CANDIDATO, HISTORICO_CANDIDATURA_ID FROM read_parquet(?)",
            [str(destination)],
        ).fetchall()
    finally:
        conn.close()
    return {(int(y), int(sq)): hid for y, sq, hid in out}


def test_links_legacy_row_by_cpf_even_when_birth_format_differs(tmp_path):
    result = _link(
        tmp_path,
        [
            _row(2002, 9574, "tse-candidate:2002:9574", "LUIZ INACIO LULA DA SILVA", "06/10/1945", "7068093868"),
            _row(2026, 280002542548, OFFICIAL, "LUIZ INÁCIO LULA DA SILVA", "1945-10-06", "07068093868"),
        ],
    )
    assert result[(2002, 9574)] == OFFICIAL
    assert result[(2026, 280002542548)] == OFFICIAL


def test_links_by_name_and_birth_when_cpf_is_missing(tmp_path):
    result = _link(
        tmp_path,
        [
            _row(2002, 9574, "tse-candidate:2002:9574", "LUIZ INACIO LULA DA SILVA", "06/10/1945", None),
            _row(2026, 280002542548, OFFICIAL, "LUIZ INÁCIO  LULA DA SILVA", "1945-10-06", None),
        ],
    )
    assert result[(2002, 9574)] == OFFICIAL


def test_masked_cpf_never_links_different_people(tmp_path):
    result = _link(
        tmp_path,
        [
            _row(2024, 1, "tse-candidate:2024:1", "MARIA A", "01/01/1980", "-4"),
            _row(2024, 2, "tse-candidate:2024:2", "JOAO B", "02/02/1970", "-4"),
            _row(2026, 3, "tse-history:2026:3", "CARLOS C", "03/03/1960", "4"),
        ],
    )
    assert result[(2024, 1)] == "tse-candidate:2024:1"
    assert result[(2024, 2)] == "tse-candidate:2024:2"


def test_same_name_without_matching_birth_or_cpf_is_not_linked(tmp_path):
    result = _link(
        tmp_path,
        [
            _row(2002, 11, "tse-candidate:2002:11", "JOSE DA SILVA", "01/01/1950", None),
            _row(2026, 12, "tse-history:2026:12", "JOSE DA SILVA", "09/09/1990", None),
        ],
    )
    assert result[(2002, 11)] == "tse-candidate:2002:11"


def test_ambiguous_official_ids_are_left_alone(tmp_path):
    result = _link(
        tmp_path,
        [
            _row(2002, 21, "tse-candidate:2002:21", "ANA LIMA", "05/05/1975", None),
            _row(2026, 22, "tse-history:2026:22", "ANA LIMA", "1975-05-05", None),
            _row(2026, 23, "tse-history:2026:23", "ANA LIMA", "1975-05-05", None),
        ],
    )
    assert result[(2002, 21)] == "tse-candidate:2002:21"


def test_cpf_alone_does_not_link_without_matching_name(tmp_path):
    result = _link(
        tmp_path,
        [
            _row(2002, 31, "tse-candidate:2002:31", "FULANO X", "01/01/1950", "12345678909"),
            _row(2026, 32, "tse-history:2026:32", "BELTRANO Y", "1950-01-01", "12345678909"),
        ],
    )
    assert result[(2002, 31)] == "tse-candidate:2002:31"


def test_row_carrying_another_persons_cpf_and_birth_is_not_linked(tmp_path):
    # Reproduz a corrupcao de 2008: nome de Jose, mas CPF e nascimento da Zeni.
    result = _link(
        tmp_path,
        [
            _row(2008, 51816, "tse-candidate:2008:51816", "JOSE CANDIDO MARTINS DE ARAUJO", "19/04/1954", "24947920610"),
            _row(2026, 130002540613, "tse-history:2026:130002540613", "ZENI FRANCISCA DA SILVA", "1954-04-19", "24947920610"),
        ],
    )
    assert result[(2008, 51816)] == "tse-candidate:2008:51816"


def test_name_and_cpf_link_even_when_birth_is_missing(tmp_path):
    result = _link(
        tmp_path,
        [
            _row(2002, 41, "tse-candidate:2002:41", "ANA LIMA", None, "12345678909"),
            _row(2026, 42, "tse-history:2026:42", "ANA LIMA", "1975-05-05", "12345678909"),
        ],
    )
    assert result[(2002, 41)] == "tse-history:2026:42"


def test_vote_history_includes_2002_without_cpf_after_linking(tmp_path):
    source, destination = tmp_path / "analytics.parquet", tmp_path / "linked" / "analytics.parquet"
    destination.parent.mkdir()
    _write(
        pd.DataFrame(
            [
                _row(2002, 9574, "tse-candidate:2002:9574", "LUIZ INACIO LULA DA SILVA", "06/10/1945", None),
                _row(2006, 23, OFFICIAL, "LUIZ INACIO LULA DA SILVA", "06/10/1945", None),
                _row(2026, 280002542548, OFFICIAL, "LUIZ INÁCIO LULA DA SILVA", "1945-10-06", None),
            ]
        ),
        source,
    )

    def years(path) -> tuple[list[int], set[str | None]]:
        service = DuckDBAnalyticsService.from_file(str(path), default_top_n=20, max_top_n=100)
        try:
            history = service.candidate_vote_history("280002542548")
        finally:
            service.close()
        return [item["year"] for item in history["items"]], {item["person_id"] for item in history["items"]}

    before_years, _ = years(source)
    assert 2002 not in before_years  # reproduz o bug: sem CPF, 2002 fica de fora

    conn = duckdb.connect()
    try:
        link_person_identities.link_legacy_identities(conn, source, destination)
    finally:
        conn.close()
    after_years, person_ids = years(destination)
    assert after_years == [2026, 2006, 2002]
    assert len(person_ids) == 1
