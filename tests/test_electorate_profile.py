"""Perfil do ELEITORADO ponderado pelos votos do candidato (#13)."""
from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.services.analytics import AnalyticsService
from app.services.analytics.electorate import compute_electorate_profile


def _profile_row(ano, uf, cd, zona, eleitores, masc, fem, a18_34, a35_59, a60, sup_comp, medio_comp, under18=0) -> dict:
    return {
        "ano": ano, "uf": uf, "cd_municipio": cd, "nr_zona": zona, "eleitores": eleitores,
        "masculino": masc, "feminino": fem, "genero_outros": eleitores - masc - fem,
        "idade_16_17": under18, "idade_18_34": a18_34, "idade_35_59": a35_59, "idade_60_mais": a60,
        "idade_invalida": eleitores - under18 - a18_34 - a35_59 - a60,
        "edu_analfabeto": 0, "edu_le_escreve": 0, "edu_fund_incompleto": 0, "edu_fund_completo": 0,
        "edu_medio_incompleto": 0, "edu_medio_completo": medio_comp, "edu_superior_incompleto": 0,
        "edu_superior_completo": sup_comp, "edu_nao_informado": 0,
    }


PROFILE = pd.DataFrame(
    [
        # Cidade A (71072): 100 eleitores, 40% homens, 60% mulheres, muito superior completo.
        _profile_row(2022, "SP", "71072", 1, 60, 24, 36, 18, 30, 12, 40, 20),
        _profile_row(2022, "SP", "71072", 2, 40, 16, 24, 12, 20, 8, 20, 20),
        # Cidade B (62910): 100 eleitores, 60% homens, 40% mulheres, maioria medio completo.
        _profile_row(2022, "SP", "62910", 1, 100, 60, 40, 40, 40, 20, 10, 70),
        # Perfil antigo, para o fallback de ano.
        _profile_row(2018, "SP", "71072", 1, 100, 50, 50, 25, 50, 25, 50, 50),
    ]
)


def _rows(*cities) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"ANO_ELEICAO": year, "SG_UF": "SP", "CD_MUNICIPIO": cd, "NM_UE": name, "DS_CARGO": "Presidente",
             "SQ_CANDIDATO": 1, "NM_CANDIDATO": "C", "DS_GENERO": "FEMININO", "IDADE": 40,
             "DS_GRAU_INSTRUCAO": "SUPERIOR COMPLETO", "QT_VOTOS_NOMINAIS_VALIDOS": votes}
            for year, cd, name, votes in cities
        ]
    )


def test_profile_is_the_electorate_weighted_by_votes_not_the_candidate():
    rows = _rows((2022, "71072", "SÃO PAULO", 300), (2022, "62910", "CAMPINAS", 100))
    result = compute_electorate_profile(rows, PROFILE, 2022, "QT_VOTOS_NOMINAIS_VALIDOS")
    # 75% dos votos na cidade A (40% homens) e 25% na B (60%): 0,75*40 + 0,25*60 = 45.
    assert result["gender"]["male_share"] == pytest.approx(45.0)
    assert result["gender"]["female_share"] == pytest.approx(55.0)
    assert result["age_bands"]["a18_34"] == pytest.approx(0.75 * 30 + 0.25 * 40)
    assert result["basis"] == "eleitorado" and result["electorate_year"] == 2022
    assert result["coverage"] == 100.0


def test_candidate_attributes_do_not_leak_into_the_result():
    # A "candidata" (DS_GENERO FEMININO, 40 anos, SUPERIOR) nao define o perfil: antes dava female_share=100.
    rows = _rows((2022, "62910", "CAMPINAS", 50))
    result = compute_electorate_profile(rows, PROFILE, 2022, "QT_VOTOS_NOMINAIS_VALIDOS")
    assert result["gender"]["female_share"] == pytest.approx(40.0)
    assert result["dominant_education"] == "ENSINO MÉDIO COMPLETO"  # o do eleitorado de B, nao "SUPERIOR"


def test_dominant_education_follows_the_weighted_electorate():
    only_a = compute_electorate_profile(_rows((2022, "71072", "SÃO PAULO", 10)), PROFILE, 2022, "QT_VOTOS_NOMINAIS_VALIDOS")
    assert only_a["dominant_education"] == "SUPERIOR COMPLETO"


def test_state_level_votes_use_the_uf_electorate():
    rows = pd.DataFrame(
        [{"ANO_ELEICAO": 2022, "SG_UF": "SP", "DS_CARGO": "Governador", "SQ_CANDIDATO": 1,
          "QT_VOTOS_NOMINAIS_VALIDOS": 1000}]
    )
    result = compute_electorate_profile(rows, PROFILE, 2022, "QT_VOTOS_NOMINAIS_VALIDOS")
    # Sem municipio: perfil da UF inteira em 2022 = (24+16+60)/200 homens.
    assert result["gender"]["male_share"] == pytest.approx(50.0)
    assert result["coverage"] == 100.0


def test_uses_the_latest_profile_not_after_the_election_year():
    rows = _rows((2020, "71072", "SÃO PAULO", 10))
    result = compute_electorate_profile(rows, PROFILE, 2020, "QT_VOTOS_NOMINAIS_VALIDOS")
    assert result["electorate_year"] == 2018 and result["gender"]["male_share"] == pytest.approx(50.0)
    assert compute_electorate_profile(_rows((2010, "71072", "X", 1)), PROFILE, 2010, "QT_VOTOS_NOMINAIS_VALIDOS")["basis"] == "indisponivel"


def test_unmatched_municipalities_reduce_coverage():
    rows = _rows((2022, "71072", "SÃO PAULO", 100), (2022, "99999", "SEM PERFIL", 100))
    result = compute_electorate_profile(rows, PROFILE, 2022, "QT_VOTOS_NOMINAIS_VALIDOS")
    assert result["coverage"] == 50.0 and result["gender"]["male_share"] == pytest.approx(40.0)


def test_unavailable_without_profile_or_rows():
    assert compute_electorate_profile(_rows((2022, "71072", "X", 1)), None, 2022, None)["basis"] == "indisponivel"
    assert compute_electorate_profile(_rows(), PROFILE, 2022, None)["basis"] == "indisponivel"


@pytest.fixture
def client():
    df = _rows((2022, "71072", "SÃO PAULO", 300), (2022, "62910", "CAMPINAS", 100))
    with TestClient(main_module.app) as test_client:
        service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
        service.electorate_profile_df = PROFILE
        main_module.service = service
        yield test_client
    main_module.service = None


def test_endpoint_returns_electorate_basis_and_new_fields(client: TestClient):
    payload = client.get("/v1/candidates/1/electorate-profile", params={"year": 2022, "office": "Presidente"}).json()
    assert payload["basis"] == "eleitorado" and payload["electorate_year"] == 2022
    assert payload["gender"]["male_share"] == pytest.approx(45.0)
    assert payload["age_bands"]["under_18"] == 0.0
    assert payload["dominant_income_band"] == "N/A" and payload["urban_concentration"] is None


def _no_age_profile() -> pd.DataFrame:
    # Como 2000-2006 do TSE: faixa etaria "#NE" (tudo cai em idade_invalida), mas com genero e escolaridade.
    row = _profile_row(2006, "SP", "71072", 1, 100, 48, 52, 0, 0, 0, 30, 70)
    row["idade_invalida"] = 100
    return pd.DataFrame([row])


def test_year_without_age_data_is_flagged_not_reported_as_zero_share():
    rows = _rows((2006, "71072", "SÃO PAULO", 10))
    result = compute_electorate_profile(rows, _no_age_profile(), 2006, "QT_VOTOS_NOMINAIS_VALIDOS")
    assert result["basis"] == "eleitorado"
    assert "idade" in result["unavailable"]
    assert result["gender"]["female_share"] == pytest.approx(52.0)  # o que existe continua valendo
    assert result["dominant_education"] == "ENSINO MÉDIO COMPLETO"
    assert "escolaridade" not in result["unavailable"]


def test_age_bands_are_shares_among_voters_with_known_age():
    # 100 eleitores, 20 sem idade informada: 18-34 = 40 de 80 conhecidos = 50%.
    row = _profile_row(2022, "SP", "71072", 1, 100, 50, 50, 40, 30, 10, 10, 10)
    row["idade_invalida"] = 20
    result = compute_electorate_profile(_rows((2022, "71072", "X", 1)), pd.DataFrame([row]), 2022, "QT_VOTOS_NOMINAIS_VALIDOS")
    bands = result["age_bands"]
    assert bands["a18_34"] == pytest.approx(50.0) and bands["a35_59"] == pytest.approx(37.5)
    assert sum(bands.values()) == pytest.approx(100.0)
    assert "idade" not in result["unavailable"]


def test_income_and_urban_are_always_listed_as_unavailable():
    result = compute_electorate_profile(_rows((2022, "71072", "X", 1)), PROFILE, 2022, "QT_VOTOS_NOMINAIS_VALIDOS")
    assert {"renda", "urbano"} <= set(result["unavailable"])
    assert "idade" not in result["unavailable"]


def test_unavailable_profile_lists_every_dimension():
    result = compute_electorate_profile(_rows((2022, "71072", "X", 1)), None, 2022, None)
    assert set(result["unavailable"]) == {"genero", "idade", "escolaridade", "renda", "urbano"}
