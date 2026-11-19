"""Gerador do perfil do eleitorado: agregacao por zona e downloads robustos (#13)."""
from __future__ import annotations

import io
import zipfile

import pandas as pd
import pytest

from scripts import build_electorate_profile as builder


def _raw(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_aggregate_splits_gender_age_and_education_per_zone():
    raw = _raw(
        [
            {"SG_UF": "SP", "CD_MUNICIPIO": "71072", "NR_ZONA": "1", "DS_GENERO": "FEMININO",
             "DS_FAIXA_ETARIA": "18 anos", "DS_GRAU_ESCOLARIDADE": "SUPERIOR COMPLETO", "QT_ELEITORES": "10"},
            {"SG_UF": "SP", "CD_MUNICIPIO": "71072", "NR_ZONA": "1", "DS_GENERO": "MASCULINO",
             "DS_FAIXA_ETARIA": "21 a 24 anos", "DS_GRAU_ESCOLARIDADE": "LÊ E ESCREVE", "QT_ELEITORES": "20"},
            {"SG_UF": "SP", "CD_MUNICIPIO": "71072", "NR_ZONA": "1", "DS_GENERO": "NÃO INFORMADO",
             "DS_FAIXA_ETARIA": "100 anos ou mais", "DS_GRAU_ESCOLARIDADE": "NÃO INFORMADO", "QT_ELEITORES": "1"},
            {"SG_UF": "SP", "CD_MUNICIPIO": "71072", "NR_ZONA": "2", "DS_GENERO": "FEMININO",
             "DS_FAIXA_ETARIA": "17 anos", "DS_GRAU_ESCOLARIDADE": "ENSINO MÉDIO COMPLETO", "QT_ELEITORES": "5"},
        ]
    )
    out = builder.aggregate_frame(raw, 2022).set_index("nr_zona")
    assert out.loc[1, "eleitores"] == 31 and out.loc[1, "feminino"] == 10 and out.loc[1, "masculino"] == 20
    assert out.loc[1, "genero_outros"] == 1
    assert out.loc[1, "idade_18_34"] == 30 and out.loc[1, "idade_60_mais"] == 1
    assert out.loc[1, "edu_superior_completo"] == 10 and out.loc[1, "edu_le_escreve"] == 20
    assert out.loc[1, "edu_nao_informado"] == 1
    assert out.loc[2, "idade_16_17"] == 5 and out.loc[2, "cd_municipio"] == "71072"


def test_age_label_without_digits_is_invalid_not_dropped():
    # 2000-2006: "#NE" em toda a base; os eleitores continuam contados (so a idade e desconhecida).
    raw = _raw(
        [{"SG_UF": "SP", "CD_MUNICIPIO": "71072", "NR_ZONA": "1", "DS_GENERO": "FEMININO",
          "DS_FAIXA_ETARIA": "#NE", "DS_GRAU_ESCOLARIDADE": "ANALFABETO", "QT_ELEITORES_PERFIL": "7"}]
    )
    out = builder.aggregate_frame(raw, 2004)
    assert out.loc[0, "eleitores"] == 7 and out.loc[0, "idade_invalida"] == 7
    assert out.loc[0, "idade_18_34"] == 0


def _zip_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("x.csv", "a;b\n1;2\n")
    return buffer.getvalue()


def test_partial_zip_in_cache_is_discarded_and_downloaded_again(tmp_path, monkeypatch):
    (tmp_path / "perfil_eleitorado_2012.zip").write_bytes(b"PK\x03\x04 truncado")
    calls = []
    monkeypatch.setattr(builder, "_download", lambda url, dest, **kw: (calls.append(url), dest.write_bytes(_zip_bytes())))
    path = builder.ensure_zip(2012, tmp_path)
    assert calls and zipfile.is_zipfile(path)


def test_valid_cached_zip_is_reused_without_network(tmp_path, monkeypatch):
    (tmp_path / "perfil_eleitorado_2022.zip").write_bytes(_zip_bytes())
    monkeypatch.setattr(builder, "_download", lambda *a, **k: pytest.fail("nao deve baixar"))
    assert builder.ensure_zip(2022, tmp_path).exists()


def test_download_retries_and_never_leaves_a_partial_file(tmp_path, monkeypatch):
    attempts = {"n": 0}

    class FakeResponse(io.BytesIO):
        headers = {"Content-Length": str(len(_zip_bytes()))}
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def fake_urlopen(url, timeout=0):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise TimeoutError("lento")
        return FakeResponse(_zip_bytes())

    monkeypatch.setattr(builder.urllib.request, "urlopen", fake_urlopen)
    destination = tmp_path / "perfil_eleitorado_2014.zip"
    builder._download("http://x/y.zip", destination)
    assert attempts["n"] == 3 and zipfile.is_zipfile(destination)
    assert not list(tmp_path.glob("*.part"))


def test_download_gives_up_after_the_attempt_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(builder.urllib.request, "urlopen", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("x")))
    with pytest.raises(SystemExit):
        builder._download("http://x/y.zip", tmp_path / "a.zip", attempts=2)
    assert not (tmp_path / "a.zip").exists()
