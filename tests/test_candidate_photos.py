"""Fotos oficiais 2006-2022 (#9): nome do arquivo, conversao, indice e foto_url no summary."""
from __future__ import annotations

import io
import zipfile

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.domain.photos import normalize_sq, parse_photo_filename, photo_object_key
from app.infra import cache as cache_module
from app.services.analytics.duckdb_service import DuckDBAnalyticsService
from app.services.analytics_service import AnalyticsService
from app.services.photos import CandidatePhotoIndex

BASE_URL = "https://fotos.example.com"


# ---------------------------------------------------------------- nome do arquivo
@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("FBR280001600166_div.jpeg", ("BR", "", "280001600166")),  # 2018/2022
        ("FRR230002529954_div.jpg", ("RR", "", "230002529954")),
        ("BR280000000015_div.jpg", ("BR", "", "280000000015")),  # 2010/2014: sem o F
        ("RR230000000069_div.jpg", ("RR", "", "230000000069")),
        ("FBR1_div.png", ("BR", "", "1")),  # 2006
        ("FRR1647_div.png", ("RR", "", "1647")),  # 2008
        ("FMG53953_00070_div.png", ("MG", "53953", "70")),  # 2004: UE + SQ
        ("fotos/FSP100_00005_div.PNG", ("SP", "100", "5")),
    ],
)
def test_parse_photo_filename(name, expected):
    assert tuple(parse_photo_filename(name)) == expected


@pytest.mark.parametrize("name", ["leiame.pdf", "FXX123_div.jpg", "FRR_div.jpg", "FRR12_foo.jpg", ""])
def test_parse_photo_filename_ignores_other_files(name):
    assert parse_photo_filename(name) is None


def test_object_key_is_lowercase_and_scoped():
    assert photo_object_key(2022, "RR", "", "00070") == "fotos/2022/rr/70.jpg"
    assert photo_object_key(2008, "SP", "00100", "5") == "fotos/2008/sp/100/5.jpg"
    assert photo_object_key(2022, "BR", "", "7", prefix="/img/") == "img/2022/br/7.jpg"
    assert normalize_sq("00070") == "70" and normalize_sq(70.0) == "70" and normalize_sq("70.0") == "70" and normalize_sq(None) == ""


# ---------------------------------------------------------------- script de conversao
def _image_bytes(fmt: str, size=(800, 600), mode="RGB") -> bytes:
    from PIL import Image

    out = io.BytesIO()
    Image.new(mode, size, (200, 30, 30) if mode == "RGB" else (200, 30, 30, 120)).save(out, format=fmt)
    return out.getvalue()


def test_process_zip_converts_resizes_and_indexes(tmp_path):
    pytest.importorskip("PIL")
    from PIL import Image

    from scripts import build_candidate_photos as builder

    zip_path = tmp_path / "foto_cand2022_RR_div.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("leiame.pdf", b"%PDF")
        archive.writestr("FRR230000000001_div.jpg", _image_bytes("JPEG"))  # grande: reduz
        archive.writestr("FRR230000000002_div.png", _image_bytes("PNG", (100, 100), "RGBA"))  # pequena com alfa: vira JPEG
        archive.writestr("FRR230000000003_div.jpg", b"not an image")  # corrompida
        archive.writestr("FRR230000000001_div.jpeg", _image_bytes("JPEG"))  # mesma chave: duplicada
        archive.writestr("alguma-coisa.txt", b"x")

    result = builder.process_zip(zip_path, 2022, tmp_path / "out", max_side=360, quality=80)
    stats, frame = result["stats"], result["index"]

    assert sorted(frame["sq"]) == ["230000000001", "230000000002"]
    assert set(frame["uf"]) == {"RR"} and set(frame["ue"]) == {""} and set(frame["ano"]) == {2022}
    assert stats["photos"] == 2 and stats["failed"] == 1 and stats["duplicates"] == 1 and stats["unmatched"] == 1
    big = Image.open(tmp_path / "out" / "fotos" / "2022" / "rr" / "230000000001.jpg")
    assert max(big.size) == 360 and big.format == "JPEG"
    small = Image.open(tmp_path / "out" / "fotos" / "2022" / "rr" / "230000000002.jpg")
    assert small.size == (100, 100) and small.mode == "RGB"  # nunca amplia


def test_process_image_keeps_small_jpeg_untouched():
    pytest.importorskip("PIL")
    from scripts import build_candidate_photos as builder

    from PIL import Image

    out = io.BytesIO()
    Image.new("RGB", (50, 50), (10, 120, 200)).save(out, format="JPEG", quality=20, optimize=True)
    original = out.getvalue()  # ja pequeno e bem comprimido: recomprimir a 80 so engordaria
    assert builder.process_image(original, max_side=360, quality=80) == original


def test_process_zip_keeps_the_municipality_when_the_name_has_it(tmp_path):
    pytest.importorskip("PIL")
    from scripts import build_candidate_photos as builder

    zip_path = tmp_path / "foto_cand2004_MG_div.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("FMG53953_00070_div.png", _image_bytes("PNG", (60, 60)))
        archive.writestr("FMG41670_00070_div.png", _image_bytes("PNG", (60, 60)))  # mesmo SQ, outro municipio
    result = builder.process_zip(zip_path, 2004, tmp_path / "out", max_side=360, quality=80)
    assert sorted(zip(result["index"]["ue"], result["index"]["sq"])) == [("41670", "70"), ("53953", "70")]
    assert (tmp_path / "out" / "fotos" / "2004" / "mg" / "53953" / "70.jpg").exists()
    assert result["stats"]["with_ue"] == 2


def test_resumable_download_resumes_and_flags_missing(tmp_path, monkeypatch):
    from scripts import build_candidate_photos as builder

    import urllib.error

    def fake_urlopen(request, timeout=0):
        raise urllib.error.HTTPError(request.full_url, 404, "nf", {}, None)

    monkeypatch.setattr(builder.urllib.request, "urlopen", fake_urlopen)
    assert builder._download("https://x/y.zip", tmp_path / "y.zip") is False
    assert not (tmp_path / "y.zip").exists()


def test_merge_index_filters_by_upload_marker(tmp_path):
    pytest.importorskip("pyarrow")
    from scripts import build_candidate_photos as builder

    (tmp_path / "_index").mkdir()
    for year, uf in ((2022, "RR"), (2022, "AC")):
        pd.DataFrame({"ano": [year], "uf": [uf], "ue": [""], "sq": ["1"]}).astype({"ano": "int16"}).to_parquet(
            builder._part_path(tmp_path, year, uf), index=False
        )
    builder._uploaded_marker(tmp_path, 2022, "RR").touch()
    assert len(builder.merge_index(tmp_path, only_uploaded=False)) == 2
    only = builder.merge_index(tmp_path, only_uploaded=True)
    assert list(only["uf"]) == ["RR"]


# ---------------------------------------------------------------- indice e API
def _write_index(path, rows) -> None:
    frame = pd.DataFrame(rows, columns=["ano", "uf", "ue", "sq"]).astype({"ano": "int16"})
    conn = duckdb.connect()
    conn.register("frame", frame)
    conn.execute(f"COPY frame TO '{path}' (FORMAT PARQUET)")
    conn.close()


INDEX_ROWS = [
    (2022, "BR", "", "5"),  # Presidente: pacote BR
    (2022, "SP", "", "6"),  # Deputado Federal
    (2008, "SP", "100", "7"),  # vereador: SQ repete entre municipios, a foto traz o UE
    (2008, "SP", "200", "7"),
]


def test_index_lookup_scopes_by_municipality_and_ignores_missing(tmp_path):
    path = tmp_path / "candidate_photos.parquet"
    _write_index(path, INDEX_ROWS)
    index = CandidatePhotoIndex.from_file(path, BASE_URL)
    assert index is not None
    assert index.lookup(2022, "SP", "6") == f"{BASE_URL}/fotos/2022/sp/6.jpg"
    assert index.lookup(2008, "SP", 7, ues=["200"]) == f"{BASE_URL}/fotos/2008/sp/200/7.jpg"
    assert index.lookup(2008, "SP", 7, ues=["999"]) is None  # municipio sem foto: nunca a de outro
    assert index.lookup(2008, "SP", 7) is None
    assert index.lookup(2022, "SP", 99) is None and index.lookup(2022, "SP", "") is None
    # BR so entra para cargo nacional (um SQ pequeno nao pode pegar a foto de outro cargo).
    assert index.lookup_rows(2022, [("AC", 5, "", "Presidente"), ("SP", 5, "", "Presidente")]) == f"{BASE_URL}/fotos/2022/br/5.jpg"
    assert index.lookup_rows(2022, [("SP", 5, "", "Deputado Federal")]) is None


def test_index_is_disabled_without_file_or_base_url(tmp_path):
    path = tmp_path / "candidate_photos.parquet"
    assert CandidatePhotoIndex.from_file(path, BASE_URL) is None  # sem arquivo
    _write_index(path, INDEX_ROWS)
    assert CandidatePhotoIndex.from_file(path, None) is None and CandidatePhotoIndex.from_file(path, "  ") is None
    path.write_bytes(b"corrompido")
    assert CandidatePhotoIndex.from_file(path, BASE_URL) is None  # arquivo ruim nao derruba a API


def _rows() -> list[dict]:
    base = {"DS_GENERO": "MASCULINO", "IDADE": 50, "NR_TURNO": 1, "DS_SIT_TOT_TURNO": "NÃO ELEITO", "QT_VOTOS_NOMINAIS_VALIDOS": 10}
    rows = []
    for uf in ("AC", "SP"):  # Presidente vem com a UF de cada estado
        rows.append({**base, "ANO_ELEICAO": 2022, "DS_CARGO": "Presidente", "SG_UF": uf, "SQ_CANDIDATO": 5, "NM_CANDIDATO": "PRES"})
    rows.append({**base, "ANO_ELEICAO": 2022, "DS_CARGO": "Deputado Federal", "SG_UF": "SP", "SQ_CANDIDATO": 6, "NM_CANDIDATO": "DEP"})
    rows.append({**base, "ANO_ELEICAO": 2008, "DS_CARGO": "Vereador", "SG_UF": "SP", "CD_MUNICIPIO": "200", "NM_UE": "CIDADE", "SQ_CANDIDATO": 7, "NM_CANDIDATO": "VER"})
    rows.append({**base, "ANO_ELEICAO": 2022, "DS_CARGO": "Deputado Federal", "SG_UF": "SP", "SQ_CANDIDATO": 8, "NM_CANDIDATO": "SEM FOTO"})
    return rows


@pytest.fixture(params=["pandas", "duckdb"])
def client(request, tmp_path, monkeypatch):
    cache_module.ANALYTICS_CACHE.clear()
    monkeypatch.setattr("app.services.analytics.duckdb_service.settings.photos_base_url", BASE_URL)
    monkeypatch.setattr("app.services.analytics.pandas_service.settings.photos_base_url", BASE_URL)
    df = pd.DataFrame(_rows())
    index_path = tmp_path / "candidate_photos.parquet"
    _write_index(index_path, INDEX_ROWS)
    if request.param == "pandas":
        service = AnalyticsService(dataframe=df, default_top_n=20, max_top_n=100)
        service.photo_index = CandidatePhotoIndex.from_file(index_path, BASE_URL)
    else:
        path = tmp_path / "analytics.parquet"
        conn = duckdb.connect()
        conn.register("frame", df)
        conn.execute(f"COPY frame TO '{path}' (FORMAT PARQUET)")
        conn.close()
        service = DuckDBAnalyticsService.from_file(str(path), default_top_n=20, max_top_n=100, database_path=str(tmp_path / "db.duckdb"))
        assert service.photo_index is not None  # carregado do candidate_photos.parquet ao lado
    with TestClient(main_module.app) as test_client:
        main_module.service = service
        yield test_client
    main_module.service = None
    cache_module.ANALYTICS_CACHE.clear()


def test_summary_returns_hosted_photo_for_old_years(client):
    president = client.get("/v1/candidates/5/summary", params={"year": 2022}).json()
    assert president["foto_url"] == f"{BASE_URL}/fotos/2022/br/5.jpg"
    deputy = client.get("/v1/candidates/6/summary", params={"year": 2022}).json()
    assert deputy["foto_url"] == f"{BASE_URL}/fotos/2022/sp/6.jpg"


def test_summary_without_photo_is_null_not_a_broken_url(client):
    assert client.get("/v1/candidates/8/summary", params={"year": 2022}).json()["foto_url"] is None
    # 2008: so existe foto do vereador no municipio 100 e 200; a linha do candidato e do 200.
    assert client.get("/v1/candidates/7/summary", params={"year": 2008}).json()["foto_url"] == f"{BASE_URL}/fotos/2008/sp/200/7.jpg"
