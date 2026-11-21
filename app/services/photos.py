"""Indice das fotos oficiais hospedadas no storage proprio (candidate_photos.parquet).

O indice so diz QUAIS fotos existem (ano, uf, ue, sq); a URL e montada a partir do prefixo publico
(`PHOTOS_BASE_URL`). A API nunca le nem serve a imagem: devolve so a URL, e o app carrega do CDN com cache.
Fotos podem ser milhoes, entao a consulta e pontual no parquet (ordenado por ano/uf/sq) em vez de
carregar tudo em memoria.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from threading import Lock

import duckdb

from app.domain.photos import normalize_sq, photo_object_key

logger = logging.getLogger("app.photos")

PHOTO_INDEX_FILENAME = "candidate_photos.parquet"
MAX_LOOKUP_CANDIDATES = 50
NATIONAL_OFFICES = {"PRESIDENTE", "VICE-PRESIDENTE", "VICE PRESIDENTE"}


class CandidatePhotoIndex:
    def __init__(self, conn: duckdb.DuckDBPyConnection, base_url: str, prefix: str = "fotos") -> None:
        self._conn = conn
        self._base_url = base_url.rstrip("/")
        self._prefix = prefix
        self._lock = Lock()

    @classmethod
    def from_file(cls, path: str | Path, base_url: str | None, prefix: str = "fotos") -> "CandidatePhotoIndex | None":
        """None quando nao ha arquivo ou URL publica configurada: a API segue sem fotos antigas."""
        file_path = Path(path)
        if not base_url or not str(base_url).strip() or not file_path.exists():
            return None
        try:
            conn = duckdb.connect(":memory:")
            escaped = str(file_path).replace("'", "''")
            conn.execute(f"CREATE VIEW photos AS SELECT ano, uf, ue, sq FROM read_parquet('{escaped}')")
            conn.execute("SELECT 1 FROM photos LIMIT 1").fetchall()
        except Exception:  # noqa: BLE001 - indice opcional: arquivo corrompido nao pode derrubar a API
            logger.exception("candidate_photos_index_unavailable")
            return None
        return cls(conn, str(base_url).strip(), prefix)

    def lookup(self, year: int, uf: str, sq: object, ues: Iterable[object] = ()) -> str | None:
        """URL da foto de (ano, uf, sq). Se o SQ se repete entre municipios, escolhe pelo UE informado."""
        sq_text = normalize_sq(sq)
        uf_text = str(uf or "").strip().upper()
        if not sq_text or not uf_text:
            return None
        with self._lock:
            found = {
                str(row[0])
                for row in self._conn.execute(
                    "SELECT ue FROM photos WHERE ano = ? AND uf = ? AND sq = ?", [int(year), uf_text, sq_text]
                ).fetchall()
            }
        if not found:
            return None
        if "" in found:
            ue = ""
        else:
            wanted = {normalize_sq(u) for u in ues} - {""}
            matches = sorted(found & wanted)
            # Sem UE conhecido ou com mais de um municipio possivel o SQ nao identifica a pessoa
            # (2000-2008 repetem o SQ entre municipios): sem foto e melhor que a foto de outra pessoa.
            if len(matches) != 1:
                return None
            ue = matches[0]
        return f"{self._base_url}/{photo_object_key(year, uf_text, ue, sq_text, self._prefix)}"

    def lookup_rows(self, year: int, rows: list[tuple[object, object, object, object]]) -> str | None:
        """Primeira foto achada entre as linhas (uf, sq, cd_municipio, cargo) do candidato naquele ano.

        Presidente e vice vem na base com a UF de cada estado, mas a foto fica no pacote `BR`:
        so para esses cargos a consulta tenta `BR` (um SQ pequeno de 2006 nao pode pegar a foto de outro cargo).
        """
        ues_by_key: dict[tuple[str, str], list[object]] = {}
        for uf, sq, ue, cargo in rows[: MAX_LOOKUP_CANDIDATES * 20]:
            sq_text = normalize_sq(sq)
            if str(cargo or "").strip().upper() in NATIONAL_OFFICES:
                ues_by_key.setdefault(("BR", sq_text), []).append(ue)
            ues_by_key.setdefault((str(uf or "").strip().upper(), sq_text), []).append(ue)
        keys = sorted(ues_by_key, key=lambda key: key[0] != "BR")  # BR primeiro: so entra para cargo nacional
        for uf, sq in keys[:MAX_LOOKUP_CANDIDATES]:
            url = self.lookup(year, uf, sq, ues_by_key[(uf, sq)])
            if url:
                return url
        return None

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:  # noqa: BLE001
            pass
