"""
Camada de ingestão (sistemas de origem → Bronze).

Regra da Bronze: guardar o dado **como chegou**, particionado pela data de
referência, com metadados de rastreabilidade. Cada objeto gravado é registrado
num manifesto (fonte, caminho, linhas, bytes e hash SHA-256), usado depois na
reconciliação Bronze × Silver.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

from banco360.config import Config


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for bloco in iter(lambda: f.read(1 << 20), b""):
            h.update(bloco)
    return h.hexdigest()


def registrar_manifesto(cfg: Config, d: date, fonte: str, caminho: Path, linhas: int) -> dict:
    pasta = cfg.camada("_manifestos") / f"dt={d.isoformat()}"
    pasta.mkdir(parents=True, exist_ok=True)
    arquivo = pasta / "ingestao.json"
    itens = json.loads(arquivo.read_text(encoding="utf-8")) if arquivo.exists() else []
    item = {
        "fonte": fonte,
        "objeto": caminho.relative_to(cfg.lake_dir).as_posix(),
        "linhas": linhas,
        "bytes": caminho.stat().st_size,
        "sha256": sha256(caminho),
        "ingerido_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    itens = [i for i in itens if i["fonte"] != fonte] + [item]
    arquivo.write_text(json.dumps(itens, indent=2, ensure_ascii=False), encoding="utf-8")
    return item


def ler_manifesto(cfg: Config, d: date) -> dict[str, dict]:
    arquivo = cfg.camada("_manifestos") / f"dt={d.isoformat()}" / "ingestao.json"
    if not arquivo.exists():
        return {}
    return {i["fonte"]: i for i in json.loads(arquivo.read_text(encoding="utf-8"))}
