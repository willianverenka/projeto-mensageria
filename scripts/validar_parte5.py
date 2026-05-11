from __future__ import annotations

import filecmp
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SERVERS = ["servidor_python", "servidor_java", "servidor_csharp"]
FILES = ["logins.jsonl", "canais.json", "publicacoes.jsonl"]


def run(cmd: list[str], *, env: dict[str, str] | None = None) -> None:
    print("+", " ".join(cmd))
    subprocess.run(cmd, cwd=ROOT, check=True, env=env)


def normalized_contents(path: Path) -> object:
    if path.name == "canais.json":
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"arquivo JSON invalido em {path}: {exc}") from exc
        if not isinstance(data, list):
            raise RuntimeError(f"esperado array JSON em {path}")
        return sorted(data)

    entries: list[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"arquivo JSONL invalido em {path}: {exc}") from exc
            entries.append(json.dumps(obj, sort_keys=True, ensure_ascii=False))
    return sorted(entries)


def main() -> int:
    data_root = Path(tempfile.mkdtemp(prefix="projeto-mensageria-parte5-", dir="/private/tmp"))
    env = os.environ.copy()
    env["DATA_ROOT"] = str(data_root)

    try:
        run(["docker", "compose", "down", "--remove-orphans"], env=env)
        run(["docker", "compose", "up", "-d", "--build"], env=env)
        time.sleep(20)

        run(["docker", "compose", "exec", "-T", "orquestrador", "python", "scripts/parte5_driver.py", "--phase", "nominal"], env=env)

        run(["docker", "compose", "kill", "-s", "SIGKILL", "servidor_go"], env=env)
        time.sleep(5)

        run(["docker", "compose", "exec", "-T", "orquestrador", "python", "scripts/parte5_driver.py", "--phase", "failover"], env=env)

        run(["docker", "compose", "down"], env=env)

        reference = data_root / "servidor_python"
        for server in SERVERS[1:]:
            current = data_root / server
            for filename in FILES:
                left = reference / filename
                right = current / filename
                if not left.exists() or not right.exists():
                    raise FileNotFoundError(f"arquivo ausente ao comparar: {left} vs {right}")
                if normalized_contents(left) != normalized_contents(right):
                    raise RuntimeError(f"conteudo divergente em {filename}: {left} vs {right}")

        print(f"validacao concluida em {data_root}")
        return 0
    finally:
        try:
            run(["docker", "compose", "down", "--remove-orphans"], env=env)
        except Exception:
            pass
        shutil.rmtree(data_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
