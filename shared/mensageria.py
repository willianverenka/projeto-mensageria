from __future__ import annotations

import itertools
import os
from typing import Literal, Optional

from google.protobuf.timestamp_pb2 import Timestamp

from shared.protos import contrato_pb2


_id_counter = itertools.count(1)


def novo_cabecalho(origem: str) -> contrato_pb2.Cabecalho:
    cab = contrato_pb2.Cabecalho()
    cab.id_transacao = next(_id_counter)
    cab.linguagem_origem = origem
    ts = Timestamp()
    ts.GetCurrentTime()
    cab.timestamp_envio.CopyFrom(ts)
    return cab


def envelope_bytes(env: contrato_pb2.Envelope) -> bytes:
    return env.SerializeToString()


def envelope_from_bytes(data: bytes) -> contrato_pb2.Envelope:
    env = contrato_pb2.Envelope()
    env.ParseFromString(data)
    return env


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    return value if value is not None else default


Role = Literal["cliente", "servidor", "orquestrador"]


def origem_label(role: Role) -> str:
    return f"python-{role}"

