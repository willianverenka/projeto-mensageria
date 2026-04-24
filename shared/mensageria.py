from __future__ import annotations

import itertools
import os
import threading
import time
from typing import Literal, Optional

from google.protobuf.timestamp_pb2 import Timestamp

from shared.protos import contrato_pb2


_id_counter = itertools.count(1)
_id_lock = threading.Lock()


def _next_id() -> int:
    with _id_lock:
        return next(_id_counter)


def timestamp_from_ns(ns: int) -> Timestamp:
    ts = Timestamp()
    ts.seconds = ns // 1_000_000_000
    ts.nanos = ns % 1_000_000_000
    return ts


def timestamp_to_ns(ts: Timestamp) -> int:
    return ts.seconds * 1_000_000_000 + ts.nanos


def timestamp_to_text(ts: Timestamp | None) -> str:
    if ts is None:
        return "0.000000000"
    return f"{ts.seconds}.{ts.nanos:09d}"


def cabecalho_texto(cab: contrato_pb2.Cabecalho | None) -> str:
    if cab is None:
        return "tx=0 origem=desconhecida ts=0.000000000 relogio=0"
    return (
        f"tx={cab.id_transacao} "
        f"origem={cab.linguagem_origem or 'desconhecida'} "
        f"ts={timestamp_to_text(cab.timestamp_envio)} "
        f"relogio={cab.relogio_logico}"
    )


class RelogioProcesso:
    def __init__(self) -> None:
        self._logical = 0
        self._offset_ns = 0
        self._lock = threading.Lock()

    def before_send(self) -> int:
        with self._lock:
            self._logical += 1
            return self._logical

    def on_receive(self, recebido: int) -> int:
        with self._lock:
            self._logical = max(self._logical, recebido)
            return self._logical

    def valor(self) -> int:
        with self._lock:
            return self._logical

    def now_corrigido_ns(self) -> int:
        with self._lock:
            offset_ns = self._offset_ns
        return time.time_ns() + offset_ns

    def now_corrigido_timestamp(self) -> Timestamp:
        return timestamp_from_ns(self.now_corrigido_ns())

    def atualizar_offset(
        self,
        referencia_timestamp: Timestamp,
        instante_envio_local_ns: int,
        instante_recebimento_local_ns: int,
    ) -> int:
        ponto_medio_local_ns = (instante_envio_local_ns + instante_recebimento_local_ns) // 2
        referencia_ns = timestamp_to_ns(referencia_timestamp)
        with self._lock:
            self._offset_ns = referencia_ns - ponto_medio_local_ns
            return self._offset_ns

    def offset_ms(self) -> float:
        with self._lock:
            return self._offset_ns / 1_000_000.0


def novo_cabecalho(
    origem: str, relogio: Optional[RelogioProcesso] = None
) -> contrato_pb2.Cabecalho:
    cab = contrato_pb2.Cabecalho()
    cab.id_transacao = _next_id()
    cab.linguagem_origem = origem
    if relogio is None:
        cab.timestamp_envio.CopyFrom(timestamp_from_ns(time.time_ns()))
        cab.relogio_logico = 0
    else:
        cab.timestamp_envio.CopyFrom(relogio.now_corrigido_timestamp())
        cab.relogio_logico = relogio.before_send()
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


Role = Literal["cliente", "servidor", "orquestrador", "referencia"]


def origem_label(role: Role) -> str:
    return f"python-{role}"
