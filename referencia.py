import logging
import os
import time
from dataclasses import dataclass

import zmq

from shared.mensageria import (
    RelogioProcesso,
    cabecalho_texto,
    envelope_bytes,
    envelope_from_bytes,
    novo_cabecalho,
    origem_label,
)
from shared.protos import contrato_pb2


logging.basicConfig(level=logging.INFO, format="%(asctime)s [REFERENCIA] %(message)s")


REFERENCE_BIND = os.getenv("REFERENCE_BIND", "tcp://*:5559")
HEARTBEAT_TIMEOUT_SECONDS = int(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "30"))


@dataclass
class RegistroServidor:
    nome: str
    rank: int
    last_seen_monotonic: float


class Referencia:
    def __init__(self) -> None:
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(REFERENCE_BIND)
        self.relogio = RelogioProcesso()
        self.origem = origem_label("referencia")
        self.next_rank = 1
        self.ranks_por_nome: dict[str, int] = {}
        self.ativos: dict[str, RegistroServidor] = {}
        logging.info("Serviço de referência ouvindo em %s", REFERENCE_BIND)

    def _remover_expirados(self) -> None:
        agora = time.monotonic()
        expirados = [
            nome
            for nome, registro in self.ativos.items()
            if agora - registro.last_seen_monotonic > HEARTBEAT_TIMEOUT_SECONDS
        ]
        for nome in expirados:
            logging.info("Removendo servidor expirado da lista ativa: %s", nome)
            del self.ativos[nome]

    def _registrar_servidor(self, nome: str) -> int:
        agora = time.monotonic()
        existente = self.ativos.get(nome)
        if existente is not None:
            existente.last_seen_monotonic = agora
            return existente.rank

        rank = self.ranks_por_nome.get(nome)
        if rank is None:
            rank = self.next_rank
            self.next_rank += 1
            self.ranks_por_nome[nome] = rank

        self.ativos[nome] = RegistroServidor(nome=nome, rank=rank, last_seen_monotonic=agora)
        return rank

    def _resposta_vazia(self) -> contrato_pb2.Cabecalho:
        return novo_cabecalho(self.origem, self.relogio)

    def loop(self) -> None:
        while True:
            data = self.socket.recv()
            env = envelope_from_bytes(data)
            self.relogio.on_receive(env.cabecalho.relogio_logico)
            self._remover_expirados()
            tipo = env.WhichOneof("conteudo")
            logging.info("Recebido %s %s", tipo, cabecalho_texto(env.cabecalho))

            resposta_env = contrato_pb2.Envelope()
            cab = self._resposta_vazia()
            if tipo == "heartbeat_req":
                cab.timestamp_envio.seconds = 0
                cab.timestamp_envio.nanos = 0
            resposta_env.cabecalho.CopyFrom(cab)

            if tipo == "register_server_req":
                resposta = self._processar_register(env.register_server_req)
                resposta.cabecalho.CopyFrom(cab)
                resposta_env.register_server_res.CopyFrom(resposta)
            elif tipo == "list_servers_req":
                resposta = self._processar_list()
                resposta.cabecalho.CopyFrom(cab)
                resposta_env.list_servers_res.CopyFrom(resposta)
            elif tipo == "heartbeat_req":
                resposta = self._processar_heartbeat(env.heartbeat_req)
                resposta.cabecalho.CopyFrom(cab)
                resposta_env.heartbeat_res.CopyFrom(resposta)
            else:
                resposta = contrato_pb2.HeartbeatResponse()
                resposta.cabecalho.CopyFrom(cab)
                resposta.status = contrato_pb2.STATUS_ERRO
                resposta.erro_msg = f"tipo não suportado: {tipo}"
                resposta_env.heartbeat_res.CopyFrom(resposta)

            logging.info(
                "Enviando %s %s",
                resposta_env.WhichOneof("conteudo") or "desconhecido",
                cabecalho_texto(resposta_env.cabecalho),
            )
            self.socket.send(envelope_bytes(resposta_env))

    def _processar_register(
        self, req: contrato_pb2.RegisterServerRequest
    ) -> contrato_pb2.RegisterServerResponse:
        resposta = contrato_pb2.RegisterServerResponse()
        nome = req.nome_servidor.strip()
        if not nome:
            resposta.status = contrato_pb2.STATUS_ERRO
            resposta.erro_msg = "nome de servidor vazio"
            return resposta

        rank = self._registrar_servidor(nome)
        resposta.status = contrato_pb2.STATUS_SUCESSO
        resposta.rank = rank
        logging.info("Servidor registrado/renovado: %s rank=%s", nome, rank)
        return resposta

    def _processar_list(self) -> contrato_pb2.ListServersResponse:
        resposta = contrato_pb2.ListServersResponse()
        ativos = sorted(self.ativos.values(), key=lambda item: item.rank)
        for registro in ativos:
            item = contrato_pb2.ServerInfo()
            item.nome = registro.nome
            item.rank = registro.rank
            resposta.servidores.append(item)
        return resposta

    def _processar_heartbeat(
        self, req: contrato_pb2.HeartbeatRequest
    ) -> contrato_pb2.HeartbeatResponse:
        resposta = contrato_pb2.HeartbeatResponse()
        nome = req.nome_servidor.strip()
        registro = self.ativos.get(nome)
        if registro is None:
            resposta.status = contrato_pb2.STATUS_ERRO
            resposta.erro_msg = "servidor não registrado"
            return resposta

        registro.last_seen_monotonic = time.monotonic()
        resposta.status = contrato_pb2.STATUS_SUCESSO
        return resposta


def main() -> None:
    Referencia().loop()


if __name__ == "__main__":
    main()
