import json
import logging
import os
from pathlib import Path

import zmq

from shared.mensageria import (
    envelope_bytes,
    envelope_from_bytes,
    novo_cabecalho,
    origem_label,
)
from shared.protos import contrato_pb2


logging.basicConfig(level=logging.INFO, format="%(asctime)s [SERVIDOR] %(message)s")


ORQ_ENDPOINT = os.getenv("ORQ_ENDPOINT_SERVIDOR", "tcp://orquestrador:5556")
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

LOGINS_PATH = DATA_DIR / "logins.jsonl"
CANAIS_PATH = DATA_DIR / "canais.json"


class Servidor:
    def __init__(self) -> None:
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect(ORQ_ENDPOINT)
        logging.info("Servidor conectado ao orquestrador em %s", ORQ_ENDPOINT)
        self._init_storage()
        self._registrar_no_orquestrador()

    def _init_storage(self) -> None:
        if not LOGINS_PATH.exists():
            LOGINS_PATH.write_text("", encoding="utf-8")
        if not CANAIS_PATH.exists():
            CANAIS_PATH.write_text("[]", encoding="utf-8")

    def _registrar_no_orquestrador(self) -> None:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(origem_label("servidor")))
        req = contrato_pb2.ListChannelsRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        env.list_channels_req.CopyFrom(req)
        try:
            self.socket.send(envelope_bytes(env), zmq.NOBLOCK)
        except zmq.Again:
            logging.warning("Não foi possível enviar mensagem de registro inicial ao orquestrador.")

    def _ler_canais(self) -> list[str]:
        try:
            return json.loads(CANAIS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return []

    def _salvar_canais(self, canais: list[str]) -> None:
        CANAIS_PATH.write_text(json.dumps(canais, ensure_ascii=False), encoding="utf-8")

    def _registrar_login(self, nome_usuario: str, timestamp_iso: str) -> None:
        with LOGINS_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"usuario": nome_usuario, "timestamp": timestamp_iso}, ensure_ascii=False) + "\n")

    def loop(self) -> None:
        logging.info("Servidor iniciado. Aguardando mensagens...")
        while True:
            data = self.socket.recv()
            env = envelope_from_bytes(data)
            tipo = env.WhichOneof("conteudo")

            logging.info("Servidor python processando mensagem: %s", tipo)

            if tipo == "login_req":
                resp = self._processar_login(env.login_req)
            elif tipo == "create_channel_req":
                resp = self._processar_create_channel(env.create_channel_req)
            elif tipo == "list_channels_req":
                resp = self._processar_list_channels(env.list_channels_req)
            else:
                logging.warning("Tipo de mensagem não suportado: %s", tipo)
                continue

            cab = novo_cabecalho(origem_label("servidor"))
            resp_env = contrato_pb2.Envelope()
            resp_env.cabecalho.CopyFrom(cab)

            if isinstance(resp, contrato_pb2.LoginResponse):
                resp_env.login_res.CopyFrom(resp)
            elif isinstance(resp, contrato_pb2.CreateChannelResponse):
                resp_env.create_channel_res.CopyFrom(resp)
            elif isinstance(resp, contrato_pb2.ListChannelsResponse):
                resp_env.list_channels_res.CopyFrom(resp)

            self.socket.send(envelope_bytes(resp_env))

    def _processar_login(self, req: contrato_pb2.LoginRequest) -> contrato_pb2.LoginResponse:
        res = contrato_pb2.LoginResponse()
        res.cabecalho.CopyFrom(req.cabecalho)

        nome = req.nome_usuario.strip()
        if not nome:
            res.status = contrato_pb2.STATUS_ERRO
            res.erro_msg = "nome de usuário vazio"
            return res

        ts = req.cabecalho.timestamp_envio
        ts_iso = ""
        if ts.seconds or ts.nanos:
            ts_iso = f"{ts.seconds}.{ts.nanos:09d}"
        self._registrar_login(nome, ts_iso)

        res.status = contrato_pb2.STATUS_SUCESSO
        return res

    def _processar_create_channel(
        self, req: contrato_pb2.CreateChannelRequest
    ) -> contrato_pb2.CreateChannelResponse:
        res = contrato_pb2.CreateChannelResponse()
        res.cabecalho.CopyFrom(req.cabecalho)

        nome = req.nome_canal.strip()
        if not nome:
            res.status = contrato_pb2.STATUS_ERRO
            res.erro_msg = "nome de canal vazio"
            return res

        canais = self._ler_canais()
        if nome in canais:
            res.status = contrato_pb2.STATUS_ERRO
            res.erro_msg = "canal já existe"
            return res

        canais.append(nome)
        self._salvar_canais(canais)

        res.status = contrato_pb2.STATUS_SUCESSO
        return res

    def _processar_list_channels(
        self, req: contrato_pb2.ListChannelsRequest
    ) -> contrato_pb2.ListChannelsResponse:
        res = contrato_pb2.ListChannelsResponse()
        res.cabecalho.CopyFrom(req.cabecalho)
        canais = self._ler_canais()
        res.canais.extend(canais)
        return res


def main() -> None:
    servidor = Servidor()
    servidor.loop()


if __name__ == "__main__":
    main()

