import logging
import os
import random
import string
import threading
import time
from typing import List

import zmq

from shared.mensageria import (
    RelogioProcesso,
    cabecalho_texto,
    envelope_bytes,
    envelope_from_bytes,
    novo_cabecalho,
    origem_label,
    timestamp_to_text,
)
from shared.protos import contrato_pb2


logging.basicConfig(level=logging.INFO, format="%(asctime)s [CLIENTE] %(message)s")


ORQ_ENDPOINT = os.getenv("ORQ_ENDPOINT", "tcp://orquestrador:5555")
PROXY_SUB_ENDPOINT = os.getenv("PROXY_SUB_ENDPOINT", "tcp://proxy:5558")
NOME_USUARIO = os.getenv("CLIENTE_NOME", "cliente_python")
NOME_CANAL = os.getenv("CLIENTE_CANAL", "canal_padrao")


def _agora_texto() -> str:
    agora_ns = time.time_ns()
    return f"{agora_ns // 1_000_000_000}.{agora_ns % 1_000_000_000:09d}"


class Cliente:
    def __init__(self) -> None:
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect(ORQ_ENDPOINT)
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect(PROXY_SUB_ENDPOINT)
        self.subscribed_channels: set[str] = set()
        self.relogio = RelogioProcesso()
        self.origem = origem_label("cliente")
        logging.info("Conectado ao orquestrador em %s", ORQ_ENDPOINT)
        logging.info("Conectado ao proxy sub em %s", PROXY_SUB_ENDPOINT)

    def _enviar_e_aguardar(self, env: contrato_pb2.Envelope) -> contrato_pb2.Envelope:
        tipo = env.WhichOneof("conteudo") or "desconhecido"
        logging.info("Enviando %s %s", tipo, cabecalho_texto(env.cabecalho))
        self.socket.send(envelope_bytes(env))
        data = self.socket.recv()
        resp_env = envelope_from_bytes(data)
        self.relogio.on_receive(resp_env.cabecalho.relogio_logico)
        logging.info(
            "Recebido %s %s",
            resp_env.WhichOneof("conteudo") or "desconhecido",
            cabecalho_texto(resp_env.cabecalho),
        )
        return resp_env

    def fazer_login(self, nome_usuario: str) -> bool:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        login_req = contrato_pb2.LoginRequest()
        login_req.cabecalho.CopyFrom(env.cabecalho)
        login_req.nome_usuario = nome_usuario
        env.login_req.CopyFrom(login_req)

        resp_env = self._enviar_e_aguardar(env)
        conteudo = resp_env.WhichOneof("conteudo")
        if conteudo != "login_res":
            logging.error("Resposta inesperada ao login: %s", conteudo)
            return False

        res = resp_env.login_res
        if res.status == contrato_pb2.STATUS_SUCESSO:
            logging.info("Login bem-sucedido para '%s'", nome_usuario)
            return True

        logging.warning("Falha no login: %s", res.erro_msg)
        return False

    def criar_canal(self, nome_canal: str) -> None:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        req = contrato_pb2.CreateChannelRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        req.nome_canal = nome_canal
        env.create_channel_req.CopyFrom(req)

        resp_env = self._enviar_e_aguardar(env)
        conteudo = resp_env.WhichOneof("conteudo")
        if conteudo != "create_channel_res":
            logging.error("Resposta inesperada a criação de canal: %s", conteudo)
            return

        res = resp_env.create_channel_res
        if res.status == contrato_pb2.STATUS_SUCESSO:
            logging.info("Canal '%s' criado com sucesso", nome_canal)
        else:
            logging.warning("Falha ao criar canal '%s': %s", nome_canal, res.erro_msg)

    def listar_canais(self) -> List[str]:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        req = contrato_pb2.ListChannelsRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        env.list_channels_req.CopyFrom(req)

        resp_env = self._enviar_e_aguardar(env)
        conteudo = resp_env.WhichOneof("conteudo")
        if conteudo != "list_channels_res":
            logging.error("Resposta inesperada a listagem de canais: %s", conteudo)
            return []

        res = resp_env.list_channels_res
        canais = list(res.canais)
        logging.info("Canais existentes: %s", ", ".join(canais) if canais else "(nenhum)")
        return canais

    def publicar(self, canal: str, mensagem: str) -> bool:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        req = contrato_pb2.PublishRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        req.canal = canal
        req.mensagem = mensagem
        env.publish_req.CopyFrom(req)

        resp_env = self._enviar_e_aguardar(env)
        conteudo = resp_env.WhichOneof("conteudo")
        if conteudo != "publish_res":
            logging.error("Resposta inesperada ao publicar: %s", conteudo)
            return False
        res = resp_env.publish_res
        if res.status == contrato_pb2.STATUS_SUCESSO:
            return True
        logging.warning("Falha ao publicar em '%s': %s", canal, res.erro_msg)
        return False

    def inscrever(self, canal: str) -> None:
        if canal in self.subscribed_channels:
            return
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, canal.encode("utf-8"))
        self.subscribed_channels.add(canal)
        logging.info("Inscrito no canal '%s'", canal)

    def iniciar_receptor(self) -> None:
        def _loop() -> None:
            while True:
                try:
                    topic, payload = self.sub_socket.recv_multipart()
                    msg = contrato_pb2.ChannelMessage()
                    msg.ParseFromString(payload)
                    self.relogio.on_receive(msg.relogio_logico)
                    logging.info(
                        "[CANAL=%s] msg='%s' remetente=%s envio=%s recebimento=%s relogio=%s",
                        topic.decode("utf-8"),
                        msg.mensagem,
                        msg.remetente,
                        timestamp_to_text(msg.timestamp_envio),
                        _agora_texto(),
                        msg.relogio_logico,
                    )
                except Exception as exc:
                    logging.warning("Erro no receptor de pub/sub: %s", exc)
                    time.sleep(0.2)

        thread = threading.Thread(target=_loop, daemon=True)
        thread.start()


def main() -> None:
    cliente = Cliente()

    for _ in range(3):
        if cliente.fazer_login(NOME_USUARIO):
            break
        time.sleep(1)

    cliente.iniciar_receptor()

    canais = cliente.listar_canais()
    if len(canais) < 5:
        sufixo = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
        novo_canal = f"{NOME_CANAL}_{sufixo}"
        cliente.criar_canal(novo_canal)
        canais = cliente.listar_canais()

    while len(cliente.subscribed_channels) < 3 and len(cliente.subscribed_channels) < len(canais):
        disponiveis = [c for c in canais if c not in cliente.subscribed_channels]
        if not disponiveis:
            break
        cliente.inscrever(random.choice(disponiveis))

    while True:
        canais = cliente.listar_canais()
        if not canais:
            time.sleep(1)
            continue
        canal_escolhido = random.choice(canais)
        for _ in range(10):
            msg = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
            cliente.publicar(canal_escolhido, msg)
            time.sleep(1)


if __name__ == "__main__":
    main()
