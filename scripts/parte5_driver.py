from __future__ import annotations

import argparse
import os
import random
import string
import sys
import time

import zmq

from shared.mensageria import RelogioProcesso, envelope_bytes, envelope_from_bytes, novo_cabecalho
from shared.protos import contrato_pb2


ORQ_ENDPOINT = os.getenv("ORQ_ENDPOINT", "tcp://orquestrador:5555")
PROXY_SUB_ENDPOINT = os.getenv("PROXY_SUB_ENDPOINT", "tcp://proxy:5558")
CLIENTE_NOME = os.getenv("CLIENTE_NOME", "validador_parte5")
CLIENTE_CANAL = os.getenv("CLIENTE_CANAL", "canal_validador")


def _novo_env(relogio: RelogioProcesso, origem: str) -> contrato_pb2.Envelope:
    env = contrato_pb2.Envelope()
    env.cabecalho.CopyFrom(novo_cabecalho(origem, relogio))
    return env


def _send(sock: zmq.Socket, env: contrato_pb2.Envelope) -> contrato_pb2.Envelope:
    sock.send(envelope_bytes(env))
    return envelope_from_bytes(sock.recv())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", default="nominal")
    args = parser.parse_args()

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.DEALER)
    sock.connect(ORQ_ENDPOINT)
    sub = ctx.socket(zmq.SUB)
    sub.connect(PROXY_SUB_ENDPOINT)
    sub.setsockopt(zmq.SUBSCRIBE, b"")
    sub.setsockopt(zmq.RCVTIMEO, 4000)

    relogio = RelogioProcesso()
    origem = "python-cliente"
    sufixo = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    canal = f"{CLIENTE_CANAL}_{sufixo}"

    login_env = _novo_env(relogio, origem)
    login_env.login_req.CopyFrom(contrato_pb2.LoginRequest(cabecalho=login_env.cabecalho, nome_usuario=CLIENTE_NOME))
    login_resp = _send(sock, login_env)
    if login_resp.WhichOneof("conteudo") != "login_res" or login_resp.login_res.status != contrato_pb2.STATUS_SUCESSO:
        print(
            f"login falhou tipo={login_resp.WhichOneof('conteudo')} "
            f"status={getattr(login_resp.login_res, 'status', None)} "
            f"erro={getattr(login_resp.login_res, 'erro_msg', '')}",
            file=sys.stderr,
        )
        return 1

    create_env = _novo_env(relogio, origem)
    create_env.create_channel_req.CopyFrom(
        contrato_pb2.CreateChannelRequest(cabecalho=create_env.cabecalho, nome_canal=canal)
    )
    create_resp = _send(sock, create_env)
    if (
        create_resp.WhichOneof("conteudo") != "create_channel_res"
        or create_resp.create_channel_res.status != contrato_pb2.STATUS_SUCESSO
    ):
        print("create_channel falhou", file=sys.stderr)
        return 1

    list_env = _novo_env(relogio, origem)
    list_env.list_channels_req.CopyFrom(contrato_pb2.ListChannelsRequest(cabecalho=list_env.cabecalho))
    list_resp = _send(sock, list_env)
    if list_resp.WhichOneof("conteudo") != "list_channels_res" or canal not in list_resp.list_channels_res.canais:
        print("list_channels nao refletiu o canal criado", file=sys.stderr)
        return 1

    publish_env = _novo_env(relogio, origem)
    publish_env.publish_req.CopyFrom(
        contrato_pb2.PublishRequest(cabecalho=publish_env.cabecalho, canal=canal, mensagem=f"ola-{sufixo}")
    )
    publish_resp = _send(sock, publish_env)
    if publish_resp.WhichOneof("conteudo") != "publish_res" or publish_resp.publish_res.status != contrato_pb2.STATUS_SUCESSO:
        print("publish falhou", file=sys.stderr)
        return 1

    deadline = time.monotonic() + 8.0
    while time.monotonic() < deadline:
        remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
        if not sub.poll(min(500, remaining_ms)):
            continue
        try:
            topic, payload = sub.recv_multipart(zmq.NOBLOCK)
        except zmq.Again:
            continue
        if topic.decode("utf-8") != canal:
            continue
        msg = contrato_pb2.ChannelMessage()
        msg.ParseFromString(payload)
        if msg.mensagem != f"ola-{sufixo}":
            print("mensagem pub/sub inesperada", file=sys.stderr)
            return 1
        break
    else:
        print("timeout aguardando pub/sub", file=sys.stderr)
        return 1

    print(f"fase={args.phase} ok canal={canal}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
