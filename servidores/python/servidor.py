import json
import logging
import os
import time
from pathlib import Path

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


logging.basicConfig(level=logging.INFO, format="%(asctime)s [SERVIDOR] %(message)s")


ORQ_ENDPOINT = os.getenv("ORQ_ENDPOINT_SERVIDOR", "tcp://orquestrador:5556")
PROXY_PUB_ENDPOINT = os.getenv("PROXY_PUB_ENDPOINT", "tcp://proxy:5557")
REFERENCE_ENDPOINT = os.getenv("REFERENCE_ENDPOINT", "tcp://referencia:5559")
SERVER_NAME = os.getenv("SERVER_NAME", "servidor_python")
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

LOGINS_PATH = DATA_DIR / "logins.jsonl"
CANAIS_PATH = DATA_DIR / "canais.json"
PUBLICACOES_PATH = DATA_DIR / "publicacoes.jsonl"


class Servidor:
    def __init__(self) -> None:
        self.context = zmq.Context.instance()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect(ORQ_ENDPOINT)
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.connect(PROXY_PUB_ENDPOINT)
        self.reference_socket = self.context.socket(zmq.REQ)
        self.reference_socket.connect(REFERENCE_ENDPOINT)
        self.relogio = RelogioProcesso()
        self.origem = origem_label("servidor")
        self.requests_processadas = 0
        logging.info("Servidor conectado ao orquestrador em %s", ORQ_ENDPOINT)
        logging.info("Servidor conectado ao proxy pub em %s", PROXY_PUB_ENDPOINT)
        logging.info("Servidor conectado à referência em %s", REFERENCE_ENDPOINT)
        self._init_storage()
        self._registrar_na_referencia()
        self._listar_servidores_referencia()

    def _init_storage(self) -> None:
        if not LOGINS_PATH.exists():
            LOGINS_PATH.write_text("", encoding="utf-8")
        if not CANAIS_PATH.exists():
            CANAIS_PATH.write_text("[]", encoding="utf-8")
        if not PUBLICACOES_PATH.exists():
            PUBLICACOES_PATH.write_text("", encoding="utf-8")

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

    def _registrar_publicacao(
        self, canal: str, mensagem: str, remetente: str, timestamp_iso: str
    ) -> None:
        with PUBLICACOES_PATH.open("a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "canal": canal,
                        "mensagem": mensagem,
                        "remetente": remetente,
                        "timestamp": timestamp_iso,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    def _enviar_para_referencia(
        self, env: contrato_pb2.Envelope, atualizar_offset: bool = True
    ) -> tuple[contrato_pb2.Envelope, int, int]:
        tipo = env.WhichOneof("conteudo") or "desconhecido"
        logging.info("Enviando %s para referência %s", tipo, cabecalho_texto(env.cabecalho))
        envio_ns = time.time_ns()
        self.reference_socket.send(envelope_bytes(env))
        data = self.reference_socket.recv()
        recebimento_ns = time.time_ns()
        resp_env = envelope_from_bytes(data)
        self.relogio.on_receive(resp_env.cabecalho.relogio_logico)
        if atualizar_offset:
            self.relogio.atualizar_offset(resp_env.cabecalho.timestamp_envio, envio_ns, recebimento_ns)
            logging.info("Offset físico atualizado para %.3f ms", self.relogio.offset_ms())
        logging.info(
            "Recebido %s da referência %s",
            resp_env.WhichOneof("conteudo") or "desconhecido",
            cabecalho_texto(resp_env.cabecalho),
        )
        return resp_env, envio_ns, recebimento_ns

    def _registrar_na_referencia(self) -> None:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        req = contrato_pb2.RegisterServerRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        req.nome_servidor = SERVER_NAME
        env.register_server_req.CopyFrom(req)

        resp_env, _, _ = self._enviar_para_referencia(env)
        if resp_env.WhichOneof("conteudo") != "register_server_res":
            raise RuntimeError("Resposta inesperada no registro da referência")

        res = resp_env.register_server_res
        if res.status != contrato_pb2.STATUS_SUCESSO:
            raise RuntimeError(f"Falha ao registrar servidor na referência: {res.erro_msg}")

        logging.info("Servidor %s registrado na referência com rank=%s", SERVER_NAME, res.rank)

    def _listar_servidores_referencia(self) -> list[contrato_pb2.ServerInfo]:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        req = contrato_pb2.ListServersRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        env.list_servers_req.CopyFrom(req)

        resp_env, _, _ = self._enviar_para_referencia(env)
        if resp_env.WhichOneof("conteudo") != "list_servers_res":
            logging.warning("Resposta inesperada ao pedir lista de servidores")
            return []

        servidores = list(resp_env.list_servers_res.servidores)
        resumo = ", ".join(f"{item.nome}(rank={item.rank})" for item in servidores) or "(nenhum)"
        logging.info("Servidores disponíveis na referência: %s", resumo)
        return servidores

    def _heartbeat_e_sincronizar(self) -> None:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        req = contrato_pb2.HeartbeatRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        req.nome_servidor = SERVER_NAME
        env.heartbeat_req.CopyFrom(req)

        resp_env, _, _ = self._enviar_para_referencia(env, atualizar_offset=True)
        if resp_env.WhichOneof("conteudo") != "heartbeat_res":
            logging.warning("Resposta inesperada ao heartbeat")
            return

        res = resp_env.heartbeat_res
        if res.status != contrato_pb2.STATUS_SUCESSO:
            logging.warning("Falha ao enviar heartbeat: %s", res.erro_msg)
            return

        logging.info("Heartbeat enviado com sucesso para %s", SERVER_NAME)
        self._listar_servidores_referencia()

    def loop(self) -> None:
        logging.info("Servidor iniciado. Aguardando mensagens...")
        while True:
            data = self.socket.recv()
            env = envelope_from_bytes(data)
            self.relogio.on_receive(env.cabecalho.relogio_logico)
            tipo = env.WhichOneof("conteudo")
            logging.info("Recebido %s %s", tipo, cabecalho_texto(env.cabecalho))

            if tipo == "login_req":
                resp = self._processar_login(env.login_req)
            elif tipo == "create_channel_req":
                resp = self._processar_create_channel(env.create_channel_req)
            elif tipo == "list_channels_req":
                resp = self._processar_list_channels(env.list_channels_req)
            elif tipo == "publish_req":
                resp = self._processar_publish(env.publish_req, env.cabecalho)
            else:
                logging.warning("Tipo de mensagem não suportado: %s", tipo)
                continue

            cab = novo_cabecalho(self.origem, self.relogio)
            resp_env = contrato_pb2.Envelope()
            resp_env.cabecalho.CopyFrom(cab)

            if isinstance(resp, contrato_pb2.LoginResponse):
                resp.cabecalho.CopyFrom(cab)
                resp_env.login_res.CopyFrom(resp)
            elif isinstance(resp, contrato_pb2.CreateChannelResponse):
                resp.cabecalho.CopyFrom(cab)
                resp_env.create_channel_res.CopyFrom(resp)
            elif isinstance(resp, contrato_pb2.ListChannelsResponse):
                resp.cabecalho.CopyFrom(cab)
                resp_env.list_channels_res.CopyFrom(resp)
            elif isinstance(resp, contrato_pb2.PublishResponse):
                resp.cabecalho.CopyFrom(cab)
                resp_env.publish_res.CopyFrom(resp)

            logging.info("Enviando %s %s", resp_env.WhichOneof("conteudo"), cabecalho_texto(resp_env.cabecalho))
            self.socket.send(envelope_bytes(resp_env))

            self.requests_processadas += 1
            if self.requests_processadas % 10 == 0:
                self._heartbeat_e_sincronizar()

    def _processar_login(self, req: contrato_pb2.LoginRequest) -> contrato_pb2.LoginResponse:
        res = contrato_pb2.LoginResponse()

        nome = req.nome_usuario.strip()
        if not nome:
            res.status = contrato_pb2.STATUS_ERRO
            res.erro_msg = "nome de usuário vazio"
            return res

        self._registrar_login(nome, timestamp_to_text(req.cabecalho.timestamp_envio))
        res.status = contrato_pb2.STATUS_SUCESSO
        return res

    def _processar_create_channel(
        self, req: contrato_pb2.CreateChannelRequest
    ) -> contrato_pb2.CreateChannelResponse:
        res = contrato_pb2.CreateChannelResponse()

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
        res.canais.extend(self._ler_canais())
        return res

    def _processar_publish(
        self, req: contrato_pb2.PublishRequest, envelope_cabecalho: contrato_pb2.Cabecalho
    ) -> contrato_pb2.PublishResponse:
        res = contrato_pb2.PublishResponse()

        canal = req.canal.strip()
        mensagem = req.mensagem.strip()
        if not canal:
            res.status = contrato_pb2.STATUS_ERRO
            res.erro_msg = "canal vazio"
            return res
        if not mensagem:
            res.status = contrato_pb2.STATUS_ERRO
            res.erro_msg = "mensagem vazia"
            return res

        canais = self._ler_canais()
        if canal not in canais:
            res.status = contrato_pb2.STATUS_ERRO
            res.erro_msg = "canal inexistente"
            return res

        remetente = envelope_cabecalho.linguagem_origem or "desconhecido"
        cab_publicacao = novo_cabecalho(self.origem, self.relogio)
        channel_msg = contrato_pb2.ChannelMessage()
        channel_msg.canal = canal
        channel_msg.mensagem = mensagem
        channel_msg.remetente = remetente
        channel_msg.timestamp_envio.CopyFrom(cab_publicacao.timestamp_envio)
        channel_msg.relogio_logico = cab_publicacao.relogio_logico

        logging.info("Publicando em %s ts=%s relogio=%s", canal, timestamp_to_text(channel_msg.timestamp_envio), channel_msg.relogio_logico)
        self.pub_socket.send_multipart([canal.encode("utf-8"), channel_msg.SerializeToString()])
        self._registrar_publicacao(
            canal,
            mensagem,
            remetente,
            timestamp_to_text(channel_msg.timestamp_envio),
        )

        res.status = contrato_pb2.STATUS_SUCESSO
        return res


def main() -> None:
    servidor = Servidor()
    servidor.loop()


if __name__ == "__main__":
    main()
