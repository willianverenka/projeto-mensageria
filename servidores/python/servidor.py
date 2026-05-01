import json
import logging
import os
import threading
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
    timestamp_from_ns,
    timestamp_to_ns,
    timestamp_to_text,
)
from shared.protos import contrato_pb2


logging.basicConfig(level=logging.INFO, format="%(asctime)s [SERVIDOR] %(message)s")


ORQ_ENDPOINT = os.getenv("ORQ_ENDPOINT_SERVIDOR", "tcp://orquestrador:5556")
PROXY_PUB_ENDPOINT = os.getenv("PROXY_PUB_ENDPOINT", "tcp://proxy:5557")
PROXY_SUB_ENDPOINT = os.getenv("PROXY_SUB_ENDPOINT", "tcp://proxy:5558")
REFERENCE_ENDPOINT = os.getenv("REFERENCE_ENDPOINT", "tcp://referencia:5559")
SERVER_NAME = os.getenv("SERVER_NAME", "servidor_python")
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

CLOCK_PORT = 5560
ELECTION_PORT = 5561
REQUEST_TIMEOUT_MS = 2000
ANNOUNCEMENT_TIMEOUT_SECONDS = 3.0
STARTUP_ELECTION_DELAY_SECONDS = 0.2
TOPICO_COORDENADOR = "servers"

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
        self.announce_socket = self.context.socket(zmq.PUB)
        self.announce_socket.connect(PROXY_PUB_ENDPOINT)
        self.reference_socket = self.context.socket(zmq.REQ)
        self.reference_socket.connect(REFERENCE_ENDPOINT)
        self.reference_socket.setsockopt(zmq.RCVTIMEO, REQUEST_TIMEOUT_MS)
        self.reference_socket.setsockopt(zmq.SNDTIMEO, REQUEST_TIMEOUT_MS)
        self.reference_socket.setsockopt(zmq.LINGER, 0)
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect(PROXY_SUB_ENDPOINT)
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, TOPICO_COORDENADOR.encode("utf-8"))
        self.relogio = RelogioProcesso()
        self.origem = origem_label("servidor")
        self.requests_processadas = 0
        self._lock = threading.RLock()
        self._coordinator_name = ""
        self._coordinator_version = 0
        self._active_servers: dict[str, int] = {}
        self._my_rank = 0
        self._election_running = False
        logging.info("Servidor conectado ao orquestrador em %s", ORQ_ENDPOINT)
        logging.info("Servidor conectado ao proxy pub em %s", PROXY_PUB_ENDPOINT)
        logging.info("Servidor conectado ao proxy sub em %s", PROXY_SUB_ENDPOINT)
        logging.info("Servidor conectado à referência em %s", REFERENCE_ENDPOINT)
        self._init_storage()
        self._registrar_na_referencia()
        servidores = self._listar_servidores_referencia()
        self._atualizar_servidores_ativos(servidores)
        self._definir_coordenador_tentativo(servidores)
        self._iniciar_servicos_internos()
        time.sleep(STARTUP_ELECTION_DELAY_SECONDS)
        self._executar_eleicao("startup")
        logging.info(
            "Servidor pronto. Rank local=%s coordenador=%s",
            self._my_rank,
            self._coordenador_atual() or "(desconhecido)",
        )

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
        try:
            self.reference_socket.send(envelope_bytes(env))
            data = self.reference_socket.recv()
        except zmq.Again as exc:
            raise TimeoutError("timeout ao comunicar com a referência") from exc
        recebimento_ns = time.time_ns()
        resp_env = envelope_from_bytes(data)
        self.relogio.on_receive(resp_env.cabecalho.relogio_logico)
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

        self._my_rank = res.rank
        self._atualizar_servidores_ativos(
            [contrato_pb2.ServerInfo(nome=SERVER_NAME, rank=self._my_rank)]
        )
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
        self._atualizar_servidores_ativos(servidores)
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
        servidores = self._listar_servidores_referencia()
        if self._coordenador_atual() and self._coordenador_atual() not in {item.nome for item in servidores}:
            logging.warning(
                "Coordenador %s ausente da lista ativa; iniciando eleição",
                self._coordenador_atual(),
            )
            self._iniciar_eleicao_async("coordenador ausente da lista ativa")

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
            if self.requests_processadas % 15 == 0:
                self._sincronizar_relogio_fisico()

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

    def _iniciar_servicos_internos(self) -> None:
        threading.Thread(target=self._loop_relogio, daemon=True).start()
        threading.Thread(target=self._loop_eleicao, daemon=True).start()
        threading.Thread(target=self._loop_anuncios_coord, daemon=True).start()

    def _loop_relogio(self) -> None:
        socket = self.context.socket(zmq.REP)
        socket.bind(f"tcp://*:{CLOCK_PORT}")
        logging.info("Serviço de relógio interno ouvindo em tcp://*:%s", CLOCK_PORT)
        while True:
            try:
                data = socket.recv()
                env = envelope_from_bytes(data)
                self.relogio.on_receive(env.cabecalho.relogio_logico)
                tipo = env.WhichOneof("conteudo")

                resposta_env = contrato_pb2.Envelope()
                cab = novo_cabecalho(self.origem, self.relogio)
                resposta_env.cabecalho.CopyFrom(cab)

                if tipo != "heartbeat_req":
                    resposta = contrato_pb2.HeartbeatResponse()
                    resposta.status = contrato_pb2.STATUS_ERRO
                    resposta.erro_msg = f"tipo não suportado: {tipo}"
                else:
                    resposta = self._processar_pedido_relogio(env.heartbeat_req)

                resposta.cabecalho.CopyFrom(cab)
                resposta_env.heartbeat_res.CopyFrom(resposta)
                socket.send(envelope_bytes(resposta_env))
            except Exception as exc:
                logging.warning("Erro no serviço interno de relógio: %s", exc)

    def _loop_eleicao(self) -> None:
        socket = self.context.socket(zmq.REP)
        socket.bind(f"tcp://*:{ELECTION_PORT}")
        logging.info("Serviço de eleição interno ouvindo em tcp://*:%s", ELECTION_PORT)
        while True:
            try:
                data = socket.recv()
                env = envelope_from_bytes(data)
                self.relogio.on_receive(env.cabecalho.relogio_logico)
                tipo = env.WhichOneof("conteudo")

                resposta_env = contrato_pb2.Envelope()
                cab = novo_cabecalho(self.origem, self.relogio)
                resposta_env.cabecalho.CopyFrom(cab)

                if tipo != "register_server_req":
                    resposta = contrato_pb2.RegisterServerResponse()
                    resposta.status = contrato_pb2.STATUS_ERRO
                    resposta.erro_msg = f"tipo não suportado: {tipo}"
                else:
                    resposta = self._processar_pedido_eleicao(env.register_server_req)

                resposta.cabecalho.CopyFrom(cab)
                resposta_env.register_server_res.CopyFrom(resposta)
                socket.send(envelope_bytes(resposta_env))
            except Exception as exc:
                logging.warning("Erro no serviço interno de eleição: %s", exc)

    def _loop_anuncios_coord(self) -> None:
        logging.info("Escutando anúncios de coordenador no tópico '%s'", TOPICO_COORDENADOR)
        while True:
            try:
                topico, payload = self.sub_socket.recv_multipart()
                if topico.decode("utf-8") != TOPICO_COORDENADOR:
                    continue

                msg = contrato_pb2.ChannelMessage()
                msg.ParseFromString(payload)
                self.relogio.on_receive(msg.relogio_logico)

                coordenador = msg.mensagem.strip()
                if not coordenador:
                    continue

                self._set_coordenador(
                    coordenador,
                    f"anúncio publicado por {msg.remetente or 'desconhecido'}",
                    incrementar_versao=True,
                )
                logging.info(
                    "Anúncio de coordenador recebido em servers: %s",
                    coordenador,
                )
            except Exception as exc:
                logging.warning("Erro ao receber anúncio de coordenador: %s", exc)
                time.sleep(0.2)

    def _servidores_ativos_snapshot(self) -> list[contrato_pb2.ServerInfo]:
        with self._lock:
            servidores = []
            for nome, rank in self._active_servers.items():
                item = contrato_pb2.ServerInfo()
                item.nome = nome
                item.rank = rank
                servidores.append(item)
        servidores.sort(key=lambda item: item.rank)
        return servidores

    def _resumo_servidores(self, servidores: list[contrato_pb2.ServerInfo]) -> str:
        return ", ".join(f"{item.nome}(rank={item.rank})" for item in servidores) or "(nenhum)"

    def _atualizar_servidores_ativos(self, servidores: list[contrato_pb2.ServerInfo]) -> None:
        with self._lock:
            if servidores:
                self._active_servers = {item.nome: item.rank for item in servidores}
            elif not self._active_servers and self._my_rank:
                self._active_servers = {SERVER_NAME: self._my_rank}
            if self._my_rank:
                self._active_servers[SERVER_NAME] = self._my_rank

        logging.info(
            "Lista ativa local atualizada: %s",
            self._resumo_servidores(self._servidores_ativos_snapshot()),
        )

    def _maior_servidor_ativo(self) -> str:
        servidores = self._servidores_ativos_snapshot()
        if not servidores:
            return SERVER_NAME
        return max(servidores, key=lambda item: item.rank).nome

    def _coordenador_atual(self) -> str:
        with self._lock:
            return self._coordinator_name

    def _versao_coordenador(self) -> int:
        with self._lock:
            return self._coordinator_version

    def _set_coordenador(
        self,
        nome: str,
        motivo: str,
        *,
        incrementar_versao: bool,
    ) -> None:
        with self._lock:
            antigo = self._coordinator_name
            self._coordinator_name = nome
            if incrementar_versao:
                self._coordinator_version += 1
            versao = self._coordinator_version

        if antigo == nome:
            logging.info("Coordenador mantido em %s (%s, versao=%s)", nome, motivo, versao)
        else:
            logging.info("Coordenador atualizado de %s para %s (%s, versao=%s)", antigo or "(vazio)", nome, motivo, versao)

    def _definir_coordenador_tentativo(self, servidores: list[contrato_pb2.ServerInfo]) -> None:
        if self._coordenador_atual():
            return

        candidato = self._maior_servidor_ativo()
        self._set_coordenador(candidato, "coordenador tentativo inicial", incrementar_versao=False)

    def _is_coordenador(self) -> bool:
        return self._coordenador_atual() == SERVER_NAME

    def _iniciar_eleicao_async(self, motivo: str) -> None:
        with self._lock:
            if self._election_running:
                logging.info("Eleição já em andamento; ignorando novo gatilho (%s)", motivo)
                return
            self._election_running = True

        def _worker() -> None:
            try:
                self._executar_eleicao(motivo)
            finally:
                with self._lock:
                    self._election_running = False

        threading.Thread(target=_worker, daemon=True).start()

    def _executar_eleicao(self, motivo: str) -> None:
        logging.info("Iniciando eleição (%s)", motivo)
        versao_inicial = self._versao_coordenador()
        servidores = self._servidores_ativos_snapshot()
        superiores = [
            item
            for item in servidores
            if item.rank > self._my_rank and item.nome != SERVER_NAME
        ]

        if not superiores:
            self._tornar_coordenador("nenhum servidor de maior rank respondeu")
            return

        respostas_ok: list[contrato_pb2.ServerInfo] = []
        for item in sorted(superiores, key=lambda server: server.rank, reverse=True):
            if self._consultar_eleicao_servidor(item.nome):
                respostas_ok.append(item)

        if not respostas_ok:
            if self._versao_coordenador() > versao_inicial:
                logging.info("Eleição concluída durante a coleta de respostas")
                return
            self._tornar_coordenador("nenhum servidor de maior rank respondeu")
            return

        if self._versao_coordenador() > versao_inicial:
            logging.info("Eleição concluída por anúncio recebido durante a coleta")
            return

        deadline = time.monotonic() + ANNOUNCEMENT_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if self._versao_coordenador() > versao_inicial:
                logging.info("Eleição concluída por anúncio de coordenador")
                return
            time.sleep(0.1)

        candidato = max(respostas_ok, key=lambda item: item.rank)
        self._set_coordenador(
            candidato.nome,
            f"coordenador inferido após timeout de anúncio ({motivo})",
            incrementar_versao=True,
        )

    def _tornar_coordenador(self, motivo: str) -> None:
        self._set_coordenador(SERVER_NAME, motivo, incrementar_versao=True)
        self._anunciar_coordenador()

    def _anunciar_coordenador(self) -> None:
        cab = novo_cabecalho(self.origem, self.relogio)
        msg = contrato_pb2.ChannelMessage()
        msg.canal = TOPICO_COORDENADOR
        msg.mensagem = SERVER_NAME
        msg.remetente = SERVER_NAME
        msg.timestamp_envio.CopyFrom(cab.timestamp_envio)
        msg.relogio_logico = cab.relogio_logico
        try:
            self.announce_socket.send_multipart(
                [TOPICO_COORDENADOR.encode("utf-8"), msg.SerializeToString()]
            )
            logging.info("Anunciado coordenador em servers: %s", SERVER_NAME)
        except Exception as exc:
            logging.warning("Falha ao anunciar coordenador: %s", exc)

    def _enviar_para_servidor(
        self,
        nome_servidor: str,
        porta: int,
        env: contrato_pb2.Envelope,
        timeout_ms: int = REQUEST_TIMEOUT_MS,
    ) -> tuple[contrato_pb2.Envelope | None, int, int]:
        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
        socket.setsockopt(zmq.SNDTIMEO, timeout_ms)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(f"tcp://{nome_servidor}:{porta}")
        envio_ns = time.time_ns()
        try:
            socket.send(envelope_bytes(env))
            data = socket.recv()
            recebimento_ns = time.time_ns()
            resp_env = envelope_from_bytes(data)
            self.relogio.on_receive(resp_env.cabecalho.relogio_logico)
            return resp_env, envio_ns, recebimento_ns
        except zmq.Again:
            return None, envio_ns, time.time_ns()
        except Exception as exc:
            logging.warning("Falha ao comunicar com %s:%s: %s", nome_servidor, porta, exc)
            return None, envio_ns, time.time_ns()
        finally:
            socket.close(0)

    def _consultar_relogio_servidor(
        self, nome_servidor: str
    ) -> contrato_pb2.Envelope | None:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        req = contrato_pb2.HeartbeatRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        req.nome_servidor = SERVER_NAME
        env.heartbeat_req.CopyFrom(req)

        logging.info(
            "Enviando heartbeat_req para %s %s",
            nome_servidor,
            cabecalho_texto(env.cabecalho),
        )
        resp_env, _, _ = self._enviar_para_servidor(nome_servidor, CLOCK_PORT, env)
        if resp_env is None:
            logging.warning("Sem resposta de relógio de %s", nome_servidor)
            return None
        if resp_env.WhichOneof("conteudo") != "heartbeat_res":
            logging.warning("Resposta inesperada de relógio de %s", nome_servidor)
            return None
        if resp_env.heartbeat_res.status != contrato_pb2.STATUS_SUCESSO:
            logging.warning(
                "Servidor %s rejeitou pedido de relógio: %s",
                nome_servidor,
                resp_env.heartbeat_res.erro_msg,
            )
            return None
        logging.info(
            "Recebido heartbeat_res de %s %s",
            nome_servidor,
            cabecalho_texto(resp_env.cabecalho),
        )
        return resp_env

    def _consultar_eleicao_servidor(self, nome_servidor: str) -> bool:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        req = contrato_pb2.RegisterServerRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        req.nome_servidor = SERVER_NAME
        env.register_server_req.CopyFrom(req)

        logging.info(
            "Enviando pedido de eleição para %s %s",
            nome_servidor,
            cabecalho_texto(env.cabecalho),
        )
        resp_env, _, _ = self._enviar_para_servidor(
            nome_servidor, ELECTION_PORT, env, timeout_ms=REQUEST_TIMEOUT_MS
        )
        if resp_env is None:
            logging.warning("Sem resposta de eleição de %s", nome_servidor)
            return False
        if resp_env.WhichOneof("conteudo") != "register_server_res":
            logging.warning("Resposta inesperada de eleição de %s", nome_servidor)
            return False
        if resp_env.register_server_res.status != contrato_pb2.STATUS_SUCESSO:
            logging.warning(
                "Servidor %s rejeitou eleição: %s",
                nome_servidor,
                resp_env.register_server_res.erro_msg,
            )
            return False

        logging.info(
            "Servidor %s respondeu OK à eleição (rank=%s)",
            nome_servidor,
            resp_env.register_server_res.rank,
        )
        return True

    def _sincronizar_como_seguidor(self, coordenador: str) -> bool:
        if not coordenador:
            return False

        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        req = contrato_pb2.HeartbeatRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        req.nome_servidor = SERVER_NAME
        env.heartbeat_req.CopyFrom(req)

        resp_env, envio_ns, recebimento_ns = self._enviar_para_servidor(
            coordenador, CLOCK_PORT, env
        )
        if resp_env is None:
            return False
        if resp_env.WhichOneof("conteudo") != "heartbeat_res":
            return False
        if resp_env.heartbeat_res.status != contrato_pb2.STATUS_SUCESSO:
            return False

        self.relogio.atualizar_offset(
            resp_env.cabecalho.timestamp_envio,
            envio_ns,
            recebimento_ns,
        )
        logging.info(
            "Offset físico atualizado para %.3f ms usando coordenador %s",
            self.relogio.offset_ms(),
            coordenador,
        )
        return True

    def _sincronizar_como_coordenador(self) -> None:
        servidores = self._servidores_ativos_snapshot()
        amostras = [self.relogio.now_corrigido_ns()]
        participantes = [SERVER_NAME]
        falhas: list[str] = []

        for item in servidores:
            if item.nome == SERVER_NAME:
                continue
            resp_env = self._consultar_relogio_servidor(item.nome)
            if resp_env is None:
                falhas.append(item.nome)
                continue
            amostras.append(timestamp_to_ns(resp_env.cabecalho.timestamp_envio))
            participantes.append(item.nome)

        media_ns = sum(amostras) // len(amostras)
        agora_ns = time.time_ns()
        self.relogio.atualizar_offset(timestamp_from_ns(media_ns), agora_ns, agora_ns)
        logging.info(
            "Berkeley aplicado pelo coordenador %s com participantes=%s falhas=%s media=%s offset=%.3f ms",
            SERVER_NAME,
            ", ".join(participantes),
            ", ".join(falhas) if falhas else "(nenhuma)",
            timestamp_to_text(timestamp_from_ns(media_ns)),
            self.relogio.offset_ms(),
        )

    def _sincronizar_relogio_fisico(self) -> None:
        coordenador = self._coordenador_atual()
        if not coordenador:
            logging.warning("Coordenador desconhecido; iniciando eleição")
            self._executar_eleicao("coordenador desconhecido")
            coordenador = self._coordenador_atual()

        if self._is_coordenador():
            self._sincronizar_como_coordenador()
            return

        ativos = {item.nome for item in self._servidores_ativos_snapshot()}
        if coordenador and coordenador not in ativos:
            logging.warning(
                "Coordenador %s ausente da lista ativa; iniciando eleição",
                coordenador,
            )
            self._executar_eleicao("coordenador ausente da lista ativa")
            coordenador = self._coordenador_atual()
            if self._is_coordenador():
                self._sincronizar_como_coordenador()
                return

        if coordenador and self._sincronizar_como_seguidor(coordenador):
            return

        logging.warning("Falha ao sincronizar com coordenador %s; iniciando eleição", coordenador or "(desconhecido)")
        self._executar_eleicao("falha ao sincronizar com coordenador")
        coordenador = self._coordenador_atual()
        if self._is_coordenador():
            self._sincronizar_como_coordenador()
        elif coordenador:
            if not self._sincronizar_como_seguidor(coordenador):
                logging.warning("Ainda não foi possível sincronizar com %s após a eleição", coordenador)

    def _processar_pedido_relogio(
        self, req: contrato_pb2.HeartbeatRequest
    ) -> contrato_pb2.HeartbeatResponse:
        resposta = contrato_pb2.HeartbeatResponse()
        solicitante = req.nome_servidor.strip() or "desconhecido"
        coordenador = self._coordenador_atual()

        if self._is_coordenador():
            resposta.status = contrato_pb2.STATUS_SUCESSO
            logging.info("Respondendo relógio ao servidor %s como coordenador", solicitante)
            return resposta

        if solicitante == coordenador and coordenador:
            resposta.status = contrato_pb2.STATUS_SUCESSO
            logging.info("Respondendo relógio ao coordenador %s", solicitante)
            return resposta

        resposta.status = contrato_pb2.STATUS_ERRO
        resposta.erro_msg = "nao sou coordenador"
        logging.info(
            "Pedido de relógio de %s rejeitado; coordenador atual=%s",
            solicitante,
            coordenador or "(desconhecido)",
        )
        return resposta

    def _processar_pedido_eleicao(
        self, req: contrato_pb2.RegisterServerRequest
    ) -> contrato_pb2.RegisterServerResponse:
        resposta = contrato_pb2.RegisterServerResponse()
        solicitante = req.nome_servidor.strip() or "desconhecido"
        rank_solicitante = self._rank_servidor(solicitante)
        if rank_solicitante is None:
            rank_solicitante = -1

        if self._my_rank > rank_solicitante:
            resposta.status = contrato_pb2.STATUS_SUCESSO
            resposta.rank = self._my_rank
            logging.info(
                "Respondendo eleição OK para %s (solicitante rank=%s, meu rank=%s)",
                solicitante,
                rank_solicitante,
                self._my_rank,
            )
            if not self._is_coordenador():
                self._iniciar_eleicao_async(f"pedido de eleição recebido de {solicitante}")
            return resposta

        resposta.status = contrato_pb2.STATUS_ERRO
        resposta.erro_msg = "rank inferior"
        resposta.rank = self._my_rank
        logging.info(
            "Pedido de eleição de %s rejeitado (solicitante rank=%s, meu rank=%s)",
            solicitante,
            rank_solicitante,
            self._my_rank,
        )
        return resposta

    def _rank_servidor(self, nome_servidor: str) -> int | None:
        with self._lock:
            return self._active_servers.get(nome_servidor)


def main() -> None:
    servidor = Servidor()
    servidor.loop()


if __name__ == "__main__":
    main()
