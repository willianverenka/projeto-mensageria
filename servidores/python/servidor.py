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
SNAPSHOT_PORT = 5562
REQUEST_TIMEOUT_MS = 2000
TIMEOUT_TESTE_COORDENADOR = int(os.getenv("TIMEOUT_TESTE_COORDENADOR", "500"))
SNAPSHOT_TIMEOUT_MS = 10000
TOPICO_COORDENADOR = "servers"
TOPICO_REPLICA = "__replica__"
LOG_MODE = os.getenv("SERVER_LOG_MODE", "presentation").strip().lower() or "presentation"
ERRO_SERVIDOR_NAO_REGISTRADO = "servidor não registrado"

LOGINS_PATH = DATA_DIR / "logins.jsonl"
CANAIS_PATH = DATA_DIR / "canais.json"
PUBLICACOES_PATH = DATA_DIR / "publicacoes.jsonl"


def _log_verbose(message: str, *args: object) -> None:
    if LOG_MODE == "verbose":
        logging.info(message, *args)


def _log_eleicao(message: str, *args: object) -> None:
    logging.info("[ELEICAO] " + message, *args)


def _log_eleicao_verbose(message: str, *args: object) -> None:
    _log_verbose("[ELEICAO] " + message, *args)


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
        self.sub_socket.setsockopt(zmq.SUBSCRIBE, TOPICO_REPLICA.encode("utf-8"))
        self.relogio = RelogioProcesso()
        self.origem = origem_label("servidor")
        self.requests_processadas = 0
        self._lock = threading.RLock()
        self._storage_lock = threading.RLock()
        self._reference_lock = threading.Lock()
        self._coordinator_name = ""
        self._active_servers: dict[str, int] = {}
        self._known_server_ranks: dict[str, int] = {}
        self._my_rank = 0
        self._election_running = False
        _log_verbose("Servidor conectado ao orquestrador em %s", ORQ_ENDPOINT)
        _log_verbose("Servidor conectado ao proxy pub em %s", PROXY_PUB_ENDPOINT)
        _log_verbose("Servidor conectado ao proxy sub em %s", PROXY_SUB_ENDPOINT)
        _log_verbose("Servidor conectado à referência em %s", REFERENCE_ENDPOINT)
        self._init_storage()
        self._registrar_na_referencia()
        servidores = self._listar_servidores_referencia()
        self._atualizar_servidores_ativos(servidores)
        self._iniciar_servicos_internos()
        self._sincronizar_replicas()
        self._iniciar_eleicao_async("inicializacao")
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
        with self._storage_lock:
            try:
                canais = json.loads(CANAIS_PATH.read_text(encoding="utf-8"))
                return sorted({str(canal) for canal in canais if str(canal).strip()})
            except Exception:
                return []

    def _salvar_canais(self, canais: list[str]) -> None:
        with self._storage_lock:
            normalizados = sorted({canal.strip() for canal in canais if canal.strip()})
            CANAIS_PATH.write_text(json.dumps(normalizados, ensure_ascii=False), encoding="utf-8")

    def _adicionar_canal_idempotente(self, nome: str) -> bool:
        with self._storage_lock:
            canais = self._ler_canais()
            if nome in canais:
                return False
            canais.append(nome)
            self._salvar_canais(canais)
            return True

    def _ler_jsonl(self, path: Path) -> list[dict[str, str]]:
        if not path.exists():
            return []
        registros: list[dict[str, str]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, dict):
                registros.append({str(key): str(value) for key, value in item.items()})
        return registros

    def _salvar_jsonl(self, path: Path, registros: list[dict[str, str]]) -> None:
        texto = "".join(
            json.dumps(registro, ensure_ascii=False, separators=(",", ":")) + "\n"
            for registro in registros
        )
        path.write_text(texto, encoding="utf-8")

    def _registrar_login(self, nome_usuario: str, timestamp_iso: str) -> None:
        with self._storage_lock:
            registros = self._ler_jsonl(LOGINS_PATH)
            chave = (nome_usuario, timestamp_iso)
            if any((item.get("usuario"), item.get("timestamp")) == chave for item in registros):
                return
            registros.append({"usuario": nome_usuario, "timestamp": timestamp_iso})
            registros.sort(key=lambda item: (item.get("timestamp", ""), item.get("usuario", "")))
            self._salvar_jsonl(LOGINS_PATH, registros)

    def _registrar_publicacao(
        self, canal: str, mensagem: str, remetente: str, timestamp_iso: str
    ) -> None:
        with self._storage_lock:
            registros = self._ler_jsonl(PUBLICACOES_PATH)
            chave = (canal, mensagem, remetente, timestamp_iso)
            if any(
                (
                    item.get("canal"),
                    item.get("mensagem"),
                    item.get("remetente"),
                    item.get("timestamp"),
                )
                == chave
                for item in registros
            ):
                return
            registros.append(
                {
                    "canal": canal,
                    "mensagem": mensagem,
                    "remetente": remetente,
                    "timestamp": timestamp_iso,
                }
            )
            registros.sort(
                key=lambda item: (
                    item.get("timestamp", ""),
                    item.get("canal", ""),
                    item.get("remetente", ""),
                    item.get("mensagem", ""),
                )
            )
            self._salvar_jsonl(PUBLICACOES_PATH, registros)

    def _replica_origem(self) -> str:
        return f"replica:{SERVER_NAME}"

    def _timestamp_text_to_timestamp(self, texto: str):
        try:
            seconds_text, nanos_text = texto.split(".", 1)
            ns = int(seconds_text) * 1_000_000_000 + int(nanos_text[:9].ljust(9, "0"))
            return timestamp_from_ns(ns)
        except Exception:
            return timestamp_from_ns(0)

    def _novo_envelope_replicacao(self) -> contrato_pb2.Envelope:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self._replica_origem(), self.relogio))
        return env

    def _publicar_replicacao(self, env: contrato_pb2.Envelope) -> None:
        try:
            self.pub_socket.send_multipart(
                [TOPICO_REPLICA.encode("utf-8"), envelope_bytes(env)]
            )
            _log_verbose("Operação replicada em %s: %s", TOPICO_REPLICA, env.WhichOneof("conteudo"))
        except Exception as exc:
            logging.warning("Falha ao publicar réplica: %s", exc)

    def _replicar_login(self, req: contrato_pb2.LoginRequest) -> None:
        env = self._novo_envelope_replicacao()
        env.login_req.CopyFrom(req)
        self._publicar_replicacao(env)

    def _replicar_create_channel(self, req: contrato_pb2.CreateChannelRequest) -> None:
        env = self._novo_envelope_replicacao()
        env.create_channel_req.CopyFrom(req)
        self._publicar_replicacao(env)

    def _replicar_publicacao(
        self,
        canal: str,
        mensagem: str,
        remetente: str,
        cab_publicacao: contrato_pb2.Cabecalho,
    ) -> None:
        env = self._novo_envelope_replicacao()
        req = contrato_pb2.PublishRequest()
        req.cabecalho.CopyFrom(cab_publicacao)
        req.cabecalho.linguagem_origem = remetente
        req.canal = canal
        req.mensagem = mensagem
        env.publish_req.CopyFrom(req)
        self._publicar_replicacao(env)

    def _operacoes_snapshot(self) -> list[contrato_pb2.Envelope]:
        operacoes: list[contrato_pb2.Envelope] = []
        with self._storage_lock:
            for item in self._ler_jsonl(LOGINS_PATH):
                nome = item.get("usuario", "").strip()
                timestamp = item.get("timestamp", "").strip()
                if not nome:
                    continue
                env = self._novo_envelope_replicacao()
                req = contrato_pb2.LoginRequest()
                req.cabecalho.linguagem_origem = "snapshot"
                req.cabecalho.timestamp_envio.CopyFrom(self._timestamp_text_to_timestamp(timestamp))
                req.nome_usuario = nome
                env.login_req.CopyFrom(req)
                operacoes.append(env)

            for canal in self._ler_canais():
                env = self._novo_envelope_replicacao()
                req = contrato_pb2.CreateChannelRequest()
                req.cabecalho.linguagem_origem = "snapshot"
                req.cabecalho.timestamp_envio.CopyFrom(timestamp_from_ns(0))
                req.nome_canal = canal
                env.create_channel_req.CopyFrom(req)
                operacoes.append(env)

            for item in self._ler_jsonl(PUBLICACOES_PATH):
                canal = item.get("canal", "").strip()
                mensagem = item.get("mensagem", "").strip()
                remetente = item.get("remetente", "").strip() or "desconhecido"
                timestamp = item.get("timestamp", "").strip()
                if not canal or not mensagem:
                    continue
                env = self._novo_envelope_replicacao()
                req = contrato_pb2.PublishRequest()
                req.cabecalho.linguagem_origem = remetente
                req.cabecalho.timestamp_envio.CopyFrom(self._timestamp_text_to_timestamp(timestamp))
                req.canal = canal
                req.mensagem = mensagem
                env.publish_req.CopyFrom(req)
                operacoes.append(env)
        return operacoes

    def _aplicar_operacao_replicada(self, env: contrato_pb2.Envelope) -> None:
        tipo = env.WhichOneof("conteudo")
        if tipo == "login_req":
            req = env.login_req
            nome = req.nome_usuario.strip()
            if nome:
                self._registrar_login(nome, timestamp_to_text(req.cabecalho.timestamp_envio))
        elif tipo == "create_channel_req":
            nome = env.create_channel_req.nome_canal.strip()
            if nome:
                self._adicionar_canal_idempotente(nome)
        elif tipo == "publish_req":
            req = env.publish_req
            canal = req.canal.strip()
            mensagem = req.mensagem.strip()
            if not canal or not mensagem:
                return
            self._adicionar_canal_idempotente(canal)
            remetente = req.cabecalho.linguagem_origem or "desconhecido"
            self._registrar_publicacao(
                canal,
                mensagem,
                remetente,
                timestamp_to_text(req.cabecalho.timestamp_envio),
            )

    def _aplicar_operacoes_replicadas(self, operacoes: list[contrato_pb2.Envelope]) -> None:
        with self._storage_lock:
            canais = set(self._ler_canais())
            logins = self._ler_jsonl(LOGINS_PATH)
            logins_por_chave = {
                (item.get("usuario", ""), item.get("timestamp", "")): item
                for item in logins
            }
            publicacoes = self._ler_jsonl(PUBLICACOES_PATH)
            publicacoes_por_chave = {
                (
                    item.get("canal", ""),
                    item.get("mensagem", ""),
                    item.get("remetente", ""),
                    item.get("timestamp", ""),
                ): item
                for item in publicacoes
            }

            for env in operacoes:
                tipo = env.WhichOneof("conteudo")
                if tipo == "login_req":
                    req = env.login_req
                    nome = req.nome_usuario.strip()
                    timestamp = timestamp_to_text(req.cabecalho.timestamp_envio)
                    if nome:
                        logins_por_chave[(nome, timestamp)] = {
                            "usuario": nome,
                            "timestamp": timestamp,
                        }
                elif tipo == "create_channel_req":
                    nome = env.create_channel_req.nome_canal.strip()
                    if nome:
                        canais.add(nome)
                elif tipo == "publish_req":
                    req = env.publish_req
                    canal = req.canal.strip()
                    mensagem = req.mensagem.strip()
                    if not canal or not mensagem:
                        continue
                    canais.add(canal)
                    remetente = req.cabecalho.linguagem_origem or "desconhecido"
                    timestamp = timestamp_to_text(req.cabecalho.timestamp_envio)
                    publicacoes_por_chave[(canal, mensagem, remetente, timestamp)] = {
                        "canal": canal,
                        "mensagem": mensagem,
                        "remetente": remetente,
                        "timestamp": timestamp,
                    }

            self._salvar_canais(list(canais))
            logins_ordenados = sorted(
                logins_por_chave.values(),
                key=lambda item: (item.get("timestamp", ""), item.get("usuario", "")),
            )
            publicacoes_ordenadas = sorted(
                publicacoes_por_chave.values(),
                key=lambda item: (
                    item.get("timestamp", ""),
                    item.get("canal", ""),
                    item.get("remetente", ""),
                    item.get("mensagem", ""),
                ),
            )
            self._salvar_jsonl(LOGINS_PATH, logins_ordenados)
            self._salvar_jsonl(PUBLICACOES_PATH, publicacoes_ordenadas)

    def _resposta_snapshot_status(self, status: int, erro_msg: str = "") -> contrato_pb2.Envelope:
        cab = novo_cabecalho(self.origem, self.relogio)
        resp = contrato_pb2.HeartbeatResponse()
        resp.cabecalho.CopyFrom(cab)
        resp.status = status
        resp.erro_msg = erro_msg
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(cab)
        env.heartbeat_res.CopyFrom(resp)
        return env

    def _enviar_para_referencia(
        self, env: contrato_pb2.Envelope, atualizar_offset: bool = True
    ) -> tuple[contrato_pb2.Envelope, int, int]:
        tipo = env.WhichOneof("conteudo") or "desconhecido"
        _log_verbose("Enviando %s para referência %s", tipo, cabecalho_texto(env.cabecalho))
        envio_ns = time.time_ns()
        try:
            with self._reference_lock:
                self.reference_socket.send(envelope_bytes(env))
                data = self.reference_socket.recv()
        except zmq.Again as exc:
            raise TimeoutError("timeout ao comunicar com a referência") from exc
        recebimento_ns = time.time_ns()
        resp_env = envelope_from_bytes(data)
        self.relogio.on_receive(resp_env.cabecalho.relogio_logico)
        _log_verbose(
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

        rank_anterior = self._my_rank
        self._my_rank = res.rank
        if rank_anterior and rank_anterior != self._my_rank:
            _log_verbose(
                "Rank local atualizado de %s para %s após registro na referência",
                rank_anterior,
                self._my_rank,
            )
        self._atualizar_servidores_ativos(
            [contrato_pb2.ServerInfo(nome=SERVER_NAME, rank=self._my_rank)]
        )
        _log_verbose("Servidor %s registrado na referência com rank=%s", SERVER_NAME, res.rank)

    def _registrar_novamente_apos_heartbeat(self) -> bool:
        try:
            self._registrar_na_referencia()
            _log_verbose(
                "Heartbeat recuperado: servidor registrado novamente rank=%s",
                self._my_rank,
            )
            return True
        except Exception as exc:
            _log_verbose("Falha ao registrar novamente após heartbeat rejeitado: %s", exc)
            return False

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
        _log_verbose("Servidores disponíveis na referência: %s", resumo)
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
            _log_verbose("Resposta inesperada ao heartbeat")
            return

        res = resp_env.heartbeat_res
        if res.status != contrato_pb2.STATUS_SUCESSO:
            if res.erro_msg == ERRO_SERVIDOR_NAO_REGISTRADO:
                _log_verbose(
                    "Heartbeat rejeitado: servidor não registrado; registrando novamente"
                )
                if not self._registrar_novamente_apos_heartbeat():
                    return
            else:
                _log_verbose("Falha ao enviar heartbeat: %s", res.erro_msg)
                return
        else:
            _log_verbose("Heartbeat enviado com sucesso para %s", SERVER_NAME)

        self._listar_servidores_referencia()
        self._sincronizar_replicas()

    def loop(self) -> None:
        _log_verbose("Servidor iniciado. Aguardando mensagens...")
        while True:
            data = self.socket.recv()
            env = envelope_from_bytes(data)
            self.relogio.on_receive(env.cabecalho.relogio_logico)
            tipo = env.WhichOneof("conteudo")
            _log_verbose("Recebido %s %s", tipo, cabecalho_texto(env.cabecalho))

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

            _log_verbose("Enviando %s %s", resp_env.WhichOneof("conteudo"), cabecalho_texto(resp_env.cabecalho))
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
        self._replicar_login(req)
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

        if not self._adicionar_canal_idempotente(nome):
            res.status = contrato_pb2.STATUS_ERRO
            res.erro_msg = "canal já existe"
            return res

        self._replicar_create_channel(req)
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

        _log_verbose("Publicando em %s ts=%s relogio=%s", canal, timestamp_to_text(channel_msg.timestamp_envio), channel_msg.relogio_logico)
        self.pub_socket.send_multipart([canal.encode("utf-8"), channel_msg.SerializeToString()])
        self._registrar_publicacao(
            canal,
            mensagem,
            remetente,
            timestamp_to_text(channel_msg.timestamp_envio),
        )
        self._replicar_publicacao(canal, mensagem, remetente, cab_publicacao)

        res.status = contrato_pb2.STATUS_SUCESSO
        return res

    def _iniciar_servicos_internos(self) -> None:
        threading.Thread(target=self._loop_relogio, daemon=True).start()
        threading.Thread(target=self._loop_eleicao, daemon=True).start()
        threading.Thread(target=self._loop_snapshot, daemon=True).start()
        threading.Thread(target=self._loop_anuncios_coord, daemon=True).start()

    def _loop_snapshot(self) -> None:
        socket = self.context.socket(zmq.REP)
        socket.bind(f"tcp://*:{SNAPSHOT_PORT}")
        _log_verbose("Serviço de snapshot ouvindo em tcp://*:%s", SNAPSHOT_PORT)
        while True:
            try:
                data = socket.recv()
                env = envelope_from_bytes(data)
                self.relogio.on_receive(env.cabecalho.relogio_logico)
                if env.WhichOneof("conteudo") != "heartbeat_req":
                    resposta = self._resposta_snapshot_status(
                        contrato_pb2.STATUS_ERRO,
                        "tipo não suportado para snapshot",
                    )
                    socket.send_multipart([envelope_bytes(resposta)])
                    continue

                frames = [envelope_bytes(self._resposta_snapshot_status(contrato_pb2.STATUS_SUCESSO))]
                frames.extend(envelope_bytes(item) for item in self._operacoes_snapshot())
                socket.send_multipart(frames)
            except Exception as exc:
                logging.warning("Erro no serviço de snapshot: %s", exc)

    def _sincronizar_replicas(self) -> None:
        candidatos: list[str] = []
        coordenador = self._coordenador_atual()
        if coordenador and coordenador != SERVER_NAME:
            candidatos.append(coordenador)
        for item in self._servidores_ativos_snapshot():
            if item.nome != SERVER_NAME and item.nome not in candidatos:
                candidatos.append(item.nome)

        for nome in candidatos:
            self._solicitar_snapshot(nome)

    def _solicitar_snapshot(self, nome_servidor: str) -> bool:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        req = contrato_pb2.HeartbeatRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        req.nome_servidor = SERVER_NAME
        env.heartbeat_req.CopyFrom(req)

        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.RCVTIMEO, SNAPSHOT_TIMEOUT_MS)
        socket.setsockopt(zmq.SNDTIMEO, SNAPSHOT_TIMEOUT_MS)
        socket.setsockopt(zmq.LINGER, 0)
        socket.connect(f"tcp://{nome_servidor}:{SNAPSHOT_PORT}")
        try:
            socket.send(envelope_bytes(env))
            frames = socket.recv_multipart()
        except zmq.Again:
            return False
        except Exception as exc:
            logging.warning("Falha ao pedir snapshot de %s: %s", nome_servidor, exc)
            return False
        finally:
            socket.close(0)

        if not frames:
            return False
        status_env = envelope_from_bytes(frames[0])
        self.relogio.on_receive(status_env.cabecalho.relogio_logico)
        if (
            status_env.WhichOneof("conteudo") != "heartbeat_res"
            or status_env.heartbeat_res.status != contrato_pb2.STATUS_SUCESSO
        ):
            return False

        operacoes: list[contrato_pb2.Envelope] = []
        for frame in frames[1:]:
            try:
                op = envelope_from_bytes(frame)
                self.relogio.on_receive(op.cabecalho.relogio_logico)
                operacoes.append(op)
            except Exception as exc:
                logging.warning("Falha ao aplicar operação de snapshot de %s: %s", nome_servidor, exc)
        if operacoes:
            self._aplicar_operacoes_replicadas(operacoes)
        aplicadas = len(operacoes)
        if aplicadas:
            _log_verbose("Snapshot aplicado de %s com %s operações", nome_servidor, aplicadas)
        return True

    def _loop_relogio(self) -> None:
        socket = self.context.socket(zmq.REP)
        socket.bind(f"tcp://*:{CLOCK_PORT}")
        _log_verbose("Serviço de relógio interno ouvindo em tcp://*:%s", CLOCK_PORT)
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
        _log_verbose("Serviço de eleição interno ouvindo em tcp://*:%s", ELECTION_PORT)
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
        _log_verbose(
            "Escutando tópicos internos '%s' e '%s'",
            TOPICO_COORDENADOR,
            TOPICO_REPLICA,
        )
        while True:
            try:
                topico, payload = self.sub_socket.recv_multipart()
                topico_texto = topico.decode("utf-8")

                if topico_texto == TOPICO_REPLICA:
                    env = envelope_from_bytes(payload)
                    if env.cabecalho.linguagem_origem == self._replica_origem():
                        continue
                    self.relogio.on_receive(env.cabecalho.relogio_logico)
                    self._aplicar_operacao_replicada(env)
                    _log_verbose("Operação replicada recebida: %s", env.WhichOneof("conteudo"))
                    continue

                if topico_texto != TOPICO_COORDENADOR:
                    continue

                msg = contrato_pb2.ChannelMessage()
                msg.ParseFromString(payload)
                self.relogio.on_receive(msg.relogio_logico)

                coordenador = msg.mensagem.strip()
                if not coordenador:
                    continue

                remetente = msg.remetente.strip()
                if remetente == SERVER_NAME and coordenador == SERVER_NAME:
                    continue

                self._processar_anuncio_coordenador(coordenador, remetente)
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
            for item in servidores:
                self._known_server_ranks[item.nome] = item.rank
            if servidores:
                self._active_servers = {item.nome: item.rank for item in servidores}
            elif not self._active_servers and self._my_rank:
                self._active_servers = {SERVER_NAME: self._my_rank}
            if self._my_rank:
                self._active_servers[SERVER_NAME] = self._my_rank
                self._known_server_ranks[SERVER_NAME] = self._my_rank

        _log_verbose(
            "Lista ativa local atualizada: %s",
            self._resumo_servidores(self._servidores_ativos_snapshot()),
        )

    def _restaurar_servidor_ativo_local(self, nome: str) -> None:
        with self._lock:
            rank = self._active_servers.get(nome) or self._known_server_ranks.get(nome)
            if rank is None:
                _log_verbose(
                    "Coordenador %s respondeu ao teste direto, mas rank local e desconhecido",
                    nome,
                )
                return
            self._active_servers[nome] = rank

        _log_verbose(
            "Coordenador %s respondeu ao teste direto; restaurado na lista ativa local rank=%s",
            nome,
            rank,
        )

    def _maior_servidor_ativo(self) -> str:
        servidores = self._servidores_ativos_snapshot()
        if not servidores:
            return SERVER_NAME
        return max(servidores, key=lambda item: item.rank).nome

    def _coordenador_atual(self) -> str:
        with self._lock:
            return self._coordinator_name

    def _set_coordenador(self, nome: str) -> str:
        with self._lock:
            antigo = self._coordinator_name
            self._coordinator_name = nome
            return antigo

    def _avaliar_eleicao_apos_atualizacao(self, motivo: str) -> None:
        servidores = self._servidores_ativos_snapshot()
        if not servidores:
            return

        maior = max(servidores, key=lambda item: item.rank)
        coordenador = self._coordenador_atual()
        rank_coordenador = self._rank_servidor(coordenador) if coordenador else None
        if not coordenador or rank_coordenador is None or maior.rank > rank_coordenador:
            self._iniciar_eleicao_async(motivo)

    def _is_coordenador(self) -> bool:
        return self._coordenador_atual() == SERVER_NAME

    def _iniciar_eleicao_async(self, motivo: str) -> None:
        with self._lock:
            if self._election_running:
                _log_verbose("Eleição já em andamento; ignorando novo gatilho (%s)", motivo)
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
        _log_eleicao("inicio motivo=%s", motivo)
        servidores = self._listar_servidores_referencia()
        self._atualizar_servidores_ativos(servidores)
        servidores = self._servidores_ativos_snapshot()
        superiores = [
            item
            for item in servidores
            if item.rank > self._my_rank and item.nome != SERVER_NAME
        ]

        if not superiores:
            self._tornar_coordenador("sem_servidor_maior")
            return

        recebeu_ok = False
        for item in sorted(superiores, key=lambda server: server.rank, reverse=True):
            if self._consultar_eleicao_servidor(item.nome):
                recebeu_ok = True

        if not recebeu_ok:
            self._tornar_coordenador("sem_resposta_de_servidor_maior")
            return

        _log_eleicao("delegada motivo=%s", motivo)

    def _tornar_coordenador(self, motivo: str) -> None:
        self._set_coordenador(SERVER_NAME)
        _log_eleicao(
            "eleito coordenador=%s rank=%s motivo=%s",
            SERVER_NAME,
            self._my_rank,
            motivo,
        )
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
            _log_eleicao("anuncio publicado coordenador=%s rank=%s", SERVER_NAME, self._my_rank)
        except Exception as exc:
            logging.warning("Falha ao anunciar coordenador: %s", exc)

    def _processar_anuncio_coordenador(self, coordenador: str, remetente: str) -> None:
        self._listar_servidores_referencia()
        rank_anunciado = self._rank_servidor(coordenador)
        if coordenador == SERVER_NAME:
            rank_anunciado = self._my_rank
        if rank_anunciado is None:
            _log_eleicao_verbose(
                "anuncio ignorado coordenador=%s rank=desconhecido motivo=servidor_desconhecido",
                coordenador,
            )
            return

        atual = self._coordenador_atual()
        rank_atual = self._rank_servidor(atual) if atual else None
        aceitar = False
        motivo = "sem_coordenador"
        if not atual or atual == coordenador:
            aceitar = True
            motivo = "sem_coordenador" if not atual else "mesmo_coordenador"
        elif rank_atual is None:
            aceitar = True
            motivo = "coordenador_atual_desconhecido"
        elif rank_anunciado >= rank_atual:
            aceitar = True
            motivo = "rank_maior_ou_igual"
        elif atual != SERVER_NAME and not self._consultar_eleicao_servidor(atual, registrar_logs=False):
            aceitar = True
            motivo = "coordenador_atual_indisponivel"

        if aceitar:
            self._set_coordenador(coordenador)
            _log_eleicao(
                "anuncio aceito coordenador=%s rank=%s origem=%s motivo=%s",
                coordenador,
                rank_anunciado,
                remetente or "desconhecido",
                motivo,
            )
            return

        _log_eleicao_verbose(
            "anuncio ignorado coordenador=%s rank=%s motivo=rank_menor_que_atual atual=%s",
            coordenador,
            rank_anunciado,
            atual,
        )

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
            _log_verbose("Falha ao comunicar com %s:%s: %s", nome_servidor, porta, exc)
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

        _log_verbose(
            "Enviando heartbeat_req para %s %s",
            nome_servidor,
            cabecalho_texto(env.cabecalho),
        )
        resp_env, _, _ = self._enviar_para_servidor(nome_servidor, CLOCK_PORT, env)
        if resp_env is None:
            _log_verbose("Sem resposta de relógio de %s", nome_servidor)
            return None
        if resp_env.WhichOneof("conteudo") != "heartbeat_res":
            _log_verbose("Resposta inesperada de relógio de %s", nome_servidor)
            return None
        if resp_env.heartbeat_res.status != contrato_pb2.STATUS_SUCESSO:
            _log_verbose(
                "Servidor %s rejeitou pedido de relógio: %s",
                nome_servidor,
                resp_env.heartbeat_res.erro_msg,
            )
            return None
        _log_verbose(
            "Recebido heartbeat_res de %s %s",
            nome_servidor,
            cabecalho_texto(resp_env.cabecalho),
        )
        return resp_env

    def _consultar_eleicao_servidor(self, nome_servidor: str, registrar_logs: bool = True) -> bool:
        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        req = contrato_pb2.RegisterServerRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        req.nome_servidor = SERVER_NAME
        env.register_server_req.CopyFrom(req)

        if registrar_logs:
            _log_eleicao_verbose("req destino=%s", nome_servidor)
        resp_env, _, _ = self._enviar_para_servidor(
            nome_servidor, ELECTION_PORT, env, timeout_ms=REQUEST_TIMEOUT_MS
        )
        if resp_env is None:
            if registrar_logs:
                _log_eleicao_verbose("sem_resposta destino=%s", nome_servidor)
            return False
        if resp_env.WhichOneof("conteudo") != "register_server_res":
            if registrar_logs:
                _log_eleicao_verbose("resposta_invalida destino=%s", nome_servidor)
            return False
        if resp_env.register_server_res.status != contrato_pb2.STATUS_SUCESSO:
            if registrar_logs:
                _log_eleicao_verbose(
                    "rejeitada destino=%s motivo=%s",
                    nome_servidor,
                    resp_env.register_server_res.erro_msg,
                )
            return False

        if registrar_logs:
            _log_eleicao_verbose(
                "ok origem=%s rank=%s",
                nome_servidor,
                resp_env.register_server_res.rank,
            )
        return True

    def _sincronizar_como_seguidor(
        self, coordenador: str, timeout_ms: int = REQUEST_TIMEOUT_MS
    ) -> bool:
        if not coordenador:
            return False

        env = contrato_pb2.Envelope()
        env.cabecalho.CopyFrom(novo_cabecalho(self.origem, self.relogio))
        req = contrato_pb2.HeartbeatRequest()
        req.cabecalho.CopyFrom(env.cabecalho)
        req.nome_servidor = SERVER_NAME
        env.heartbeat_req.CopyFrom(req)

        resp_env, envio_ns, recebimento_ns = self._enviar_para_servidor(
            coordenador, CLOCK_PORT, env, timeout_ms=timeout_ms
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
        _log_verbose(
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
        _log_verbose(
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
            self._iniciar_eleicao_async("coordenador_desconhecido")
            return

        if self._is_coordenador():
            self._sincronizar_como_coordenador()
            return

        ativos = {item.nome for item in self._servidores_ativos_snapshot()}
        if coordenador and coordenador not in ativos:
            _log_verbose(
                "Coordenador %s ausente da lista da referencia; testando diretamente com timeout=%sms",
                coordenador,
                TIMEOUT_TESTE_COORDENADOR,
            )
            if self._sincronizar_como_seguidor(
                coordenador, timeout_ms=TIMEOUT_TESTE_COORDENADOR
            ):
                self._restaurar_servidor_ativo_local(coordenador)
                return
            self._iniciar_eleicao_async("coordenador_indisponivel_apos_teste")
            return

        if coordenador and self._sincronizar_como_seguidor(coordenador):
            return

        self._iniciar_eleicao_async(f"falha_sincronizar_com_{coordenador or 'desconhecido'}")

    def _processar_pedido_relogio(
        self, req: contrato_pb2.HeartbeatRequest
    ) -> contrato_pb2.HeartbeatResponse:
        resposta = contrato_pb2.HeartbeatResponse()
        solicitante = req.nome_servidor.strip() or "desconhecido"
        coordenador = self._coordenador_atual()

        if self._is_coordenador():
            resposta.status = contrato_pb2.STATUS_SUCESSO
            _log_verbose("Respondendo relógio ao servidor %s como coordenador", solicitante)
            return resposta

        if solicitante == coordenador and coordenador:
            resposta.status = contrato_pb2.STATUS_SUCESSO
            _log_verbose("Respondendo relógio ao coordenador %s", solicitante)
            return resposta

        resposta.status = contrato_pb2.STATUS_ERRO
        resposta.erro_msg = "nao sou coordenador"
        _log_verbose(
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
            _log_eleicao_verbose(
                "ok enviado destino=%s rank=%s solicitante_rank=%s",
                solicitante,
                self._my_rank,
                rank_solicitante,
            )
            if self._is_coordenador():
                self._anunciar_coordenador()
            else:
                self._iniciar_eleicao_async(f"pedido de eleição recebido de {solicitante}")
            return resposta

        resposta.status = contrato_pb2.STATUS_ERRO
        resposta.erro_msg = "rank inferior"
        resposta.rank = self._my_rank
        _log_eleicao_verbose(
            "anuncio ignorado coordenador=%s rank=%s motivo=rank_inferior solicitante=%s",
            SERVER_NAME,
            self._my_rank,
            solicitante,
        )
        return resposta

    def _rank_servidor(self, nome_servidor: str) -> int | None:
        with self._lock:
            return self._active_servers.get(nome_servidor) or self._known_server_ranks.get(
                nome_servidor
            )


def main() -> None:
    servidor = Servidor()
    servidor.loop()


if __name__ == "__main__":
    main()
