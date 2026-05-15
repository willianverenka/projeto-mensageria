package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"projeto-mensageria/shared/go/mensageria"
	"projeto-mensageria/shared/go/protos"

	"github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	orqEndpointDefault        = "tcp://orquestrador:5556"
	proxyPubEndpoint          = "tcp://proxy:5557"
	proxySubEndpoint          = "tcp://proxy:5558"
	referenceEndpoint         = "tcp://referencia:5559"
	dataDirDefault            = "/data"
	clockPort                 = 5560
	electionPort              = 5561
	snapshotPort              = 5562
	requestTimeout            = 2 * time.Second
	snapshotTimeout           = 10 * time.Second
	topicoCoordenador         = "servers"
	topicoReplica             = "__replica__"
	erroServidorNaoRegistrado = "servidor não registrado"
)

var (
	logMode                 = strings.ToLower(strings.TrimSpace(getEnv("SERVER_LOG_MODE", "presentation")))
	timeoutTesteCoordenador = time.Duration(getEnvInt("TIMEOUT_TESTE_COORDENADOR", 500)) * time.Millisecond
)

func init() {
	if logMode == "" {
		logMode = "presentation"
	}
}

func logVerbose(format string, args ...any) {
	if logMode == "verbose" {
		log.Printf(format, args...)
	}
}

func logEleicao(format string, args ...any) {
	log.Printf("[ELEICAO] "+format, args...)
}

func logEleicaoVerbose(format string, args ...any) {
	logVerbose("[ELEICAO] "+format, args...)
}

func main() {
	orqEndpoint := getEnv("ORQ_ENDPOINT_SERVIDOR", orqEndpointDefault)
	pubEndpoint := getEnv("PROXY_PUB_ENDPOINT", proxyPubEndpoint)
	refEndpoint := getEnv("REFERENCE_ENDPOINT", referenceEndpoint)
	dataDir := getEnv("DATA_DIR", dataDirDefault)
	serverName := getEnv("SERVER_NAME", "servidor_go")

	sock, err := zmq4.NewSocket(zmq4.DEALER)
	if err != nil {
		log.Fatalf("Novo socket: %v", err)
	}
	defer sock.Close()

	if err := sock.Connect(orqEndpoint); err != nil {
		log.Fatalf("Conectar ao orquestrador: %v", err)
	}
	logVerbose("[SERVIDOR] Conectado ao orquestrador em %s", orqEndpoint)

	server := &Servidor{
		sock:              sock,
		pubSock:           mustNewPubSocket(pubEndpoint),
		announceSock:      mustNewPubSocket(pubEndpoint),
		refSock:           mustNewReqSocket(refEndpoint),
		subSock:           mustNewSubSocket(proxySubEndpoint, topicoCoordenador, topicoReplica),
		dataDir:           dataDir,
		pubEndpoint:       pubEndpoint,
		referenceEndpoint: refEndpoint,
		serverName:        serverName,
		clock:             &mensageria.RelogioProcesso{},
		origem:            mensageria.OrigemLabel(mensageria.RoleServidor),
		activeServers:     map[string]int32{},
		knownRanks:        map[string]int32{},
	}
	defer server.pubSock.Close()
	defer server.announceSock.Close()
	defer server.refSock.Close()
	defer server.subSock.Close()
	logVerbose("[SERVIDOR] Conectado ao proxy pub em %s", pubEndpoint)
	logVerbose("[SERVIDOR] Conectado ao proxy sub em %s", proxySubEndpoint)
	logVerbose("[SERVIDOR] Conectado à referência em %s", refEndpoint)
	server.initStorage()
	server.registrarNaReferencia()
	servidores := server.listarServidoresReferencia()
	server.atualizarServidoresAtivos(servidores)
	server.iniciarServicosInternos()
	server.sincronizarReplicas()
	server.iniciarEleicaoAsync("inicializacao")
	log.Printf("[SERVIDOR] Servidor pronto. Rank local=%d coordenador=%s", server.myRank, server.coordenadorAtual())
	server.loop()
}

type Servidor struct {
	sock              *zmq4.Socket
	pubSock           *zmq4.Socket
	refSock           *zmq4.Socket
	dataDir           string
	pubEndpoint       string
	referenceEndpoint string
	serverName        string
	clock             *mensageria.RelogioProcesso
	origem            string
	requestsCount     int
	announceSock      *zmq4.Socket
	subSock           *zmq4.Socket
	stateMu           sync.RWMutex
	storageMu         sync.Mutex
	refMu             sync.Mutex
	coordenador       string
	activeServers     map[string]int32
	knownRanks        map[string]int32
	myRank            int32
	electionRunning   bool
}

func (s *Servidor) loginsPath() string { return filepath.Join(s.dataDir, "logins.jsonl") }
func (s *Servidor) canaisPath() string { return filepath.Join(s.dataDir, "canais.json") }
func (s *Servidor) publicacoesPath() string {
	return filepath.Join(s.dataDir, "publicacoes.jsonl")
}

func mustNewPubSocket(endpoint string) *zmq4.Socket {
	sock, err := zmq4.NewSocket(zmq4.PUB)
	if err != nil {
		log.Fatalf("Novo socket PUB: %v", err)
	}
	if err := sock.Connect(endpoint); err != nil {
		log.Fatalf("Conectar PUB ao proxy: %v", err)
	}
	return sock
}

func mustNewReqSocket(endpoint string) *zmq4.Socket {
	sock, err := zmq4.NewSocket(zmq4.REQ)
	if err != nil {
		log.Fatalf("Novo socket REQ: %v", err)
	}
	_ = sock.SetRcvtimeo(requestTimeout)
	_ = sock.SetSndtimeo(requestTimeout)
	_ = sock.SetLinger(0)
	if err := sock.Connect(endpoint); err != nil {
		log.Fatalf("Conectar REQ à referência: %v", err)
	}
	return sock
}

func mustNewReqSocketWithTimeout(endpoint string, timeout time.Duration) *zmq4.Socket {
	sock, err := zmq4.NewSocket(zmq4.REQ)
	if err != nil {
		log.Fatalf("Novo socket REQ: %v", err)
	}
	_ = sock.SetRcvtimeo(timeout)
	_ = sock.SetSndtimeo(timeout)
	_ = sock.SetLinger(0)
	if err := sock.Connect(endpoint); err != nil {
		log.Fatalf("Conectar REQ a %s: %v", endpoint, err)
	}
	return sock
}

func mustNewSubSocket(endpoint string, topics ...string) *zmq4.Socket {
	sock, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		log.Fatalf("Novo socket SUB: %v", err)
	}
	if err := sock.Connect(endpoint); err != nil {
		log.Fatalf("Conectar SUB ao proxy: %v", err)
	}
	for _, topic := range topics {
		if err := sock.SetSubscribe(topic); err != nil {
			log.Fatalf("Inscrever no tópico %s: %v", topic, err)
		}
	}
	return sock
}

func getEnv(name, defaultVal string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt(name string, defaultVal int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return n
}

func conteudoNome(env *protos.Envelope) string {
	switch env.GetConteudo().(type) {
	case *protos.Envelope_LoginReq:
		return "login_req"
	case *protos.Envelope_LoginRes:
		return "login_res"
	case *protos.Envelope_CreateChannelReq:
		return "create_channel_req"
	case *protos.Envelope_CreateChannelRes:
		return "create_channel_res"
	case *protos.Envelope_ListChannelsReq:
		return "list_channels_req"
	case *protos.Envelope_ListChannelsRes:
		return "list_channels_res"
	case *protos.Envelope_PublishReq:
		return "publish_req"
	case *protos.Envelope_PublishRes:
		return "publish_res"
	case *protos.Envelope_RegisterServerReq:
		return "register_server_req"
	case *protos.Envelope_RegisterServerRes:
		return "register_server_res"
	case *protos.Envelope_ListServersReq:
		return "list_servers_req"
	case *protos.Envelope_ListServersRes:
		return "list_servers_res"
	case *protos.Envelope_HeartbeatReq:
		return "heartbeat_req"
	case *protos.Envelope_HeartbeatRes:
		return "heartbeat_res"
	default:
		return "desconhecido"
	}
}

func (s *Servidor) initStorage() {
	if err := os.MkdirAll(s.dataDir, 0755); err != nil {
		log.Fatalf("Criar DATA_DIR: %v", err)
	}
	for _, p := range []string{s.loginsPath(), s.canaisPath(), s.publicacoesPath()} {
		if _, err := os.Stat(p); os.IsNotExist(err) {
			if p == s.canaisPath() {
				_ = os.WriteFile(p, []byte("[]"), 0644)
			} else {
				_ = os.WriteFile(p, nil, 0644)
			}
		}
	}
}

func (s *Servidor) registrarNaReferencia() {
	if err := s.registrarNaReferenciaSeguro(); err != nil {
		log.Fatalf("[SERVIDOR] %v", err)
	}
}

func (s *Servidor) registrarNaReferenciaSeguro() error {
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	req := &protos.RegisterServerRequest{Cabecalho: cab, NomeServidor: s.serverName}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_RegisterServerReq{RegisterServerReq: req},
	}
	resp, err := s.enviarParaReferencia(env, true)
	if err != nil {
		return fmt.Errorf("erro ao registrar na referência: %w", err)
	}
	res, ok := resp.GetConteudo().(*protos.Envelope_RegisterServerRes)
	if !ok {
		return fmt.Errorf("resposta inesperada ao registrar na referência: %T", resp.GetConteudo())
	}
	if res.RegisterServerRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		return fmt.Errorf("falha ao registrar na referência: %s", res.RegisterServerRes.GetErroMsg())
	}
	rankAnterior := s.myRank
	s.myRank = res.RegisterServerRes.GetRank()
	if rankAnterior > 0 && rankAnterior != s.myRank {
		logVerbose("[SERVIDOR] Rank local atualizado de %d para %d após registro na referência", rankAnterior, s.myRank)
	}
	s.atualizarServidoresAtivos([]*protos.ServerInfo{{Nome: s.serverName, Rank: s.myRank}})
	logVerbose("[SERVIDOR] Servidor %s registrado na referência com rank=%d", s.serverName, res.RegisterServerRes.GetRank())
	return nil
}

func (s *Servidor) registrarNovamenteAposHeartbeat() bool {
	if err := s.registrarNaReferenciaSeguro(); err != nil {
		logVerbose("[SERVIDOR] Falha ao registrar novamente após heartbeat rejeitado: %v", err)
		return false
	}
	logVerbose("[SERVIDOR] Heartbeat recuperado: servidor registrado novamente rank=%d", s.myRank)
	return true
}

func (s *Servidor) listarServidoresReferencia() []*protos.ServerInfo {
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	req := &protos.ListServersRequest{Cabecalho: cab}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_ListServersReq{ListServersReq: req},
	}
	resp, err := s.enviarParaReferencia(env, true)
	if err != nil {
		log.Printf("[SERVIDOR] Erro ao listar servidores na referência: %v", err)
		return nil
	}
	res, ok := resp.GetConteudo().(*protos.Envelope_ListServersRes)
	if !ok {
		log.Printf("[SERVIDOR] Resposta inesperada ao listar servidores: %T", resp.GetConteudo())
		return nil
	}
	servidores := append([]*protos.ServerInfo(nil), res.ListServersRes.GetServidores()...)
	sort.Slice(servidores, func(i, j int) bool {
		return servidores[i].GetRank() < servidores[j].GetRank()
	})
	partes := make([]string, 0, len(servidores))
	for _, servidor := range servidores {
		partes = append(partes, fmt.Sprintf("%s(rank=%d)", servidor.GetNome(), servidor.GetRank()))
	}
	if len(partes) == 0 {
		partes = append(partes, "(nenhum)")
	}
	logVerbose("[SERVIDOR] Servidores disponíveis na referência: %s", strings.Join(partes, ", "))
	s.atualizarServidoresAtivos(servidores)
	return servidores
}

func (s *Servidor) heartbeatESincronizar() {
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	req := &protos.HeartbeatRequest{Cabecalho: cab, NomeServidor: s.serverName}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_HeartbeatReq{HeartbeatReq: req},
	}
	resp, err := s.enviarParaReferencia(env, true)
	if err != nil {
		logVerbose("[SERVIDOR] Erro ao enviar heartbeat: %v", err)
		return
	}
	res, ok := resp.GetConteudo().(*protos.Envelope_HeartbeatRes)
	if !ok {
		logVerbose("[SERVIDOR] Resposta inesperada ao heartbeat: %T", resp.GetConteudo())
		return
	}
	if res.HeartbeatRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		if res.HeartbeatRes.GetErroMsg() == erroServidorNaoRegistrado {
			logVerbose("[SERVIDOR] Heartbeat rejeitado: servidor não registrado; registrando novamente")
			if !s.registrarNovamenteAposHeartbeat() {
				return
			}
		} else {
			logVerbose("[SERVIDOR] Heartbeat rejeitado: %s", res.HeartbeatRes.GetErroMsg())
			return
		}
	} else {
		logVerbose("[SERVIDOR] Heartbeat enviado com sucesso para %s", s.serverName)
	}
	s.listarServidoresReferencia()
	s.sincronizarReplicas()
}

func (s *Servidor) enviarParaReferencia(env *protos.Envelope, atualizarOffset bool) (*protos.Envelope, error) {
	data, err := mensageria.EnvelopeBytes(env)
	if err != nil {
		return nil, err
	}
	logVerbose("[SERVIDOR] Enviando %s para referência %s", conteudoNome(env), mensageria.CabecalhoTexto(env.GetCabecalho()))
	envioNs := time.Now().UnixNano()
	s.refMu.Lock()
	defer s.refMu.Unlock()
	if _, err := s.refSock.SendBytes(data, 0); err != nil {
		return nil, err
	}
	reply, err := s.refSock.RecvBytes(0)
	if err != nil {
		return nil, err
	}
	recebimentoNs := time.Now().UnixNano()
	resp, err := mensageria.EnvelopeFromBytes(reply)
	if err != nil {
		return nil, err
	}
	s.clock.OnReceive(resp.GetCabecalho().GetRelogioLogico())
	logVerbose("[SERVIDOR] Recebido %s da referência %s", conteudoNome(resp), mensageria.CabecalhoTexto(resp.GetCabecalho()))
	_ = envioNs
	_ = recebimentoNs
	return resp, nil
}

func (s *Servidor) lerCanais() []string {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	return s.lerCanaisSemLock()
}

func (s *Servidor) lerCanaisSemLock() []string {
	data, err := os.ReadFile(s.canaisPath())
	if err != nil {
		return nil
	}
	var canais []string
	if err := json.Unmarshal(data, &canais); err != nil {
		return nil
	}
	canais = normalizarCanais(canais)
	return canais
}

func (s *Servidor) salvarCanais(canais []string) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	s.salvarCanaisSemLock(canais)
}

func (s *Servidor) salvarCanaisSemLock(canais []string) {
	canais = normalizarCanais(canais)
	data, _ := json.Marshal(canais)
	_ = os.WriteFile(s.canaisPath(), data, 0644)
}

func normalizarCanais(canais []string) []string {
	seen := map[string]bool{}
	normalizados := make([]string, 0, len(canais))
	for _, canal := range canais {
		canal = strings.TrimSpace(canal)
		if canal == "" || seen[canal] {
			continue
		}
		seen[canal] = true
		normalizados = append(normalizados, canal)
	}
	sort.Strings(normalizados)
	return normalizados
}

func (s *Servidor) adicionarCanalIdempotente(nome string) bool {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	canais := s.lerCanaisSemLock()
	for _, canal := range canais {
		if canal == nome {
			return false
		}
	}
	canais = append(canais, nome)
	s.salvarCanaisSemLock(canais)
	return true
}

func (s *Servidor) lerJSONL(path string) []map[string]string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	lines := strings.Split(string(data), "\n")
	registros := make([]map[string]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var item map[string]string
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			continue
		}
		registros = append(registros, item)
	}
	return registros
}

func (s *Servidor) salvarJSONL(path string, registros []map[string]string) {
	var builder strings.Builder
	for _, registro := range registros {
		line, err := json.Marshal(registro)
		if err != nil {
			continue
		}
		builder.Write(line)
		builder.WriteByte('\n')
	}
	_ = os.WriteFile(path, []byte(builder.String()), 0644)
}

func (s *Servidor) registrarLogin(nomeUsuario, timestampISO string) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	registros := s.lerJSONL(s.loginsPath())
	for _, item := range registros {
		if item["usuario"] == nomeUsuario && item["timestamp"] == timestampISO {
			return
		}
	}
	registros = append(registros, map[string]string{"usuario": nomeUsuario, "timestamp": timestampISO})
	sort.Slice(registros, func(i, j int) bool {
		if registros[i]["timestamp"] == registros[j]["timestamp"] {
			return registros[i]["usuario"] < registros[j]["usuario"]
		}
		return registros[i]["timestamp"] < registros[j]["timestamp"]
	})
	s.salvarJSONL(s.loginsPath(), registros)
}

func (s *Servidor) registrarPublicacao(canal, mensagem, remetente, timestampISO string) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()
	registros := s.lerJSONL(s.publicacoesPath())
	for _, item := range registros {
		if item["canal"] == canal &&
			item["mensagem"] == mensagem &&
			item["remetente"] == remetente &&
			item["timestamp"] == timestampISO {
			return
		}
	}
	registros = append(registros, map[string]string{
		"canal":     canal,
		"mensagem":  mensagem,
		"remetente": remetente,
		"timestamp": timestampISO,
	})
	sort.Slice(registros, func(i, j int) bool {
		left := registros[i]
		right := registros[j]
		if left["timestamp"] != right["timestamp"] {
			return left["timestamp"] < right["timestamp"]
		}
		if left["canal"] != right["canal"] {
			return left["canal"] < right["canal"]
		}
		if left["remetente"] != right["remetente"] {
			return left["remetente"] < right["remetente"]
		}
		return left["mensagem"] < right["mensagem"]
	})
	s.salvarJSONL(s.publicacoesPath(), registros)
}

func (s *Servidor) replicaOrigem() string {
	return "replica:" + s.serverName
}

func timestampTextoParaTimestamp(texto string) *timestamppb.Timestamp {
	partes := strings.SplitN(texto, ".", 2)
	if len(partes) != 2 {
		return timestamppb.New(time.Unix(0, 0).UTC())
	}
	var segundos int64
	var nanos int64
	_, _ = fmt.Sscanf(partes[0], "%d", &segundos)
	nanosTexto := partes[1]
	if len(nanosTexto) > 9 {
		nanosTexto = nanosTexto[:9]
	}
	for len(nanosTexto) < 9 {
		nanosTexto += "0"
	}
	_, _ = fmt.Sscanf(nanosTexto, "%d", &nanos)
	return timestamppb.New(time.Unix(segundos, nanos).UTC())
}

func (s *Servidor) novoEnvelopeReplicacao() *protos.Envelope {
	return &protos.Envelope{Cabecalho: mensageria.NovoCabecalho(s.replicaOrigem(), s.clock)}
}

func (s *Servidor) publicarReplicacao(env *protos.Envelope) {
	data, err := proto.Marshal(env)
	if err != nil {
		log.Printf("[SERVIDOR] Falha ao serializar réplica: %v", err)
		return
	}
	if _, err := s.pubSock.SendMessage([]byte(topicoReplica), data); err != nil {
		log.Printf("[SERVIDOR] Falha ao publicar réplica: %v", err)
	}
}

func (s *Servidor) replicarLogin(req *protos.LoginRequest) {
	env := s.novoEnvelopeReplicacao()
	env.Conteudo = &protos.Envelope_LoginReq{LoginReq: req}
	s.publicarReplicacao(env)
}

func (s *Servidor) replicarCreateChannel(req *protos.CreateChannelRequest) {
	env := s.novoEnvelopeReplicacao()
	env.Conteudo = &protos.Envelope_CreateChannelReq{CreateChannelReq: req}
	s.publicarReplicacao(env)
}

func (s *Servidor) replicarPublicacao(canal, mensagem, remetente string, cabPublicacao *protos.Cabecalho) {
	env := s.novoEnvelopeReplicacao()
	cab := proto.Clone(cabPublicacao).(*protos.Cabecalho)
	cab.LinguagemOrigem = remetente
	req := &protos.PublishRequest{
		Cabecalho: cab,
		Canal:     canal,
		Mensagem:  mensagem,
	}
	env.Conteudo = &protos.Envelope_PublishReq{PublishReq: req}
	s.publicarReplicacao(env)
}

func (s *Servidor) operacoesSnapshot() []*protos.Envelope {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()

	operacoes := []*protos.Envelope{}
	for _, item := range s.lerJSONL(s.loginsPath()) {
		nome := strings.TrimSpace(item["usuario"])
		if nome == "" {
			continue
		}
		env := s.novoEnvelopeReplicacao()
		cab := &protos.Cabecalho{
			LinguagemOrigem: "snapshot",
			TimestampEnvio:  timestampTextoParaTimestamp(item["timestamp"]),
		}
		env.Conteudo = &protos.Envelope_LoginReq{
			LoginReq: &protos.LoginRequest{Cabecalho: cab, NomeUsuario: nome},
		}
		operacoes = append(operacoes, env)
	}

	for _, canal := range s.lerCanaisSemLock() {
		env := s.novoEnvelopeReplicacao()
		cab := &protos.Cabecalho{
			LinguagemOrigem: "snapshot",
			TimestampEnvio:  timestamppb.New(time.Unix(0, 0).UTC()),
		}
		env.Conteudo = &protos.Envelope_CreateChannelReq{
			CreateChannelReq: &protos.CreateChannelRequest{Cabecalho: cab, NomeCanal: canal},
		}
		operacoes = append(operacoes, env)
	}

	for _, item := range s.lerJSONL(s.publicacoesPath()) {
		canal := strings.TrimSpace(item["canal"])
		mensagem := strings.TrimSpace(item["mensagem"])
		if canal == "" || mensagem == "" {
			continue
		}
		remetente := strings.TrimSpace(item["remetente"])
		if remetente == "" {
			remetente = "desconhecido"
		}
		env := s.novoEnvelopeReplicacao()
		cab := &protos.Cabecalho{
			LinguagemOrigem: remetente,
			TimestampEnvio:  timestampTextoParaTimestamp(item["timestamp"]),
		}
		env.Conteudo = &protos.Envelope_PublishReq{
			PublishReq: &protos.PublishRequest{Cabecalho: cab, Canal: canal, Mensagem: mensagem},
		}
		operacoes = append(operacoes, env)
	}
	return operacoes
}

func (s *Servidor) aplicarOperacaoReplicada(env *protos.Envelope) {
	switch c := env.GetConteudo().(type) {
	case *protos.Envelope_LoginReq:
		nome := strings.TrimSpace(c.LoginReq.GetNomeUsuario())
		if nome != "" {
			s.registrarLogin(nome, mensageria.TimestampTexto(c.LoginReq.GetCabecalho().GetTimestampEnvio()))
		}
	case *protos.Envelope_CreateChannelReq:
		nome := strings.TrimSpace(c.CreateChannelReq.GetNomeCanal())
		if nome != "" {
			s.adicionarCanalIdempotente(nome)
		}
	case *protos.Envelope_PublishReq:
		req := c.PublishReq
		canal := strings.TrimSpace(req.GetCanal())
		mensagem := strings.TrimSpace(req.GetMensagem())
		if canal == "" || mensagem == "" {
			return
		}
		s.adicionarCanalIdempotente(canal)
		remetente := strings.TrimSpace(req.GetCabecalho().GetLinguagemOrigem())
		if remetente == "" {
			remetente = "desconhecido"
		}
		s.registrarPublicacao(
			canal,
			mensagem,
			remetente,
			mensageria.TimestampTexto(req.GetCabecalho().GetTimestampEnvio()),
		)
	}
}

func chaveLogin(item map[string]string) string {
	return item["usuario"] + "\x00" + item["timestamp"]
}

func chavePublicacao(item map[string]string) string {
	return item["canal"] + "\x00" + item["mensagem"] + "\x00" + item["remetente"] + "\x00" + item["timestamp"]
}

func (s *Servidor) aplicarOperacoesReplicadas(operacoes []*protos.Envelope) {
	s.storageMu.Lock()
	defer s.storageMu.Unlock()

	canaisSet := map[string]bool{}
	for _, canal := range s.lerCanaisSemLock() {
		canaisSet[canal] = true
	}

	loginsPorChave := map[string]map[string]string{}
	for _, item := range s.lerJSONL(s.loginsPath()) {
		loginsPorChave[chaveLogin(item)] = item
	}

	publicacoesPorChave := map[string]map[string]string{}
	for _, item := range s.lerJSONL(s.publicacoesPath()) {
		publicacoesPorChave[chavePublicacao(item)] = item
	}

	for _, env := range operacoes {
		switch c := env.GetConteudo().(type) {
		case *protos.Envelope_LoginReq:
			nome := strings.TrimSpace(c.LoginReq.GetNomeUsuario())
			if nome == "" {
				continue
			}
			item := map[string]string{
				"usuario":   nome,
				"timestamp": mensageria.TimestampTexto(c.LoginReq.GetCabecalho().GetTimestampEnvio()),
			}
			loginsPorChave[chaveLogin(item)] = item
		case *protos.Envelope_CreateChannelReq:
			nome := strings.TrimSpace(c.CreateChannelReq.GetNomeCanal())
			if nome != "" {
				canaisSet[nome] = true
			}
		case *protos.Envelope_PublishReq:
			req := c.PublishReq
			canal := strings.TrimSpace(req.GetCanal())
			mensagem := strings.TrimSpace(req.GetMensagem())
			if canal == "" || mensagem == "" {
				continue
			}
			canaisSet[canal] = true
			remetente := strings.TrimSpace(req.GetCabecalho().GetLinguagemOrigem())
			if remetente == "" {
				remetente = "desconhecido"
			}
			item := map[string]string{
				"canal":     canal,
				"mensagem":  mensagem,
				"remetente": remetente,
				"timestamp": mensageria.TimestampTexto(req.GetCabecalho().GetTimestampEnvio()),
			}
			publicacoesPorChave[chavePublicacao(item)] = item
		}
	}

	canais := make([]string, 0, len(canaisSet))
	for canal := range canaisSet {
		canais = append(canais, canal)
	}
	s.salvarCanaisSemLock(canais)

	logins := make([]map[string]string, 0, len(loginsPorChave))
	for _, item := range loginsPorChave {
		logins = append(logins, item)
	}
	sort.Slice(logins, func(i, j int) bool {
		if logins[i]["timestamp"] == logins[j]["timestamp"] {
			return logins[i]["usuario"] < logins[j]["usuario"]
		}
		return logins[i]["timestamp"] < logins[j]["timestamp"]
	})
	s.salvarJSONL(s.loginsPath(), logins)

	publicacoes := make([]map[string]string, 0, len(publicacoesPorChave))
	for _, item := range publicacoesPorChave {
		publicacoes = append(publicacoes, item)
	}
	sort.Slice(publicacoes, func(i, j int) bool {
		left := publicacoes[i]
		right := publicacoes[j]
		if left["timestamp"] != right["timestamp"] {
			return left["timestamp"] < right["timestamp"]
		}
		if left["canal"] != right["canal"] {
			return left["canal"] < right["canal"]
		}
		if left["remetente"] != right["remetente"] {
			return left["remetente"] < right["remetente"]
		}
		return left["mensagem"] < right["mensagem"]
	})
	s.salvarJSONL(s.publicacoesPath(), publicacoes)
}

func (s *Servidor) respostaSnapshotStatus(status protos.Status, erroMsg string) *protos.Envelope {
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	res := &protos.HeartbeatResponse{
		Cabecalho: cab,
		Status:    status,
		ErroMsg:   erroMsg,
	}
	return &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_HeartbeatRes{HeartbeatRes: res},
	}
}

func (s *Servidor) loop() {
	logVerbose("[SERVIDOR] Servidor iniciado. Aguardando mensagens...")
	for {
		data, err := s.sock.RecvBytes(0)
		if err != nil {
			log.Printf("[SERVIDOR] Recv: %v", err)
			continue
		}
		env := &protos.Envelope{}
		if err := proto.Unmarshal(data, env); err != nil {
			log.Printf("[SERVIDOR] Envelope inválido: %v", err)
			continue
		}

		s.clock.OnReceive(env.GetCabecalho().GetRelogioLogico())
		logVerbose("[SERVIDOR] Recebido %s %s", conteudoNome(env), mensageria.CabecalhoTexto(env.GetCabecalho()))

		conteudo := env.GetConteudo()
		if conteudo == nil {
			log.Printf("[SERVIDOR] Envelope sem conteúdo")
			continue
		}
		var resp proto.Message
		switch c := conteudo.(type) {
		case *protos.Envelope_LoginReq:
			resp = s.processarLogin(c.LoginReq)
		case *protos.Envelope_CreateChannelReq:
			resp = s.processarCreateChannel(c.CreateChannelReq)
		case *protos.Envelope_ListChannelsReq:
			resp = s.processarListChannels(c.ListChannelsReq)
		case *protos.Envelope_PublishReq:
			resp = s.processarPublish(c.PublishReq, env.GetCabecalho())
		default:
			log.Printf("[SERVIDOR] Tipo de mensagem não suportado: %T", conteudo)
			continue
		}

		cab := mensageria.NovoCabecalho(s.origem, s.clock)
		respEnv := &protos.Envelope{Cabecalho: cab}
		switch r := resp.(type) {
		case *protos.LoginResponse:
			r.Cabecalho = cab
			respEnv.Conteudo = &protos.Envelope_LoginRes{LoginRes: r}
		case *protos.CreateChannelResponse:
			r.Cabecalho = cab
			respEnv.Conteudo = &protos.Envelope_CreateChannelRes{CreateChannelRes: r}
		case *protos.ListChannelsResponse:
			r.Cabecalho = cab
			respEnv.Conteudo = &protos.Envelope_ListChannelsRes{ListChannelsRes: r}
		case *protos.PublishResponse:
			r.Cabecalho = cab
			respEnv.Conteudo = &protos.Envelope_PublishRes{PublishRes: r}
		}
		respData, _ := proto.Marshal(respEnv)
		logVerbose("[SERVIDOR] Enviando %s %s", conteudoNome(respEnv), mensageria.CabecalhoTexto(respEnv.GetCabecalho()))
		if _, err := s.sock.SendBytes(respData, 0); err != nil {
			log.Printf("[SERVIDOR] Send: %v", err)
			continue
		}

		s.requestsCount++
		if s.requestsCount%10 == 0 {
			s.heartbeatESincronizar()
		}
		if s.requestsCount%15 == 0 {
			s.sincronizarRelogioFisico()
		}
	}
}

func (s *Servidor) iniciarServicosInternos() {
	go s.loopRelogio()
	go s.loopEleicao()
	go s.loopSnapshot()
	go s.loopAnunciosCoordenador()
}

func (s *Servidor) loopSnapshot() {
	sock := mustBindReplySocket(snapshotPort)
	defer sock.Close()
	logVerbose("[SERVIDOR] Serviço de snapshot ouvindo em tcp://*:%d", snapshotPort)
	for {
		data, err := sock.RecvBytes(0)
		if err != nil {
			log.Printf("[SERVIDOR] Erro no serviço de snapshot: %v", err)
			continue
		}

		env, err := mensageria.EnvelopeFromBytes(data)
		if err != nil {
			log.Printf("[SERVIDOR] Snapshot request inválido: %v", err)
			continue
		}
		s.clock.OnReceive(env.GetCabecalho().GetRelogioLogico())

		status := s.respostaSnapshotStatus(protos.Status_STATUS_SUCESSO, "")
		if _, ok := env.GetConteudo().(*protos.Envelope_HeartbeatReq); !ok {
			status = s.respostaSnapshotStatus(protos.Status_STATUS_ERRO, "tipo não suportado para snapshot")
		}

		frames := [][]byte{}
		statusData, _ := proto.Marshal(status)
		frames = append(frames, statusData)
		if status.GetHeartbeatRes().GetStatus() == protos.Status_STATUS_SUCESSO {
			for _, op := range s.operacoesSnapshot() {
				data, err := proto.Marshal(op)
				if err == nil {
					frames = append(frames, data)
				}
			}
		}
		parts := make([]interface{}, len(frames))
		for i, frame := range frames {
			parts[i] = frame
		}
		if _, err := sock.SendMessage(parts...); err != nil {
			log.Printf("[SERVIDOR] Erro ao enviar snapshot: %v", err)
		}
	}
}

func (s *Servidor) sincronizarReplicas() {
	candidatos := []string{}
	coordenador := s.coordenadorAtual()
	if coordenador != "" && coordenador != s.serverName {
		candidatos = append(candidatos, coordenador)
	}
	for _, servidor := range s.servidoresAtivosSnapshot() {
		nome := servidor.GetNome()
		if nome == s.serverName || contemString(candidatos, nome) {
			continue
		}
		candidatos = append(candidatos, nome)
	}
	for _, nome := range candidatos {
		s.solicitarSnapshot(nome)
	}
}

func contemString(items []string, procurado string) bool {
	for _, item := range items {
		if item == procurado {
			return true
		}
	}
	return false
}

func (s *Servidor) solicitarSnapshot(nomeServidor string) bool {
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	req := &protos.HeartbeatRequest{Cabecalho: cab, NomeServidor: s.serverName}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_HeartbeatReq{HeartbeatReq: req},
	}
	data, err := proto.Marshal(env)
	if err != nil {
		return false
	}

	sock := mustNewReqSocketWithTimeout(fmt.Sprintf("tcp://%s:%d", nomeServidor, snapshotPort), snapshotTimeout)
	defer sock.Close()
	if _, err := sock.SendBytes(data, 0); err != nil {
		return false
	}
	frames, err := sock.RecvMessageBytes(0)
	if err != nil || len(frames) == 0 {
		return false
	}

	statusEnv, err := mensageria.EnvelopeFromBytes(frames[0])
	if err != nil {
		return false
	}
	s.clock.OnReceive(statusEnv.GetCabecalho().GetRelogioLogico())
	status, ok := statusEnv.GetConteudo().(*protos.Envelope_HeartbeatRes)
	if !ok || status.HeartbeatRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		return false
	}

	operacoes := []*protos.Envelope{}
	for _, frame := range frames[1:] {
		op, err := mensageria.EnvelopeFromBytes(frame)
		if err != nil {
			continue
		}
		s.clock.OnReceive(op.GetCabecalho().GetRelogioLogico())
		operacoes = append(operacoes, op)
	}
	if len(operacoes) > 0 {
		s.aplicarOperacoesReplicadas(operacoes)
	}
	aplicadas := len(operacoes)
	if aplicadas > 0 {
		logVerbose("[SERVIDOR] Snapshot aplicado de %s com %d operações", nomeServidor, aplicadas)
	}
	return true
}

func mustBindReplySocket(port int) *zmq4.Socket {
	sock, err := zmq4.NewSocket(zmq4.REP)
	if err != nil {
		log.Fatalf("Novo socket REP: %v", err)
	}
	if err := sock.Bind(fmt.Sprintf("tcp://*:%d", port)); err != nil {
		log.Fatalf("Bind REP na porta %d: %v", port, err)
	}
	return sock
}

func (s *Servidor) loopRelogio() {
	sock := mustBindReplySocket(clockPort)
	defer sock.Close()
	logVerbose("[SERVIDOR] Serviço de relógio interno ouvindo em tcp://*:%d", clockPort)
	for {
		data, err := sock.RecvBytes(0)
		if err != nil {
			log.Printf("[SERVIDOR] Erro no serviço interno de relógio: %v", err)
			continue
		}

		env, err := mensageria.EnvelopeFromBytes(data)
		if err != nil {
			log.Printf("[SERVIDOR] Envelope inválido no relógio interno: %v", err)
			continue
		}

		s.clock.OnReceive(env.GetCabecalho().GetRelogioLogico())
		cab := mensageria.NovoCabecalho(s.origem, s.clock)
		respostaEnv := &protos.Envelope{Cabecalho: cab}

		var resposta *protos.HeartbeatResponse
		switch c := env.GetConteudo().(type) {
		case *protos.Envelope_HeartbeatReq:
			resposta = s.processarPedidoRelogio(c.HeartbeatReq)
		default:
			resposta = &protos.HeartbeatResponse{
				Status:    protos.Status_STATUS_ERRO,
				ErroMsg:   fmt.Sprintf("tipo não suportado: %T", c),
				Cabecalho: nil,
			}
		}
		resposta.Cabecalho = cab
		respostaEnv.Conteudo = &protos.Envelope_HeartbeatRes{HeartbeatRes: resposta}

		payload, _ := mensageria.EnvelopeBytes(respostaEnv)
		if _, err := sock.SendBytes(payload, 0); err != nil {
			log.Printf("[SERVIDOR] Falha ao responder relógio interno: %v", err)
		}
	}
}

func (s *Servidor) loopEleicao() {
	sock := mustBindReplySocket(electionPort)
	defer sock.Close()
	logVerbose("[SERVIDOR] Serviço de eleição interno ouvindo em tcp://*:%d", electionPort)
	for {
		data, err := sock.RecvBytes(0)
		if err != nil {
			log.Printf("[SERVIDOR] Erro no serviço interno de eleição: %v", err)
			continue
		}

		env, err := mensageria.EnvelopeFromBytes(data)
		if err != nil {
			log.Printf("[SERVIDOR] Envelope inválido na eleição interna: %v", err)
			continue
		}

		s.clock.OnReceive(env.GetCabecalho().GetRelogioLogico())
		cab := mensageria.NovoCabecalho(s.origem, s.clock)
		respostaEnv := &protos.Envelope{Cabecalho: cab}

		var resposta *protos.RegisterServerResponse
		switch c := env.GetConteudo().(type) {
		case *protos.Envelope_RegisterServerReq:
			resposta = s.processarPedidoEleicao(c.RegisterServerReq)
		default:
			resposta = &protos.RegisterServerResponse{
				Status:    protos.Status_STATUS_ERRO,
				ErroMsg:   fmt.Sprintf("tipo não suportado: %T", c),
				Cabecalho: nil,
			}
		}
		resposta.Cabecalho = cab
		respostaEnv.Conteudo = &protos.Envelope_RegisterServerRes{RegisterServerRes: resposta}

		payload, _ := mensageria.EnvelopeBytes(respostaEnv)
		if _, err := sock.SendBytes(payload, 0); err != nil {
			log.Printf("[SERVIDOR] Falha ao responder eleição interna: %v", err)
		}
	}
}

func (s *Servidor) loopAnunciosCoordenador() {
	logVerbose("[SERVIDOR] Escutando tópicos internos %s e %s", topicoCoordenador, topicoReplica)
	for {
		msg, err := s.subSock.RecvMessageBytes(0)
		if err != nil || len(msg) < 2 {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		topico := string(msg[0])
		if topico == topicoReplica {
			env, err := mensageria.EnvelopeFromBytes(msg[1])
			if err != nil {
				continue
			}
			if env.GetCabecalho().GetLinguagemOrigem() == s.replicaOrigem() {
				continue
			}
			s.clock.OnReceive(env.GetCabecalho().GetRelogioLogico())
			s.aplicarOperacaoReplicada(env)
			logVerbose("[SERVIDOR] Operação replicada recebida: %s", conteudoNome(env))
			continue
		}
		if topico != topicoCoordenador {
			continue
		}

		channelMsg := &protos.ChannelMessage{}
		if err := proto.Unmarshal(msg[1], channelMsg); err != nil {
			continue
		}
		s.clock.OnReceive(channelMsg.GetRelogioLogico())
		coordenador := strings.TrimSpace(channelMsg.GetMensagem())
		if coordenador == "" {
			continue
		}
		remetente := strings.TrimSpace(channelMsg.GetRemetente())
		if remetente == s.serverName && coordenador == s.serverName {
			continue
		}
		if remetente == "" {
			remetente = "desconhecido"
		}
		s.processarAnuncioCoordenador(coordenador, remetente)
	}
}

func (s *Servidor) servidoresAtivosSnapshot() []*protos.ServerInfo {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	servidores := make([]*protos.ServerInfo, 0, len(s.activeServers))
	for nome, rank := range s.activeServers {
		servidores = append(servidores, &protos.ServerInfo{Nome: nome, Rank: rank})
	}
	sort.Slice(servidores, func(i, j int) bool {
		return servidores[i].GetRank() < servidores[j].GetRank()
	})
	return servidores
}

func (s *Servidor) resumoServidores(servidores []*protos.ServerInfo) string {
	if len(servidores) == 0 {
		return "(nenhum)"
	}
	partes := make([]string, 0, len(servidores))
	for _, servidor := range servidores {
		partes = append(partes, fmt.Sprintf("%s(rank=%d)", servidor.GetNome(), servidor.GetRank()))
	}
	return strings.Join(partes, ", ")
}

func (s *Servidor) atualizarServidoresAtivos(servidores []*protos.ServerInfo) {
	s.stateMu.Lock()
	for _, servidor := range servidores {
		s.knownRanks[servidor.GetNome()] = servidor.GetRank()
	}
	if len(servidores) > 0 {
		s.activeServers = make(map[string]int32, len(servidores))
		for _, servidor := range servidores {
			s.activeServers[servidor.GetNome()] = servidor.GetRank()
		}
	} else if len(s.activeServers) == 0 && s.myRank > 0 {
		s.activeServers = map[string]int32{s.serverName: s.myRank}
	}
	if s.myRank > 0 {
		s.activeServers[s.serverName] = s.myRank
		s.knownRanks[s.serverName] = s.myRank
	}
	s.stateMu.Unlock()

	logVerbose("[SERVIDOR] Lista ativa local atualizada: %s", s.resumoServidores(s.servidoresAtivosSnapshot()))
}

func (s *Servidor) restaurarServidorAtivoLocal(nome string) {
	s.stateMu.Lock()
	rank, ok := s.activeServers[nome]
	if !ok {
		rank, ok = s.knownRanks[nome]
	}
	if !ok {
		s.stateMu.Unlock()
		logVerbose("[SERVIDOR] Coordenador %s respondeu ao teste direto, mas rank local e desconhecido", nome)
		return
	}
	s.activeServers[nome] = rank
	s.stateMu.Unlock()

	logVerbose("[SERVIDOR] Coordenador %s respondeu ao teste direto; restaurado na lista ativa local rank=%d", nome, rank)
}

func (s *Servidor) maiorServidorAtivo() string {
	servidores := s.servidoresAtivosSnapshot()
	if len(servidores) == 0 {
		return s.serverName
	}
	return servidores[len(servidores)-1].GetNome()
}

func (s *Servidor) coordenadorAtual() string {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.coordenador
}

func (s *Servidor) setCoordenador(nome string) string {
	s.stateMu.Lock()
	anterior := s.coordenador
	s.coordenador = nome
	s.stateMu.Unlock()
	return anterior
}

func (s *Servidor) avaliarEleicaoAposAtualizacao(motivo string) {
	servidores := s.servidoresAtivosSnapshot()
	if len(servidores) == 0 {
		return
	}
	maior := servidores[len(servidores)-1]
	coordenador := s.coordenadorAtual()
	var rankCoordenador int32 = -1
	if coordenador != "" {
		if rank, ok := s.rankServidor(coordenador); ok {
			rankCoordenador = rank
		}
	}
	if coordenador == "" || rankCoordenador < 0 || maior.GetRank() > rankCoordenador {
		s.iniciarEleicaoAsync(motivo)
	}
}

func (s *Servidor) isCoordenador() bool {
	return s.coordenadorAtual() == s.serverName
}

func (s *Servidor) iniciarEleicaoAsync(motivo string) {
	go func() {
		s.executarEleicao(motivo)
	}()
}

func (s *Servidor) executarEleicao(motivo string) {
	s.stateMu.Lock()
	if s.electionRunning {
		s.stateMu.Unlock()
		logVerbose("[SERVIDOR] Eleição já em andamento; ignorando gatilho (%s)", motivo)
		return
	}
	s.electionRunning = true
	s.stateMu.Unlock()
	defer func() {
		s.stateMu.Lock()
		s.electionRunning = false
		s.stateMu.Unlock()
	}()

	logEleicao("inicio motivo=%s", motivo)
	s.listarServidoresReferencia()
	servidores := s.servidoresAtivosSnapshot()
	superiores := make([]*protos.ServerInfo, 0, len(servidores))
	for _, item := range servidores {
		if item.GetRank() > s.myRank && item.GetNome() != s.serverName {
			superiores = append(superiores, item)
		}
	}

	if len(superiores) == 0 {
		s.tornarCoordenador("sem_servidor_maior")
		return
	}

	recebeuOk := false
	sort.Slice(superiores, func(i, j int) bool {
		return superiores[i].GetRank() > superiores[j].GetRank()
	})
	for _, item := range superiores {
		if s.consultarEleicaoServidor(item.GetNome(), true) {
			recebeuOk = true
		}
	}

	if !recebeuOk {
		s.tornarCoordenador("sem_resposta_de_servidor_maior")
		return
	}

	logEleicao("delegada motivo=%s", motivo)
}

func (s *Servidor) tornarCoordenador(motivo string) {
	s.setCoordenador(s.serverName)
	logEleicao("eleito coordenador=%s rank=%d motivo=%s", s.serverName, s.myRank, motivo)
	s.anunciarCoordenador()
}

func (s *Servidor) anunciarCoordenador() {
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	channelMsg := &protos.ChannelMessage{
		Canal:          topicoCoordenador,
		Mensagem:       s.serverName,
		Remetente:      s.serverName,
		TimestampEnvio: cab.GetTimestampEnvio(),
		RelogioLogico:  cab.GetRelogioLogico(),
	}
	payload, err := proto.Marshal(channelMsg)
	if err != nil {
		log.Printf("[SERVIDOR] Falha ao serializar anúncio de coordenador: %v", err)
		return
	}
	if _, err := s.announceSock.SendMessage([]byte(topicoCoordenador), payload); err != nil {
		log.Printf("[SERVIDOR] Falha ao anunciar coordenador: %v", err)
		return
	}
	logEleicao("anuncio publicado coordenador=%s rank=%d", s.serverName, s.myRank)
}

func (s *Servidor) processarAnuncioCoordenador(coordenador, remetente string) {
	s.listarServidoresReferencia()
	rankAnunciado, ok := s.rankServidor(coordenador)
	if coordenador == s.serverName {
		rankAnunciado = s.myRank
		ok = true
	}
	if !ok {
		logEleicaoVerbose("anuncio ignorado coordenador=%s rank=desconhecido motivo=servidor_desconhecido", coordenador)
		return
	}

	atual := s.coordenadorAtual()
	rankAtual, atualConhecido := s.rankServidor(atual)
	aceitar := false
	motivo := "sem_coordenador"
	if atual == "" || atual == coordenador {
		aceitar = true
		if atual == coordenador {
			motivo = "mesmo_coordenador"
		}
	} else if !atualConhecido {
		aceitar = true
		motivo = "coordenador_atual_desconhecido"
	} else if rankAnunciado >= rankAtual {
		aceitar = true
		motivo = "rank_maior_ou_igual"
	} else if atual != s.serverName && !s.consultarEleicaoServidor(atual, false) {
		aceitar = true
		motivo = "coordenador_atual_indisponivel"
	}

	if aceitar {
		s.setCoordenador(coordenador)
		logEleicao(
			"anuncio aceito coordenador=%s rank=%d origem=%s motivo=%s",
			coordenador,
			rankAnunciado,
			remetente,
			motivo,
		)
		return
	}

	logEleicaoVerbose(
		"anuncio ignorado coordenador=%s rank=%d motivo=rank_menor_que_atual atual=%s",
		coordenador,
		rankAnunciado,
		atual,
	)
}

func (s *Servidor) enviarParaServidor(nomeServidor string, porta int, env *protos.Envelope, timeout time.Duration) (*protos.Envelope, int64, int64, error) {
	endpoint := fmt.Sprintf("tcp://%s:%d", nomeServidor, porta)
	sock := mustNewReqSocketWithTimeout(endpoint, timeout)
	defer sock.Close()

	data, err := mensageria.EnvelopeBytes(env)
	if err != nil {
		return nil, 0, 0, err
	}

	envioNs := time.Now().UnixNano()
	if _, err := sock.SendBytes(data, 0); err != nil {
		return nil, envioNs, time.Now().UnixNano(), err
	}

	reply, err := sock.RecvBytes(0)
	if err != nil {
		return nil, envioNs, time.Now().UnixNano(), err
	}
	recebimentoNs := time.Now().UnixNano()

	resp, err := mensageria.EnvelopeFromBytes(reply)
	if err != nil {
		return nil, envioNs, recebimentoNs, err
	}
	s.clock.OnReceive(resp.GetCabecalho().GetRelogioLogico())
	return resp, envioNs, recebimentoNs, nil
}

func (s *Servidor) consultarRelogioServidor(nomeServidor string) *protos.Envelope {
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	req := &protos.HeartbeatRequest{Cabecalho: cab, NomeServidor: s.serverName}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_HeartbeatReq{HeartbeatReq: req},
	}
	logVerbose("[SERVIDOR] Enviando heartbeat_req para %s %s", nomeServidor, mensageria.CabecalhoTexto(cab))
	resp, _, _, err := s.enviarParaServidor(nomeServidor, clockPort, env, requestTimeout)
	if err != nil {
		logVerbose("[SERVIDOR] Sem resposta de relógio de %s: %v", nomeServidor, err)
		return nil
	}

	resposta, ok := resp.GetConteudo().(*protos.Envelope_HeartbeatRes)
	if !ok {
		logVerbose("[SERVIDOR] Resposta inesperada de relógio de %s: %T", nomeServidor, resp.GetConteudo())
		return nil
	}
	if resposta.HeartbeatRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		logVerbose("[SERVIDOR] Servidor %s rejeitou pedido de relógio: %s", nomeServidor, resposta.HeartbeatRes.GetErroMsg())
		return nil
	}
	logVerbose("[SERVIDOR] Recebido heartbeat_res de %s %s", nomeServidor, mensageria.CabecalhoTexto(resp.GetCabecalho()))
	return resp
}

func (s *Servidor) consultarEleicaoServidor(nomeServidor string, registrarLogs bool) bool {
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	req := &protos.RegisterServerRequest{Cabecalho: cab, NomeServidor: s.serverName}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_RegisterServerReq{RegisterServerReq: req},
	}
	if registrarLogs {
		logEleicaoVerbose("req destino=%s", nomeServidor)
	}
	resp, _, _, err := s.enviarParaServidor(nomeServidor, electionPort, env, requestTimeout)
	if err != nil {
		if registrarLogs {
			logEleicaoVerbose("sem_resposta destino=%s", nomeServidor)
		}
		return false
	}

	resposta, ok := resp.GetConteudo().(*protos.Envelope_RegisterServerRes)
	if !ok {
		if registrarLogs {
			logEleicaoVerbose("resposta_invalida destino=%s", nomeServidor)
		}
		return false
	}
	if resposta.RegisterServerRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		if registrarLogs {
			logEleicaoVerbose("rejeitada destino=%s motivo=%s", nomeServidor, resposta.RegisterServerRes.GetErroMsg())
		}
		return false
	}
	if registrarLogs {
		logEleicaoVerbose("ok origem=%s rank=%d", nomeServidor, resposta.RegisterServerRes.GetRank())
	}
	return true
}

func (s *Servidor) sincronizarComoSeguidor(coordenador string, timeout time.Duration) bool {
	if coordenador == "" {
		return false
	}
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	req := &protos.HeartbeatRequest{Cabecalho: cab, NomeServidor: s.serverName}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_HeartbeatReq{HeartbeatReq: req},
	}

	resp, envioNs, recebimentoNs, err := s.enviarParaServidor(coordenador, clockPort, env, timeout)
	if err != nil {
		logVerbose("[SERVIDOR] Falha ao sincronizar com coordenador %s: %v", coordenador, err)
		return false
	}
	resposta, ok := resp.GetConteudo().(*protos.Envelope_HeartbeatRes)
	if !ok || resposta.HeartbeatRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		logVerbose("[SERVIDOR] Coordenador %s rejeitou sincronização", coordenador)
		return false
	}

	s.clock.AtualizarOffset(resp.GetCabecalho().GetTimestampEnvio(), envioNs, recebimentoNs)
	logVerbose("[SERVIDOR] Offset físico atualizado para %.3f ms usando coordenador %s", s.clock.OffsetMillis(), coordenador)
	return true
}

func (s *Servidor) sincronizarComoCoordenador() {
	servidores := s.servidoresAtivosSnapshot()
	amostras := []int64{s.clock.NowCorrigido().AsTime().UnixNano()}
	participantes := []string{s.serverName}
	falhas := make([]string, 0)

	for _, servidor := range servidores {
		if servidor.GetNome() == s.serverName {
			continue
		}
		resp := s.consultarRelogioServidor(servidor.GetNome())
		if resp == nil {
			falhas = append(falhas, servidor.GetNome())
			continue
		}
		resposta, ok := resp.GetConteudo().(*protos.Envelope_HeartbeatRes)
		if !ok || resposta.HeartbeatRes.GetStatus() != protos.Status_STATUS_SUCESSO {
			falhas = append(falhas, servidor.GetNome())
			continue
		}
		amostras = append(amostras, resposta.HeartbeatRes.GetCabecalho().GetTimestampEnvio().AsTime().UnixNano())
		participantes = append(participantes, servidor.GetNome())
	}

	mediaNs := int64(0)
	for _, amostra := range amostras {
		mediaNs += amostra
	}
	mediaNs /= int64(len(amostras))
	agoraNs := time.Now().UnixNano()
	s.clock.AtualizarOffset(timestamppb.New(time.Unix(0, mediaNs).UTC()), agoraNs, agoraNs)
	logVerbose(
		"[SERVIDOR] Berkeley aplicado pelo coordenador %s com participantes=%s falhas=%s media=%s offset=%.3f ms",
		s.serverName,
		strings.Join(participantes, ", "),
		strings.Join(falhas, ", "),
		mensageria.TimestampTexto(timestamppb.New(time.Unix(0, mediaNs).UTC())),
		s.clock.OffsetMillis(),
	)
}

func (s *Servidor) sincronizarRelogioFisico() {
	coordenador := s.coordenadorAtual()
	if coordenador == "" {
		s.iniciarEleicaoAsync("coordenador_desconhecido")
		return
	}

	if s.isCoordenador() {
		s.sincronizarComoCoordenador()
		return
	}

	ativos := map[string]bool{}
	for _, servidor := range s.servidoresAtivosSnapshot() {
		ativos[servidor.GetNome()] = true
	}
	if coordenador != "" && !ativos[coordenador] {
		logVerbose(
			"[SERVIDOR] Coordenador %s ausente da lista da referencia; testando diretamente com timeout=%dms",
			coordenador,
			timeoutTesteCoordenador.Milliseconds(),
		)
		if s.sincronizarComoSeguidor(coordenador, timeoutTesteCoordenador) {
			s.restaurarServidorAtivoLocal(coordenador)
			return
		}
		s.iniciarEleicaoAsync("coordenador_indisponivel_apos_teste")
		return
	}

	if coordenador != "" && s.sincronizarComoSeguidor(coordenador, requestTimeout) {
		return
	}

	s.iniciarEleicaoAsync(fmt.Sprintf("falha_sincronizar_com_%s", coordenador))
}

func (s *Servidor) processarPedidoRelogio(req *protos.HeartbeatRequest) *protos.HeartbeatResponse {
	resposta := &protos.HeartbeatResponse{}
	solicitante := strings.TrimSpace(req.GetNomeServidor())
	if solicitante == "" {
		solicitante = "desconhecido"
	}

	coordenador := s.coordenadorAtual()
	if s.isCoordenador() {
		resposta.Status = protos.Status_STATUS_SUCESSO
		logVerbose("[SERVIDOR] Respondendo relógio ao servidor %s como coordenador", solicitante)
		return resposta
	}

	if solicitante == coordenador && coordenador != "" {
		resposta.Status = protos.Status_STATUS_SUCESSO
		logVerbose("[SERVIDOR] Respondendo relógio ao coordenador %s", solicitante)
		return resposta
	}

	resposta.Status = protos.Status_STATUS_ERRO
	resposta.ErroMsg = "nao sou coordenador"
	logVerbose("[SERVIDOR] Pedido de relógio de %s rejeitado; coordenador atual=%s", solicitante, coordenador)
	return resposta
}

func (s *Servidor) processarPedidoEleicao(req *protos.RegisterServerRequest) *protos.RegisterServerResponse {
	resposta := &protos.RegisterServerResponse{}
	solicitante := strings.TrimSpace(req.GetNomeServidor())
	if solicitante == "" {
		solicitante = "desconhecido"
	}

	rankSolicitante, ok := s.rankServidor(solicitante)
	if !ok {
		rankSolicitante = -1
	}

	if s.myRank > rankSolicitante {
		resposta.Status = protos.Status_STATUS_SUCESSO
		resposta.Rank = s.myRank
		logEleicaoVerbose("ok enviado destino=%s rank=%d solicitante_rank=%d", solicitante, s.myRank, rankSolicitante)
		if s.isCoordenador() {
			s.anunciarCoordenador()
		} else {
			s.iniciarEleicaoAsync(fmt.Sprintf("pedido de eleição recebido de %s", solicitante))
		}
		return resposta
	}

	resposta.Status = protos.Status_STATUS_ERRO
	resposta.ErroMsg = "rank inferior"
	resposta.Rank = s.myRank
	logEleicaoVerbose("pedido ignorado origem=%s motivo=rank_inferior solicitante_rank=%d meu_rank=%d", solicitante, rankSolicitante, s.myRank)
	return resposta
}

func (s *Servidor) rankServidor(nome string) (int32, bool) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	rank, ok := s.activeServers[nome]
	if ok {
		return rank, true
	}
	rank, ok = s.knownRanks[nome]
	return rank, ok
}

func (s *Servidor) processarLogin(req *protos.LoginRequest) *protos.LoginResponse {
	res := &protos.LoginResponse{}
	nome := strings.TrimSpace(req.GetNomeUsuario())
	if nome == "" {
		res.Status = protos.Status_STATUS_ERRO
		res.ErroMsg = "nome de usuário vazio"
		return res
	}
	s.registrarLogin(nome, mensageria.TimestampTexto(req.GetCabecalho().GetTimestampEnvio()))
	s.replicarLogin(req)
	res.Status = protos.Status_STATUS_SUCESSO
	return res
}

func (s *Servidor) processarCreateChannel(req *protos.CreateChannelRequest) *protos.CreateChannelResponse {
	res := &protos.CreateChannelResponse{}
	nome := strings.TrimSpace(req.GetNomeCanal())
	if nome == "" {
		res.Status = protos.Status_STATUS_ERRO
		res.ErroMsg = "nome de canal vazio"
		return res
	}
	if !s.adicionarCanalIdempotente(nome) {
		res.Status = protos.Status_STATUS_ERRO
		res.ErroMsg = "canal já existe"
		return res
	}
	s.replicarCreateChannel(req)
	res.Status = protos.Status_STATUS_SUCESSO
	return res
}

func (s *Servidor) processarListChannels(_ *protos.ListChannelsRequest) *protos.ListChannelsResponse {
	return &protos.ListChannelsResponse{
		Canais: s.lerCanais(),
	}
}

func (s *Servidor) processarPublish(
	req *protos.PublishRequest,
	envCabecalho *protos.Cabecalho,
) *protos.PublishResponse {
	res := &protos.PublishResponse{}
	canal := strings.TrimSpace(req.GetCanal())
	mensagem := strings.TrimSpace(req.GetMensagem())

	if canal == "" {
		res.Status = protos.Status_STATUS_ERRO
		res.ErroMsg = "canal vazio"
		return res
	}
	if mensagem == "" {
		res.Status = protos.Status_STATUS_ERRO
		res.ErroMsg = "mensagem vazia"
		return res
	}

	existe := false
	for _, c := range s.lerCanais() {
		if c == canal {
			existe = true
			break
		}
	}
	if !existe {
		res.Status = protos.Status_STATUS_ERRO
		res.ErroMsg = "canal inexistente"
		return res
	}

	remetente := "desconhecido"
	if envCabecalho != nil && envCabecalho.GetLinguagemOrigem() != "" {
		remetente = envCabecalho.GetLinguagemOrigem()
	}
	cabPub := mensageria.NovoCabecalho(s.origem, s.clock)
	channelMsg := &protos.ChannelMessage{
		Canal:          canal,
		Mensagem:       mensagem,
		Remetente:      remetente,
		TimestampEnvio: cabPub.GetTimestampEnvio(),
		RelogioLogico:  cabPub.GetRelogioLogico(),
	}
	payload, err := proto.Marshal(channelMsg)
	if err != nil {
		res.Status = protos.Status_STATUS_ERRO
		res.ErroMsg = "erro ao serializar publicacao"
		return res
	}
	logVerbose(
		"[SERVIDOR] Publicando em %s ts=%s relogio=%d",
		canal,
		mensageria.TimestampTexto(channelMsg.GetTimestampEnvio()),
		channelMsg.GetRelogioLogico(),
	)
	if _, err := s.pubSock.SendMessage([]byte(canal), payload); err != nil {
		res.Status = protos.Status_STATUS_ERRO
		res.ErroMsg = "erro ao publicar mensagem"
		return res
	}

	s.registrarPublicacao(canal, mensagem, remetente, mensageria.TimestampTexto(channelMsg.GetTimestampEnvio()))
	s.replicarPublicacao(canal, mensagem, remetente, cabPub)

	res.Status = protos.Status_STATUS_SUCESSO
	return res
}
