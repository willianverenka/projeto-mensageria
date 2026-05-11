package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
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
	orqEndpointDefault = "tcp://orquestrador:5556"
	proxyPubEndpoint   = "tcp://proxy:5557"
	proxySubEndpoint   = "tcp://proxy:5558"
	referenceEndpoint  = "tcp://referencia:5559"
	dataDirDefault     = "/data"
	clockPort          = 5560
	electionPort       = 5561
	writePort          = 5562
	replicationPort    = 5563
	requestTimeout     = 2 * time.Second
	announcementDelay  = 3 * time.Second
	startupDelay       = 200 * time.Millisecond
	topicoCoordenador  = "servers"
)

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
	log.Printf("[SERVIDOR] Conectado ao orquestrador em %s", orqEndpoint)

	server := &Servidor{
		sock:              sock,
		pubSock:           mustNewPubSocket(pubEndpoint),
		announceSock:      mustNewPubSocket(pubEndpoint),
		refSock:           mustNewReqSocket(refEndpoint),
		subSock:           mustNewSubSocket(proxySubEndpoint, topicoCoordenador),
		dataDir:           dataDir,
		pubEndpoint:       pubEndpoint,
		referenceEndpoint: refEndpoint,
		serverName:        serverName,
		clock:             &mensageria.RelogioProcesso{},
		origem:            mensageria.OrigemLabel(mensageria.RoleServidor),
		activeServers:     map[string]int32{},
		txCache:           map[string]*txCacheEntry{},
	}
	defer server.pubSock.Close()
	defer server.announceSock.Close()
	defer server.refSock.Close()
	defer server.subSock.Close()
	log.Printf("[SERVIDOR] Conectado ao proxy pub em %s", pubEndpoint)
	log.Printf("[SERVIDOR] Conectado ao proxy sub em %s", proxySubEndpoint)
	log.Printf("[SERVIDOR] Conectado à referência em %s", refEndpoint)
	server.initStorage()
	if err := server.registrarNaReferencia(); err != nil {
		log.Fatalf("[SERVIDOR] Erro ao registrar na referência: %v", err)
	}
	servidores := server.listarServidoresReferencia()
	server.atualizarServidoresAtivos(servidores)
	server.definirCoordenadorTentativo(servidores)
	server.iniciarServicosInternos()
	time.Sleep(startupDelay)
	server.executarEleicao("startup")
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
	cacheMu           sync.RWMutex
	coordenador       string
	coordenadorVersao int64
	activeServers     map[string]int32
	txCache           map[string]*txCacheEntry
	txOrder           []string
	myRank            int32
	electionRunning   bool
}

type txCacheEntry struct {
	payload   []byte
	completed bool
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

func mustNewSubSocket(endpoint, topic string) *zmq4.Socket {
	sock, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		log.Fatalf("Novo socket SUB: %v", err)
	}
	if err := sock.Connect(endpoint); err != nil {
		log.Fatalf("Conectar SUB ao proxy: %v", err)
	}
	if err := sock.SetSubscribe(topic); err != nil {
		log.Fatalf("Inscrever no tópico %s: %v", topic, err)
	}
	return sock
}

func getEnv(name, defaultVal string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return defaultVal
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

func erroLogin(msg string) *protos.LoginResponse {
	return &protos.LoginResponse{Status: protos.Status_STATUS_ERRO, ErroMsg: msg}
}

func erroCreateChannel(msg string) *protos.CreateChannelResponse {
	return &protos.CreateChannelResponse{Status: protos.Status_STATUS_ERRO, ErroMsg: msg}
}

func erroPublish(msg string) *protos.PublishResponse {
	return &protos.PublishResponse{Status: protos.Status_STATUS_ERRO, ErroMsg: msg}
}

func (s *Servidor) chaveTransacao(env *protos.Envelope) string {
	cab := env.GetCabecalho()
	return fmt.Sprintf(
		"%s|%s|%d|%s",
		conteudoNome(env),
		cab.GetLinguagemOrigem(),
		cab.GetIdTransacao(),
		mensageria.TimestampTexto(cab.GetTimestampEnvio()),
	)
}

func (s *Servidor) cacheRespostaParaEnvelope(env *protos.Envelope, payload []byte) proto.Message {
	switch env.GetConteudo().(type) {
	case *protos.Envelope_LoginReq:
		resp := &protos.LoginResponse{}
		_ = proto.Unmarshal(payload, resp)
		return resp
	case *protos.Envelope_CreateChannelReq:
		resp := &protos.CreateChannelResponse{}
		_ = proto.Unmarshal(payload, resp)
		return resp
	case *protos.Envelope_PublishReq:
		resp := &protos.PublishResponse{}
		_ = proto.Unmarshal(payload, resp)
		return resp
	default:
		return nil
	}
}

func (s *Servidor) cacheObter(chave string) (*txCacheEntry, bool) {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	entry, ok := s.txCache[chave]
	if !ok {
		return nil, false
	}
	copia := *entry
	return &copia, true
}

func (s *Servidor) cacheRegistrarAplicada(chave string, resp proto.Message) {
	payload, err := proto.Marshal(resp)
	if err != nil {
		return
	}

	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if _, exists := s.txCache[chave]; !exists {
		s.txOrder = append(s.txOrder, chave)
	}
	s.txCache[chave] = &txCacheEntry{payload: payload, completed: false}
	for len(s.txOrder) > 1024 {
		oldest := s.txOrder[0]
		s.txOrder = s.txOrder[1:]
		delete(s.txCache, oldest)
	}
}

func (s *Servidor) cacheMarcarConcluida(chave string) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if entry, ok := s.txCache[chave]; ok {
		entry.completed = true
	}
}

func (s *Servidor) respostaCache(env *protos.Envelope, chave string) proto.Message {
	entry, ok := s.cacheObter(chave)
	if !ok || len(entry.payload) == 0 {
		return nil
	}
	return s.cacheRespostaParaEnvelope(env, entry.payload)
}

func (s *Servidor) refrescarCluster(motivo string) {
	servidores := s.listarServidoresReferencia()
	ativos := map[string]bool{}
	for _, servidor := range servidores {
		ativos[servidor.GetNome()] = true
	}
	coordenador := s.coordenadorAtual()
	if coordenador != "" && !ativos[coordenador] {
		log.Printf("[SERVIDOR] Coordenador %s ausente da lista ativa; iniciando eleição (%s)", coordenador, motivo)
		s.executarEleicao(motivo)
	}
}

func isErroCoordenador(msg string) bool {
	return msg == "nao sou coordenador" || msg == "coordenador indisponivel"
}

func (s *Servidor) envelopeResposta(resp proto.Message) *protos.Envelope {
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
	return respEnv
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

func (s *Servidor) registrarNaReferencia() error {
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	req := &protos.RegisterServerRequest{Cabecalho: cab, NomeServidor: s.serverName}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_RegisterServerReq{RegisterServerReq: req},
	}
	resp, err := s.enviarParaReferencia(env, true)
	if err != nil {
		return err
	}
	res, ok := resp.GetConteudo().(*protos.Envelope_RegisterServerRes)
	if !ok {
		return fmt.Errorf("resposta inesperada ao registrar na referência: %T", resp.GetConteudo())
	}
	if res.RegisterServerRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		return fmt.Errorf("falha ao registrar na referência: %s", res.RegisterServerRes.GetErroMsg())
	}
	s.myRank = res.RegisterServerRes.GetRank()
	s.atualizarServidoresAtivos([]*protos.ServerInfo{{Nome: s.serverName, Rank: s.myRank}})
	log.Printf("[SERVIDOR] Servidor %s registrado na referência com rank=%d", s.serverName, res.RegisterServerRes.GetRank())
	return nil
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
	log.Printf("[SERVIDOR] Servidores disponíveis na referência: %s", strings.Join(partes, ", "))
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
		log.Printf("[SERVIDOR] Erro ao enviar heartbeat: %v", err)
		return
	}
	res, ok := resp.GetConteudo().(*protos.Envelope_HeartbeatRes)
	if !ok {
		log.Printf("[SERVIDOR] Resposta inesperada ao heartbeat: %T", resp.GetConteudo())
		return
	}
	if res.HeartbeatRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		log.Printf("[SERVIDOR] Heartbeat rejeitado: %s", res.HeartbeatRes.GetErroMsg())
		log.Printf("[SERVIDOR] Reregistrando servidor após heartbeat rejeitado")
		if err := s.registrarNaReferencia(); err != nil {
			log.Printf("[SERVIDOR] Falha ao reregistrar na referência: %v", err)
		}
		s.refrescarCluster("heartbeat rejeitado pela referência")
		return
	}
	log.Printf("[SERVIDOR] Heartbeat enviado com sucesso para %s", s.serverName)
	s.refrescarCluster("heartbeat bem-sucedido")
}

func (s *Servidor) enviarParaReferencia(env *protos.Envelope, atualizarOffset bool) (*protos.Envelope, error) {
	data, err := mensageria.EnvelopeBytes(env)
	if err != nil {
		return nil, err
	}
	log.Printf("[SERVIDOR] Enviando %s para referência %s", conteudoNome(env), mensageria.CabecalhoTexto(env.GetCabecalho()))
	envioNs := time.Now().UnixNano()
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
	log.Printf("[SERVIDOR] Recebido %s da referência %s", conteudoNome(resp), mensageria.CabecalhoTexto(resp.GetCabecalho()))
	_ = envioNs
	_ = recebimentoNs
	return resp, nil
}

func (s *Servidor) lerCanais() []string {
	data, err := os.ReadFile(s.canaisPath())
	if err != nil {
		return nil
	}
	var canais []string
	if err := json.Unmarshal(data, &canais); err != nil {
		return nil
	}
	return canais
}

func (s *Servidor) salvarCanais(canais []string) {
	data, _ := json.Marshal(canais)
	_ = os.WriteFile(s.canaisPath(), data, 0644)
}

func (s *Servidor) registrarLogin(nomeUsuario, timestampISO string) {
	line, _ := json.Marshal(map[string]string{"usuario": nomeUsuario, "timestamp": timestampISO})
	f, err := os.OpenFile(s.loginsPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(line, '\n'))
}

func (s *Servidor) registrarPublicacao(canal, mensagem, remetente, timestampISO string) {
	line, _ := json.Marshal(
		map[string]string{
			"canal":     canal,
			"mensagem":  mensagem,
			"remetente": remetente,
			"timestamp": timestampISO,
		},
	)
	f, err := os.OpenFile(s.publicacoesPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(line, '\n'))
}

func (s *Servidor) loop() {
	log.Println("[SERVIDOR] Servidor iniciado. Aguardando mensagens...")
	for {
		frames, err := s.sock.RecvMessageBytes(0)
		if err != nil {
			log.Printf("[SERVIDOR] Recv: %v", err)
			continue
		}
		if len(frames) == 0 {
			continue
		}
		env := &protos.Envelope{}
		if err := proto.Unmarshal(frames[len(frames)-1], env); err != nil {
			log.Printf("[SERVIDOR] Envelope inválido: %v", err)
			continue
		}

		s.clock.OnReceive(env.GetCabecalho().GetRelogioLogico())
		log.Printf("[SERVIDOR] Recebido %s %s", conteudoNome(env), mensageria.CabecalhoTexto(env.GetCabecalho()))

		conteudo := env.GetConteudo()
		if conteudo == nil {
			log.Printf("[SERVIDOR] Envelope sem conteúdo")
			continue
		}
		var resp proto.Message
		switch c := conteudo.(type) {
		case *protos.Envelope_LoginReq:
			resp = s.processarRequisicaoEscrita(env)
		case *protos.Envelope_CreateChannelReq:
			resp = s.processarRequisicaoEscrita(env)
		case *protos.Envelope_ListChannelsReq:
			resp = s.processarListChannels(c.ListChannelsReq)
		case *protos.Envelope_PublishReq:
			resp = s.processarRequisicaoEscrita(env)
		default:
			log.Printf("[SERVIDOR] Tipo de mensagem não suportado: %T", conteudo)
			continue
		}

		respEnv := s.envelopeResposta(resp)
		respData, _ := proto.Marshal(respEnv)
		log.Printf("[SERVIDOR] Enviando %s %s", conteudoNome(respEnv), mensageria.CabecalhoTexto(respEnv.GetCabecalho()))
		out := make([]interface{}, 0, len(frames))
		for _, frame := range frames[:len(frames)-1] {
			out = append(out, frame)
		}
		out = append(out, respData)
		if _, err := s.sock.SendMessage(out...); err != nil {
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
	go s.loopEscritasCoordenador()
	go s.loopReplicacao()
	go s.loopAnunciosCoordenador()
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
	log.Printf("[SERVIDOR] Serviço de relógio interno ouvindo em tcp://*:%d", clockPort)
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
	log.Printf("[SERVIDOR] Serviço de eleição interno ouvindo em tcp://*:%d", electionPort)
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

func (s *Servidor) loopEscritasCoordenador() {
	sock := mustBindReplySocket(writePort)
	defer sock.Close()
	log.Printf("[SERVIDOR] Serviço interno de escrita ouvindo em tcp://*:%d", writePort)
	for {
		data, err := sock.RecvBytes(0)
		if err != nil {
			log.Printf("[SERVIDOR] Erro no serviço interno de escrita: %v", err)
			continue
		}
		env, err := mensageria.EnvelopeFromBytes(data)
		if err != nil {
			log.Printf("[SERVIDOR] Envelope inválido na escrita interna: %v", err)
			continue
		}
		s.clock.OnReceive(env.GetCabecalho().GetRelogioLogico())
		var resp proto.Message
		if !s.isCoordenador() {
			switch env.GetConteudo().(type) {
			case *protos.Envelope_LoginReq:
				resp = erroLogin("nao sou coordenador")
			case *protos.Envelope_CreateChannelReq:
				resp = erroCreateChannel("nao sou coordenador")
			default:
				resp = erroPublish("nao sou coordenador")
			}
		} else {
			resp = s.processarEscritaComReplicacao(env, s.chaveTransacao(env))
		}
		payload, _ := mensageria.EnvelopeBytes(s.envelopeResposta(resp))
		if _, err := sock.SendBytes(payload, 0); err != nil {
			log.Printf("[SERVIDOR] Falha ao responder escrita interna: %v", err)
		}
	}
}

func (s *Servidor) loopReplicacao() {
	sock := mustBindReplySocket(replicationPort)
	defer sock.Close()
	log.Printf("[SERVIDOR] Serviço interno de réplica ouvindo em tcp://*:%d", replicationPort)
	for {
		data, err := sock.RecvBytes(0)
		if err != nil {
			log.Printf("[SERVIDOR] Erro no serviço interno de réplica: %v", err)
			continue
		}
		env, err := mensageria.EnvelopeFromBytes(data)
		if err != nil {
			log.Printf("[SERVIDOR] Envelope inválido na réplica interna: %v", err)
			continue
		}
		s.clock.OnReceive(env.GetCabecalho().GetRelogioLogico())
		resp := s.aplicarEscritaLocal(env, true, s.chaveTransacao(env))
		payload, _ := mensageria.EnvelopeBytes(s.envelopeResposta(resp))
		if _, err := sock.SendBytes(payload, 0); err != nil {
			log.Printf("[SERVIDOR] Falha ao responder réplica interna: %v", err)
		}
	}
}

func (s *Servidor) loopAnunciosCoordenador() {
	log.Printf("[SERVIDOR] Escutando anúncios de coordenador no tópico %s", topicoCoordenador)
	for {
		msg, err := s.subSock.RecvMessageBytes(0)
		if err != nil || len(msg) < 2 {
			time.Sleep(200 * time.Millisecond)
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
		s.setCoordenador(coordenador, fmt.Sprintf("anúncio publicado por %s", channelMsg.GetRemetente()), true)
		log.Printf("[SERVIDOR] Anúncio de coordenador recebido em servers: %s", coordenador)
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
	}
	s.stateMu.Unlock()

	log.Printf("[SERVIDOR] Lista ativa local atualizada: %s", s.resumoServidores(s.servidoresAtivosSnapshot()))
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

func (s *Servidor) versaoCoordenador() int64 {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.coordenadorVersao
}

func (s *Servidor) setCoordenador(nome, motivo string, incrementarVersao bool) {
	s.stateMu.Lock()
	anterior := s.coordenador
	s.coordenador = nome
	if incrementarVersao {
		s.coordenadorVersao++
	}
	versao := s.coordenadorVersao
	s.stateMu.Unlock()

	if anterior == nome {
		log.Printf("[SERVIDOR] Coordenador mantido em %s (%s, versao=%d)", nome, motivo, versao)
	} else {
		log.Printf("[SERVIDOR] Coordenador atualizado de %s para %s (%s, versao=%d)", anterior, nome, motivo, versao)
	}
}

func (s *Servidor) definirCoordenadorTentativo(servidores []*protos.ServerInfo) {
	if s.coordenadorAtual() != "" {
		return
	}
	candidato := s.maiorServidorAtivo()
	s.setCoordenador(candidato, "coordenador tentativo inicial", false)
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
		log.Printf("[SERVIDOR] Eleição já em andamento; ignorando gatilho (%s)", motivo)
		return
	}
	s.electionRunning = true
	s.stateMu.Unlock()
	defer func() {
		s.stateMu.Lock()
		s.electionRunning = false
		s.stateMu.Unlock()
	}()

	log.Printf("[SERVIDOR] Iniciando eleição (%s)", motivo)
	versaoInicial := s.versaoCoordenador()
	servidores := s.servidoresAtivosSnapshot()
	superiores := make([]*protos.ServerInfo, 0, len(servidores))
	for _, item := range servidores {
		if item.GetRank() > s.myRank && item.GetNome() != s.serverName {
			superiores = append(superiores, item)
		}
	}

	if len(superiores) == 0 {
		if s.versaoCoordenador() > versaoInicial {
			log.Printf("[SERVIDOR] Eleição concluída durante a coleta de respostas")
			return
		}
		s.tornarCoordenador("nenhum servidor de maior rank respondeu")
		return
	}

	respostasOk := make([]*protos.ServerInfo, 0, len(superiores))
	sort.Slice(superiores, func(i, j int) bool {
		return superiores[i].GetRank() > superiores[j].GetRank()
	})
	for _, item := range superiores {
		if s.consultarEleicaoServidor(item.GetNome()) {
			respostasOk = append(respostasOk, item)
		}
	}

	if s.versaoCoordenador() > versaoInicial {
		log.Printf("[SERVIDOR] Eleição concluída por anúncio recebido durante a coleta")
		return
	}

	if len(respostasOk) == 0 {
		s.tornarCoordenador("nenhum servidor de maior rank respondeu")
		return
	}

	deadline := time.Now().Add(announcementDelay)
	for time.Now().Before(deadline) {
		if s.versaoCoordenador() > versaoInicial {
			log.Printf("[SERVIDOR] Eleição concluída por anúncio de coordenador")
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	candidato := respostasOk[0]
	for _, item := range respostasOk[1:] {
		if item.GetRank() > candidato.GetRank() {
			candidato = item
		}
	}
	s.setCoordenador(
		candidato.GetNome(),
		fmt.Sprintf("coordenador inferido após timeout de anúncio (%s)", motivo),
		true,
	)
}

func (s *Servidor) tornarCoordenador(motivo string) {
	s.setCoordenador(s.serverName, motivo, true)
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
	log.Printf("[SERVIDOR] Anunciado coordenador em servers: %s", s.serverName)
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
	log.Printf("[SERVIDOR] Enviando heartbeat_req para %s %s", nomeServidor, mensageria.CabecalhoTexto(cab))
	resp, _, _, err := s.enviarParaServidor(nomeServidor, clockPort, env, requestTimeout)
	if err != nil {
		log.Printf("[SERVIDOR] Sem resposta de relógio de %s: %v", nomeServidor, err)
		return nil
	}

	resposta, ok := resp.GetConteudo().(*protos.Envelope_HeartbeatRes)
	if !ok {
		log.Printf("[SERVIDOR] Resposta inesperada de relógio de %s: %T", nomeServidor, resp.GetConteudo())
		return nil
	}
	if resposta.HeartbeatRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		log.Printf("[SERVIDOR] Servidor %s rejeitou pedido de relógio: %s", nomeServidor, resposta.HeartbeatRes.GetErroMsg())
		return nil
	}
	log.Printf("[SERVIDOR] Recebido heartbeat_res de %s %s", nomeServidor, mensageria.CabecalhoTexto(resp.GetCabecalho()))
	return resp
}

func (s *Servidor) consultarEleicaoServidor(nomeServidor string) bool {
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	req := &protos.RegisterServerRequest{Cabecalho: cab, NomeServidor: s.serverName}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_RegisterServerReq{RegisterServerReq: req},
	}
	log.Printf("[SERVIDOR] Enviando pedido de eleição para %s %s", nomeServidor, mensageria.CabecalhoTexto(cab))
	resp, _, _, err := s.enviarParaServidor(nomeServidor, electionPort, env, requestTimeout)
	if err != nil {
		log.Printf("[SERVIDOR] Sem resposta de eleição de %s: %v", nomeServidor, err)
		return false
	}

	resposta, ok := resp.GetConteudo().(*protos.Envelope_RegisterServerRes)
	if !ok {
		log.Printf("[SERVIDOR] Resposta inesperada de eleição de %s: %T", nomeServidor, resp.GetConteudo())
		return false
	}
	if resposta.RegisterServerRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		log.Printf("[SERVIDOR] Servidor %s rejeitou eleição: %s", nomeServidor, resposta.RegisterServerRes.GetErroMsg())
		return false
	}
	log.Printf("[SERVIDOR] Servidor %s respondeu OK à eleição (rank=%d)", nomeServidor, resposta.RegisterServerRes.GetRank())
	return true
}

func (s *Servidor) processarRequisicaoEscrita(env *protos.Envelope) proto.Message {
	switch env.GetConteudo().(type) {
	case *protos.Envelope_LoginReq:
		return s.encaminharEscritaAoCoordenador(env, "login")
	case *protos.Envelope_CreateChannelReq:
		return s.encaminharEscritaAoCoordenador(env, "create_channel")
	case *protos.Envelope_PublishReq:
		return s.encaminharEscritaAoCoordenador(env, "publish")
	default:
		return erroPublish("tipo de escrita nao suportado")
	}
}

func (s *Servidor) encaminharEscritaAoCoordenador(env *protos.Envelope, operacao string) proto.Message {
	chave := s.chaveTransacao(env)
	if entry, ok := s.cacheObter(chave); ok && entry.completed {
		if cached := s.respostaCache(env, chave); cached != nil {
			return cached
		}
	}

	coordenador := s.coordenadorAtual()
	if coordenador == "" {
		s.executarEleicao("coordenador desconhecido para escrita")
		coordenador = s.coordenadorAtual()
	}
	if coordenador == "" || coordenador == s.serverName {
		if operacao == "login" {
			return erroLogin("coordenador indisponivel")
		}
		if operacao == "create_channel" {
			return erroCreateChannel("coordenador indisponivel")
		}
		return erroPublish("coordenador indisponivel")
	}

	for tentativa := 0; tentativa < 2; tentativa++ {
		resp, _, _, err := s.enviarParaServidor(coordenador, writePort, env, requestTimeout)
		if err == nil && resp != nil {
			switch operacao {
			case "login":
				if r, ok := resp.GetConteudo().(*protos.Envelope_LoginRes); ok {
					if r.LoginRes.GetStatus() == protos.Status_STATUS_SUCESSO {
						return r.LoginRes
					}
					if !isErroCoordenador(r.LoginRes.GetErroMsg()) {
						return r.LoginRes
					}
				}
			case "create_channel":
				if r, ok := resp.GetConteudo().(*protos.Envelope_CreateChannelRes); ok {
					if r.CreateChannelRes.GetStatus() == protos.Status_STATUS_SUCESSO {
						return r.CreateChannelRes
					}
					if !isErroCoordenador(r.CreateChannelRes.GetErroMsg()) {
						return r.CreateChannelRes
					}
				}
			default:
				if r, ok := resp.GetConteudo().(*protos.Envelope_PublishRes); ok {
					if r.PublishRes.GetStatus() == protos.Status_STATUS_SUCESSO {
						return r.PublishRes
					}
					if !isErroCoordenador(r.PublishRes.GetErroMsg()) {
						return r.PublishRes
					}
				}
			}
		}

		s.refrescarCluster("falha ao encaminhar escrita")
		s.executarEleicao("falha ao encaminhar escrita")
		if s.isCoordenador() {
			return s.processarEscritaComReplicacao(env, chave)
		}
		coordenador = s.coordenadorAtual()
		if coordenador == "" || coordenador == s.serverName {
			break
		}
	}

	if operacao == "login" {
		return erroLogin("coordenador indisponivel")
	}
	if operacao == "create_channel" {
		return erroCreateChannel("coordenador indisponivel")
	}
	return erroPublish("coordenador indisponivel")
}

func (s *Servidor) replicarEscrita(env *protos.Envelope, chave string) proto.Message {
	tipo := env.GetConteudo()
	alvosPendentes := map[string]bool{}
	for _, item := range s.servidoresAtivosSnapshot() {
		if item.GetNome() == s.serverName {
			continue
		}
		alvosPendentes[item.GetNome()] = true
	}
	confirmados := map[string]bool{}

	if len(alvosPendentes) == 0 {
		return nil
	}

	for tentativa := 0; tentativa < 2; tentativa++ {
		pendentes := make([]string, 0, len(alvosPendentes))
		for nome := range alvosPendentes {
			if confirmados[nome] {
				continue
			}
			pendentes = append(pendentes, nome)
		}
		if len(pendentes) == 0 {
			return nil
		}

		for _, nome := range pendentes {
			resp, _, _, err := s.enviarParaServidor(nome, replicationPort, env, requestTimeout)
			if err == nil && resp != nil {
				switch c := resp.GetConteudo().(type) {
				case *protos.Envelope_LoginRes:
					if c.LoginRes.GetStatus() == protos.Status_STATUS_SUCESSO {
						confirmados[nome] = true
						continue
					}
				case *protos.Envelope_CreateChannelRes:
					if c.CreateChannelRes.GetStatus() == protos.Status_STATUS_SUCESSO {
						confirmados[nome] = true
						continue
					}
				case *protos.Envelope_PublishRes:
					if c.PublishRes.GetStatus() == protos.Status_STATUS_SUCESSO {
						confirmados[nome] = true
						continue
					}
				default:
				}
			}
		}

		s.refrescarCluster("falha ao replicar escrita")
		ativos := map[string]bool{}
		for _, item := range s.servidoresAtivosSnapshot() {
			if item.GetNome() == s.serverName {
				continue
			}
			ativos[item.GetNome()] = true
		}
		for nome := range alvosPendentes {
			if !ativos[nome] {
				delete(alvosPendentes, nome)
				delete(confirmados, nome)
			}
		}
		if len(alvosPendentes) == 0 || len(confirmados) == len(alvosPendentes) {
			return nil
		}
	}

	if len(confirmados) == len(alvosPendentes) {
		return nil
	}

	switch tipo.(type) {
	case *protos.Envelope_LoginReq:
		return erroLogin("falha ao replicar escrita")
	case *protos.Envelope_CreateChannelReq:
		return erroCreateChannel("falha ao replicar escrita")
	default:
		return erroPublish("falha ao replicar escrita")
	}
}

func (s *Servidor) aplicarEscritaLocal(env *protos.Envelope, origemReplicacao bool, chave string) proto.Message {
	if entry, ok := s.cacheObter(chave); ok && (entry.completed || origemReplicacao) {
		if cached := s.respostaCache(env, chave); cached != nil {
			return cached
		}
	}

	switch c := env.GetConteudo().(type) {
	case *protos.Envelope_LoginReq:
		return s.processarLogin(c.LoginReq)
	case *protos.Envelope_CreateChannelReq:
		return s.processarCreateChannel(c.CreateChannelReq, origemReplicacao)
	case *protos.Envelope_PublishReq:
		return s.processarPublish(c.PublishReq, env.GetCabecalho(), !origemReplicacao)
	default:
		return erroPublish("tipo de escrita nao suportado")
	}
}

func (s *Servidor) processarEscritaComReplicacao(env *protos.Envelope, chave string) proto.Message {
	if entry, ok := s.cacheObter(chave); ok && entry.completed {
		if cached := s.respostaCache(env, chave); cached != nil {
			return cached
		}
	}

	var resp proto.Message
	if cached := s.respostaCache(env, chave); cached != nil {
		resp = cached
	} else {
		resp = s.aplicarEscritaLocal(env, false, chave)
		switch r := resp.(type) {
		case *protos.LoginResponse:
			if r.GetStatus() != protos.Status_STATUS_SUCESSO {
				return r
			}
		case *protos.CreateChannelResponse:
			if r.GetStatus() != protos.Status_STATUS_SUCESSO {
				return r
			}
		case *protos.PublishResponse:
			if r.GetStatus() != protos.Status_STATUS_SUCESSO {
				return r
			}
		}
		s.cacheRegistrarAplicada(chave, resp)
	}

	if replicaErr := s.replicarEscrita(env, chave); replicaErr != nil {
		return replicaErr
	}
	s.cacheMarcarConcluida(chave)
	return resp
}

func (s *Servidor) sincronizarComoSeguidor(coordenador string) bool {
	if coordenador == "" {
		return false
	}
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	req := &protos.HeartbeatRequest{Cabecalho: cab, NomeServidor: s.serverName}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_HeartbeatReq{HeartbeatReq: req},
	}

	resp, envioNs, recebimentoNs, err := s.enviarParaServidor(coordenador, clockPort, env, requestTimeout)
	if err != nil {
		log.Printf("[SERVIDOR] Falha ao sincronizar com coordenador %s: %v", coordenador, err)
		return false
	}
	resposta, ok := resp.GetConteudo().(*protos.Envelope_HeartbeatRes)
	if !ok || resposta.HeartbeatRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		log.Printf("[SERVIDOR] Coordenador %s rejeitou sincronização", coordenador)
		return false
	}

	s.clock.AtualizarOffset(resp.GetCabecalho().GetTimestampEnvio(), envioNs, recebimentoNs)
	log.Printf("[SERVIDOR] Offset físico atualizado para %.3f ms usando coordenador %s", s.clock.OffsetMillis(), coordenador)
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
	log.Printf(
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
		log.Printf("[SERVIDOR] Coordenador desconhecido; iniciando eleição")
		s.executarEleicao("coordenador desconhecido")
		coordenador = s.coordenadorAtual()
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
		log.Printf("[SERVIDOR] Coordenador %s ausente da lista ativa; iniciando eleição", coordenador)
		s.executarEleicao("coordenador ausente da lista ativa")
		coordenador = s.coordenadorAtual()
		if s.isCoordenador() {
			s.sincronizarComoCoordenador()
			return
		}
	}

	if coordenador != "" && s.sincronizarComoSeguidor(coordenador) {
		return
	}

	log.Printf("[SERVIDOR] Falha ao sincronizar com coordenador %s; iniciando eleição", coordenador)
	s.executarEleicao("falha ao sincronizar com coordenador")
	coordenador = s.coordenadorAtual()
	if s.isCoordenador() {
		s.sincronizarComoCoordenador()
		return
	}
	if coordenador != "" && !s.sincronizarComoSeguidor(coordenador) {
		log.Printf("[SERVIDOR] Ainda não foi possível sincronizar com %s após a eleição", coordenador)
	}
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
		log.Printf("[SERVIDOR] Respondendo relógio ao servidor %s como coordenador", solicitante)
		return resposta
	}

	if solicitante == coordenador && coordenador != "" {
		resposta.Status = protos.Status_STATUS_SUCESSO
		log.Printf("[SERVIDOR] Respondendo relógio ao coordenador %s", solicitante)
		return resposta
	}

	resposta.Status = protos.Status_STATUS_ERRO
	resposta.ErroMsg = "nao sou coordenador"
	log.Printf("[SERVIDOR] Pedido de relógio de %s rejeitado; coordenador atual=%s", solicitante, coordenador)
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
		log.Printf("[SERVIDOR] Respondendo eleição OK para %s (solicitante rank=%d, meu rank=%d)", solicitante, rankSolicitante, s.myRank)
		if !s.isCoordenador() {
			s.iniciarEleicaoAsync(fmt.Sprintf("pedido de eleição recebido de %s", solicitante))
		}
		return resposta
	}

	resposta.Status = protos.Status_STATUS_ERRO
	resposta.ErroMsg = "rank inferior"
	resposta.Rank = s.myRank
	log.Printf("[SERVIDOR] Pedido de eleição de %s rejeitado (solicitante rank=%d, meu rank=%d)", solicitante, rankSolicitante, s.myRank)
	return resposta
}

func (s *Servidor) rankServidor(nome string) (int32, bool) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	rank, ok := s.activeServers[nome]
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
	res.Status = protos.Status_STATUS_SUCESSO
	return res
}

func (s *Servidor) processarCreateChannel(req *protos.CreateChannelRequest, aceitarExistente bool) *protos.CreateChannelResponse {
	res := &protos.CreateChannelResponse{}
	nome := strings.TrimSpace(req.GetNomeCanal())
	if nome == "" {
		res.Status = protos.Status_STATUS_ERRO
		res.ErroMsg = "nome de canal vazio"
		return res
	}
	canais := s.lerCanais()
	for _, c := range canais {
		if c == nome {
			if aceitarExistente {
				res.Status = protos.Status_STATUS_SUCESSO
				return res
			}
			res.Status = protos.Status_STATUS_ERRO
			res.ErroMsg = "canal já existe"
			return res
		}
	}
	canais = append(canais, nome)
	s.salvarCanais(canais)
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
	publicarNoProxy bool,
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

	remetente := req.GetCabecalho().GetLinguagemOrigem()
	if remetente == "" && envCabecalho != nil && envCabecalho.GetLinguagemOrigem() != "" {
		remetente = envCabecalho.GetLinguagemOrigem()
	}
	if remetente == "" {
		remetente = "desconhecido"
	}
	cabPub := mensageria.NovoCabecalho(s.origem, s.clock)
	channelMsg := &protos.ChannelMessage{
		Canal:          canal,
		Mensagem:       mensagem,
		Remetente:      remetente,
		TimestampEnvio: cabPub.GetTimestampEnvio(),
		RelogioLogico:  cabPub.GetRelogioLogico(),
	}
	if publicarNoProxy {
		payload, err := proto.Marshal(channelMsg)
		if err != nil {
			res.Status = protos.Status_STATUS_ERRO
			res.ErroMsg = "erro ao serializar publicacao"
			return res
		}
		log.Printf(
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
	}

	s.registrarPublicacao(canal, mensagem, remetente, mensageria.TimestampTexto(req.GetCabecalho().GetTimestampEnvio()))

	res.Status = protos.Status_STATUS_SUCESSO
	return res
}
