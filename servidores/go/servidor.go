package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"projeto-mensageria/shared/go/mensageria"
	"projeto-mensageria/shared/go/protos"

	"github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"
)

const (
	orqEndpointDefault = "tcp://orquestrador:5556"
	proxyPubEndpoint   = "tcp://proxy:5557"
	referenceEndpoint  = "tcp://referencia:5559"
	dataDirDefault     = "/data"
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
		refSock:           mustNewReqSocket(refEndpoint),
		dataDir:           dataDir,
		pubEndpoint:       pubEndpoint,
		referenceEndpoint: refEndpoint,
		serverName:        serverName,
		clock:             &mensageria.RelogioProcesso{},
		origem:            mensageria.OrigemLabel(mensageria.RoleServidor),
	}
	defer server.pubSock.Close()
	defer server.refSock.Close()
	log.Printf("[SERVIDOR] Conectado ao proxy pub em %s", pubEndpoint)
	log.Printf("[SERVIDOR] Conectado à referência em %s", refEndpoint)
	server.initStorage()
	server.registrarNaReferencia()
	server.listarServidoresReferencia()
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
	if err := sock.Connect(endpoint); err != nil {
		log.Fatalf("Conectar REQ à referência: %v", err)
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
	cab := mensageria.NovoCabecalho(s.origem, s.clock)
	req := &protos.RegisterServerRequest{Cabecalho: cab, NomeServidor: s.serverName}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_RegisterServerReq{RegisterServerReq: req},
	}
	resp, err := s.enviarParaReferencia(env, true)
	if err != nil {
		log.Fatalf("[SERVIDOR] Erro ao registrar na referência: %v", err)
	}
	res, ok := resp.GetConteudo().(*protos.Envelope_RegisterServerRes)
	if !ok {
		log.Fatalf("[SERVIDOR] Resposta inesperada ao registrar na referência: %T", resp.GetConteudo())
	}
	if res.RegisterServerRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		log.Fatalf("[SERVIDOR] Falha ao registrar na referência: %s", res.RegisterServerRes.GetErroMsg())
	}
	log.Printf("[SERVIDOR] Servidor %s registrado na referência com rank=%d", s.serverName, res.RegisterServerRes.GetRank())
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
		return
	}
	log.Printf("[SERVIDOR] Heartbeat enviado com sucesso para %s", s.serverName)
	s.listarServidoresReferencia()
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
	if atualizarOffset {
		s.clock.AtualizarOffset(resp.GetCabecalho().GetTimestampEnvio(), envioNs, recebimentoNs)
		log.Printf("[SERVIDOR] Offset físico atualizado para %.3f ms", s.clock.OffsetMillis())
	}
	log.Printf("[SERVIDOR] Recebido %s da referência %s", conteudoNome(resp), mensageria.CabecalhoTexto(resp.GetCabecalho()))
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
		log.Printf("[SERVIDOR] Recebido %s %s", conteudoNome(env), mensageria.CabecalhoTexto(env.GetCabecalho()))

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
		log.Printf("[SERVIDOR] Enviando %s %s", conteudoNome(respEnv), mensageria.CabecalhoTexto(respEnv.GetCabecalho()))
		if _, err := s.sock.SendBytes(respData, 0); err != nil {
			log.Printf("[SERVIDOR] Send: %v", err)
			continue
		}

		s.requestsCount++
		if s.requestsCount%10 == 0 {
			s.heartbeatESincronizar()
		}
	}
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

func (s *Servidor) processarCreateChannel(req *protos.CreateChannelRequest) *protos.CreateChannelResponse {
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

	s.registrarPublicacao(canal, mensagem, remetente, mensageria.TimestampTexto(channelMsg.GetTimestampEnvio()))

	res.Status = protos.Status_STATUS_SUCESSO
	return res
}
