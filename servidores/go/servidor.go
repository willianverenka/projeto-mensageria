package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	dataDirDefault     = "/data"
)

func main() {
	orqEndpoint := getEnv("ORQ_ENDPOINT_SERVIDOR", orqEndpointDefault)
	pubEndpoint := getEnv("PROXY_PUB_ENDPOINT", proxyPubEndpoint)
	dataDir := getEnv("DATA_DIR", dataDirDefault)

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
		sock:        sock,
		pubSock:     mustNewPubSocket(pubEndpoint),
		dataDir:     dataDir,
		pubEndpoint: pubEndpoint,
	}
	defer server.pubSock.Close()
	log.Printf("[SERVIDOR] Conectado ao proxy pub em %s", pubEndpoint)
	server.initStorage()
	server.registrarNoOrquestrador()
	server.loop()
}

type Servidor struct {
	sock        *zmq4.Socket
	pubSock     *zmq4.Socket
	dataDir     string
	pubEndpoint string
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

func (s *Servidor) registrarNoOrquestrador() {
	cab := mensageria.NovoCabecalho(mensageria.OrigemLabel(mensageria.RoleServidor))
	req := &protos.ListChannelsRequest{Cabecalho: cab}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_ListChannelsReq{ListChannelsReq: req},
	}
	data, _ := proto.Marshal(env)
	if _, err := s.sock.SendBytes(data, zmq4.DONTWAIT); err != nil {
		log.Printf("[SERVIDOR] Aviso: não foi possível enviar mensagem de registro ao orquestrador: %v", err)
	}
}

func getEnv(name, defaultVal string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return defaultVal
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

		conteudo := env.GetConteudo()
		if conteudo == nil {
			log.Printf("[SERVIDOR] Envelope sem conteúdo")
			continue
		}
		var resp proto.Message
		switch c := conteudo.(type) {
		case *protos.Envelope_LoginReq:
			log.Printf("[SERVIDOR] Servidor go processando mensagem: login_req")
			resp = s.processarLogin(c.LoginReq)
		case *protos.Envelope_CreateChannelReq:
			log.Printf("[SERVIDOR] Servidor go processando mensagem: create_channel_req")
			resp = s.processarCreateChannel(c.CreateChannelReq)
		case *protos.Envelope_ListChannelsReq:
			log.Printf("[SERVIDOR] Servidor go processando mensagem: list_channels_req")
			resp = s.processarListChannels(c.ListChannelsReq)
		case *protos.Envelope_PublishReq:
			log.Printf("[SERVIDOR] Servidor go processando mensagem: publish_req")
			resp = s.processarPublish(c.PublishReq, env.GetCabecalho())
		default:
			log.Printf("[SERVIDOR] Tipo de mensagem não suportado: %T", conteudo)
			continue
		}

		cab := mensageria.NovoCabecalho(mensageria.OrigemLabel(mensageria.RoleServidor))
		respEnv := &protos.Envelope{Cabecalho: cab}
		switch r := resp.(type) {
		case *protos.LoginResponse:
			respEnv.Conteudo = &protos.Envelope_LoginRes{LoginRes: r}
		case *protos.CreateChannelResponse:
			respEnv.Conteudo = &protos.Envelope_CreateChannelRes{CreateChannelRes: r}
		case *protos.ListChannelsResponse:
			respEnv.Conteudo = &protos.Envelope_ListChannelsRes{ListChannelsRes: r}
		case *protos.PublishResponse:
			respEnv.Conteudo = &protos.Envelope_PublishRes{PublishRes: r}
		}
		respData, _ := proto.Marshal(respEnv)
		if _, err := s.sock.SendBytes(respData, 0); err != nil {
			log.Printf("[SERVIDOR] Send: %v", err)
		}
	}
}

func (s *Servidor) processarLogin(req *protos.LoginRequest) *protos.LoginResponse {
	res := &protos.LoginResponse{
		Cabecalho: req.GetCabecalho(),
	}
	nome := strings.TrimSpace(req.GetNomeUsuario())
	if nome == "" {
		res.Status = protos.Status_STATUS_ERRO
		res.ErroMsg = "nome de usuário vazio"
		return res
	}
	ts := req.GetCabecalho().GetTimestampEnvio()
	tsISO := ""
	if ts != nil {
		tsISO = fmt.Sprintf("%d.%09d", ts.GetSeconds(), ts.GetNanos())
	}
	s.registrarLogin(nome, tsISO)
	res.Status = protos.Status_STATUS_SUCESSO
	return res
}

func (s *Servidor) processarCreateChannel(req *protos.CreateChannelRequest) *protos.CreateChannelResponse {
	res := &protos.CreateChannelResponse{
		Cabecalho: req.GetCabecalho(),
	}
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

func (s *Servidor) processarListChannels(req *protos.ListChannelsRequest) *protos.ListChannelsResponse {
	res := &protos.ListChannelsResponse{
		Cabecalho: req.GetCabecalho(),
		Canais:    s.lerCanais(),
	}
	return res
}

func (s *Servidor) processarPublish(
	req *protos.PublishRequest,
	envCabecalho *protos.Cabecalho,
) *protos.PublishResponse {
	res := &protos.PublishResponse{Cabecalho: req.GetCabecalho()}
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
	ts := req.GetCabecalho().GetTimestampEnvio()
	channelMsg := &protos.ChannelMessage{
		Canal:          canal,
		Mensagem:       mensagem,
		Remetente:      remetente,
		TimestampEnvio: ts,
	}
	payload, err := proto.Marshal(channelMsg)
	if err != nil {
		res.Status = protos.Status_STATUS_ERRO
		res.ErroMsg = "erro ao serializar publicacao"
		return res
	}
	if _, err := s.pubSock.SendMessage([]byte(canal), payload); err != nil {
		res.Status = protos.Status_STATUS_ERRO
		res.ErroMsg = "erro ao publicar mensagem"
		return res
	}

	timestampISO := time.Now().UTC().Format(time.RFC3339Nano)
	if ts != nil {
		timestampISO = fmt.Sprintf("%d.%09d", ts.GetSeconds(), ts.GetNanos())
	}
	s.registrarPublicacao(canal, mensagem, remetente, timestampISO)

	res.Status = protos.Status_STATUS_SUCESSO
	return res
}
