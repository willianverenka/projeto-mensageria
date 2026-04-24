package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"projeto-mensageria/shared/go/mensageria"
	"projeto-mensageria/shared/go/protos"

	"github.com/pebbe/zmq4"
	"google.golang.org/protobuf/proto"
)

const (
	orqEndpointDefault = "tcp://orquestrador:5555"
	proxySubEndpoint   = "tcp://proxy:5558"
	nomeUsuarioDefault = "cliente_go"
	nomeCanalDefault   = "canal_go"
)

func main() {
	orqEndpoint := getEnv("ORQ_ENDPOINT", orqEndpointDefault)
	subEndpoint := getEnv("PROXY_SUB_ENDPOINT", proxySubEndpoint)
	nomeUsuario := getEnv("CLIENTE_NOME", nomeUsuarioDefault)
	nomeCanal := getEnv("CLIENTE_CANAL", nomeCanalDefault)

	sock, err := zmq4.NewSocket(zmq4.DEALER)
	if err != nil {
		log.Fatalf("Novo socket: %v", err)
	}
	defer sock.Close()

	if err := sock.Connect(orqEndpoint); err != nil {
		log.Fatalf("Conectar ao orquestrador: %v", err)
	}
	log.Printf("[CLIENTE] Conectado ao orquestrador em %s", orqEndpoint)

	subSock, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		log.Fatalf("Novo socket SUB: %v", err)
	}
	defer subSock.Close()
	if err := subSock.Connect(subEndpoint); err != nil {
		log.Fatalf("Conectar SUB ao proxy: %v", err)
	}
	log.Printf("[CLIENTE] Conectado ao proxy sub em %s", subEndpoint)

	cliente := &Cliente{
		sock:       sock,
		subSock:    subSock,
		subscribed: map[string]bool{},
		clock:      &mensageria.RelogioProcesso{},
		origem:     mensageria.OrigemLabel(mensageria.RoleCliente),
	}

	for i := 0; i < 3; i++ {
		if cliente.fazerLogin(nomeUsuario) {
			break
		}
		time.Sleep(1 * time.Second)
	}

	cliente.iniciarReceptor()
	canais := cliente.listarCanais()
	if len(canais) < 5 {
		novoCanal := fmt.Sprintf("%s_%d", nomeCanal, rand.Intn(10000))
		cliente.criarCanal(novoCanal)
		canais = cliente.listarCanais()
	}
	for len(cliente.subscribed) < 3 && len(cliente.subscribed) < len(canais) {
		escolhido := canais[rand.Intn(len(canais))]
		cliente.inscrever(escolhido)
	}
	for {
		canais = cliente.listarCanais()
		if len(canais) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}
		canal := canais[rand.Intn(len(canais))]
		for i := 0; i < 10; i++ {
			_ = cliente.publicar(canal, randomMensagem(12))
			time.Sleep(1 * time.Second)
		}
	}
}

type Cliente struct {
	sock       *zmq4.Socket
	subSock    *zmq4.Socket
	subscribed map[string]bool
	clock      *mensageria.RelogioProcesso
	origem     string
	mu         sync.Mutex
}

func getEnv(name, defaultVal string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return defaultVal
}

func agoraTexto() string {
	agora := time.Now().UnixNano()
	return fmt.Sprintf("%d.%09d", agora/1_000_000_000, agora%1_000_000_000)
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
	default:
		return "desconhecido"
	}
}

func (c *Cliente) enviarEAguardar(env *protos.Envelope) (*protos.Envelope, error) {
	data, err := mensageria.EnvelopeBytes(env)
	if err != nil {
		return nil, err
	}
	log.Printf("[CLIENTE] Enviando %s %s", conteudoNome(env), mensageria.CabecalhoTexto(env.GetCabecalho()))
	if _, err := c.sock.SendBytes(data, 0); err != nil {
		return nil, err
	}
	reply, err := c.sock.RecvBytes(0)
	if err != nil {
		return nil, err
	}
	respEnv, err := mensageria.EnvelopeFromBytes(reply)
	if err != nil {
		return nil, err
	}
	c.clock.OnReceive(respEnv.GetCabecalho().GetRelogioLogico())
	log.Printf("[CLIENTE] Recebido %T %s", respEnv.GetConteudo(), mensageria.CabecalhoTexto(respEnv.GetCabecalho()))
	return respEnv, nil
}

func (c *Cliente) fazerLogin(nomeUsuario string) bool {
	cab := mensageria.NovoCabecalho(c.origem, c.clock)
	req := &protos.LoginRequest{
		Cabecalho:   cab,
		NomeUsuario: nomeUsuario,
	}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_LoginReq{LoginReq: req},
	}

	respEnv, err := c.enviarEAguardar(env)
	if err != nil {
		log.Printf("[CLIENTE] Erro ao enviar/aguardar login: %v", err)
		return false
	}

	_, ok := respEnv.GetConteudo().(*protos.Envelope_LoginRes)
	if !ok {
		log.Printf("[CLIENTE] Resposta inesperada ao login: %T", respEnv.GetConteudo())
		return false
	}
	res := respEnv.GetLoginRes()
	if res.GetStatus() == protos.Status_STATUS_SUCESSO {
		log.Printf("[CLIENTE] Login bem-sucedido para '%s'", nomeUsuario)
		return true
	}
	log.Printf("[CLIENTE] Falha no login: %s", res.GetErroMsg())
	return false
}

func (c *Cliente) criarCanal(nomeCanal string) {
	cab := mensageria.NovoCabecalho(c.origem, c.clock)
	req := &protos.CreateChannelRequest{
		Cabecalho: cab,
		NomeCanal: nomeCanal,
	}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_CreateChannelReq{CreateChannelReq: req},
	}

	respEnv, err := c.enviarEAguardar(env)
	if err != nil {
		log.Printf("[CLIENTE] Erro ao criar canal: %v", err)
		return
	}
	_, ok := respEnv.GetConteudo().(*protos.Envelope_CreateChannelRes)
	if !ok {
		log.Printf("[CLIENTE] Resposta inesperada a criação de canal: %T", respEnv.GetConteudo())
		return
	}
	res := respEnv.GetCreateChannelRes()
	if res.GetStatus() == protos.Status_STATUS_SUCESSO {
		log.Printf("[CLIENTE] Canal '%s' criado com sucesso", nomeCanal)
	} else {
		log.Printf("[CLIENTE] Falha ao criar canal '%s': %s", nomeCanal, res.GetErroMsg())
	}
}

func (c *Cliente) listarCanais() []string {
	cab := mensageria.NovoCabecalho(c.origem, c.clock)
	req := &protos.ListChannelsRequest{Cabecalho: cab}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_ListChannelsReq{ListChannelsReq: req},
	}

	respEnv, err := c.enviarEAguardar(env)
	if err != nil {
		log.Printf("[CLIENTE] Erro ao listar canais: %v", err)
		return nil
	}
	_, ok := respEnv.GetConteudo().(*protos.Envelope_ListChannelsRes)
	if !ok {
		log.Printf("[CLIENTE] Resposta inesperada a listagem de canais: %T", respEnv.GetConteudo())
		return nil
	}
	res := respEnv.GetListChannelsRes()
	canais := res.GetCanais()
	if len(canais) == 0 {
		log.Printf("[CLIENTE] Canais existentes: (nenhum)")
	} else {
		log.Printf("[CLIENTE] Canais existentes: %s", strings.Join(canais, ", "))
	}
	return canais
}

func (c *Cliente) publicar(canal, mensagem string) bool {
	cab := mensageria.NovoCabecalho(c.origem, c.clock)
	req := &protos.PublishRequest{Cabecalho: cab, Canal: canal, Mensagem: mensagem}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_PublishReq{PublishReq: req},
	}
	respEnv, err := c.enviarEAguardar(env)
	if err != nil {
		log.Printf("[CLIENTE] Erro ao publicar: %v", err)
		return false
	}
	res, ok := respEnv.GetConteudo().(*protos.Envelope_PublishRes)
	if !ok {
		log.Printf("[CLIENTE] Resposta inesperada ao publicar: %T", respEnv.GetConteudo())
		return false
	}
	if res.PublishRes.GetStatus() != protos.Status_STATUS_SUCESSO {
		log.Printf("[CLIENTE] Falha ao publicar em '%s': %s", canal, res.PublishRes.GetErroMsg())
		return false
	}
	return true
}

func (c *Cliente) inscrever(canal string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subscribed[canal] {
		return
	}
	if err := c.subSock.SetSubscribe(canal); err != nil {
		log.Printf("[CLIENTE] Erro ao inscrever no canal '%s': %v", canal, err)
		return
	}
	c.subscribed[canal] = true
	log.Printf("[CLIENTE] Inscrito no canal '%s'", canal)
}

func (c *Cliente) iniciarReceptor() {
	go func() {
		for {
			msg, err := c.subSock.RecvMessageBytes(0)
			if err != nil || len(msg) < 2 {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			channelMsg := &protos.ChannelMessage{}
			err = proto.Unmarshal(msg[1], channelMsg)
			if err != nil {
				continue
			}
			c.clock.OnReceive(channelMsg.GetRelogioLogico())
			log.Printf(
				"[CLIENTE] [CANAL=%s] msg='%s' remetente=%s envio=%s recebimento=%s relogio=%d",
				string(msg[0]),
				channelMsg.GetMensagem(),
				channelMsg.GetRemetente(),
				mensageria.TimestampTexto(channelMsg.GetTimestampEnvio()),
				agoraTexto(),
				channelMsg.GetRelogioLogico(),
			)
		}
	}()
}

func randomMensagem(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}
