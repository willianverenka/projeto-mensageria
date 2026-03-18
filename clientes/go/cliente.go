package main

import (
	"log"
	"os"
	"strings"
	"time"

	"projeto-mensageria/shared/go/mensageria"
	"projeto-mensageria/shared/go/protos"

	"github.com/pebbe/zmq4"
)

const (
	orqEndpointDefault = "tcp://orquestrador:5555"
	nomeUsuarioDefault  = "cliente_go"
	nomeCanalDefault    = "canal_go"
)

func main() {
	orqEndpoint := getEnv("ORQ_ENDPOINT", orqEndpointDefault)
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

	cliente := &Cliente{sock: sock}

	for i := 0; i < 3; i++ {
		if cliente.fazerLogin(nomeUsuario) {
			break
		}
		time.Sleep(1 * time.Second)
	}

	cliente.criarCanal(nomeCanal)
	cliente.listarCanais()

	for i := 0; i < 100; i++ {
		cliente.listarCanais()
		time.Sleep(500 * time.Millisecond)
	}
}

type Cliente struct {
	sock *zmq4.Socket
}

func getEnv(name, defaultVal string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return defaultVal
}

func (c *Cliente) enviarEAguardar(env *protos.Envelope) (*protos.Envelope, error) {
	data, err := mensageria.EnvelopeBytes(env)
	if err != nil {
		return nil, err
	}
	if _, err := c.sock.SendBytes(data, 0); err != nil {
		return nil, err
	}
	reply, err := c.sock.RecvBytes(0)
	if err != nil {
		return nil, err
	}
	return mensageria.EnvelopeFromBytes(reply)
}

func (c *Cliente) fazerLogin(nomeUsuario string) bool {
	cab := mensageria.NovoCabecalho(mensageria.OrigemLabel(mensageria.RoleCliente))
	req := &protos.LoginRequest{
		Cabecalho:   cab,
		NomeUsuario: nomeUsuario,
	}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_LoginReq{LoginReq: req},
	}

	log.Printf("[CLIENTE] Enviando LoginRequest para usuário '%s'", nomeUsuario)
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
	cab := mensageria.NovoCabecalho(mensageria.OrigemLabel(mensageria.RoleCliente))
	req := &protos.CreateChannelRequest{
		Cabecalho: cab,
		NomeCanal: nomeCanal,
	}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_CreateChannelReq{CreateChannelReq: req},
	}

	log.Printf("[CLIENTE] Solicitando criação do canal '%s'", nomeCanal)
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
	cab := mensageria.NovoCabecalho(mensageria.OrigemLabel(mensageria.RoleCliente))
	req := &protos.ListChannelsRequest{Cabecalho: cab}
	env := &protos.Envelope{
		Cabecalho: cab,
		Conteudo:  &protos.Envelope_ListChannelsReq{ListChannelsReq: req},
	}

	log.Printf("[CLIENTE] Solicitando listagem de canais")
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
