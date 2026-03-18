package mensageria

import (
	"sync/atomic"

	"projeto-mensageria/shared/go/protos"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var idCounter int32

func NovoCabecalho(origem string) *protos.Cabecalho {
	return &protos.Cabecalho{
		IdTransacao:     atomic.AddInt32(&idCounter, 1),
		LinguagemOrigem: origem,
		TimestampEnvio:  timestamppb.Now(),
	}
}

func EnvelopeBytes(env *protos.Envelope) ([]byte, error) {
	return proto.Marshal(env)
}

func EnvelopeFromBytes(data []byte) (*protos.Envelope, error) {
	env := &protos.Envelope{}
	if err := proto.Unmarshal(data, env); err != nil {
		return nil, err
	}
	return env, nil
}

func OrigemLabel(role string) string {
	return "go-" + role
}

const (
	RoleCliente  = "cliente"
	RoleServidor = "servidor"
)
