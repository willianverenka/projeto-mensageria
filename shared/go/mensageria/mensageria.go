package mensageria

import (
	"fmt"
	"sync/atomic"
	"time"

	"projeto-mensageria/shared/go/protos"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var idCounter int32

type RelogioProcesso struct {
	logical  atomic.Int64
	offsetNs atomic.Int64
}

func (r *RelogioProcesso) BeforeSend() int64 {
	return r.logical.Add(1)
}

func (r *RelogioProcesso) OnReceive(recebido int64) int64 {
	for {
		atual := r.logical.Load()
		if recebido <= atual {
			return atual
		}
		if r.logical.CompareAndSwap(atual, recebido) {
			return recebido
		}
	}
}

func (r *RelogioProcesso) Valor() int64 {
	return r.logical.Load()
}

func (r *RelogioProcesso) NowCorrigido() *timestamppb.Timestamp {
	ns := time.Now().UnixNano() + r.offsetNs.Load()
	return timestamppb.New(time.Unix(0, ns).UTC())
}

func (r *RelogioProcesso) AtualizarOffset(ref *timestamppb.Timestamp, envioNs, recebimentoNs int64) int64 {
	if ref == nil {
		return r.offsetNs.Load()
	}
	pontoMedioNs := (envioNs + recebimentoNs) / 2
	offset := ref.AsTime().UnixNano() - pontoMedioNs
	r.offsetNs.Store(offset)
	return offset
}

func (r *RelogioProcesso) OffsetMillis() float64 {
	return float64(r.offsetNs.Load()) / 1_000_000.0
}

func TimestampTexto(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return "0.000000000"
	}
	return fmt.Sprintf("%d.%09d", ts.GetSeconds(), ts.GetNanos())
}

func CabecalhoTexto(cab *protos.Cabecalho) string {
	if cab == nil {
		return "tx=0 origem=desconhecida ts=0.000000000 relogio=0"
	}
	return fmt.Sprintf(
		"tx=%d origem=%s ts=%s relogio=%d",
		cab.GetIdTransacao(),
		cab.GetLinguagemOrigem(),
		TimestampTexto(cab.GetTimestampEnvio()),
		cab.GetRelogioLogico(),
	)
}

func NovoCabecalho(origem string, relogio *RelogioProcesso) *protos.Cabecalho {
	timestamp := timestamppb.Now()
	relogioLogico := int64(0)
	if relogio != nil {
		timestamp = relogio.NowCorrigido()
		relogioLogico = relogio.BeforeSend()
	}
	return &protos.Cabecalho{
		IdTransacao:     atomic.AddInt32(&idCounter, 1),
		LinguagemOrigem: origem,
		TimestampEnvio:  timestamp,
		RelogioLogico:   relogioLogico,
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
	RoleCliente    = "cliente"
	RoleServidor   = "servidor"
	RoleReferencia = "referencia"
)
