package protos

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Status int32

const (
	Status_STATUS_SUCESSO Status = 0
	Status_STATUS_ERRO    Status = 1
)

var (
	Status_name = map[int32]string{
		0: "STATUS_SUCESSO",
		1: "STATUS_ERRO",
	}
	Status_value = map[string]int32{
		"STATUS_SUCESSO": 0,
		"STATUS_ERRO":    1,
	}
)

func (x Status) Enum() *Status {
	p := new(Status)
	*p = x
	return p
}

func (x Status) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Status) Descriptor() protoreflect.EnumDescriptor {
	return file_contrato_proto_enumTypes[0].Descriptor()
}

func (Status) Type() protoreflect.EnumType {
	return &file_contrato_proto_enumTypes[0]
}

func (x Status) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

func (Status) EnumDescriptor() ([]byte, []int) {
	return file_contrato_proto_rawDescGZIP(), []int{0}
}

type Cabecalho struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	IdTransacao     int32                  `protobuf:"varint,1,opt,name=id_transacao,json=idTransacao,proto3" json:"id_transacao,omitempty"`
	LinguagemOrigem string                 `protobuf:"bytes,2,opt,name=linguagem_origem,json=linguagemOrigem,proto3" json:"linguagem_origem,omitempty"`
	TimestampEnvio  *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=timestamp_envio,json=timestampEnvio,proto3" json:"timestamp_envio,omitempty"`
}

func (x *Cabecalho) Reset() {
	*x = Cabecalho{}
	if protoimpl.UnsafeEnabled {
		mi := &file_contrato_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Cabecalho) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Cabecalho) ProtoMessage() {}

func (x *Cabecalho) ProtoReflect() protoreflect.Message {
	mi := &file_contrato_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*Cabecalho) Descriptor() ([]byte, []int) {
	return file_contrato_proto_rawDescGZIP(), []int{0}
}

func (x *Cabecalho) GetIdTransacao() int32 {
	if x != nil {
		return x.IdTransacao
	}
	return 0
}

func (x *Cabecalho) GetLinguagemOrigem() string {
	if x != nil {
		return x.LinguagemOrigem
	}
	return ""
}

func (x *Cabecalho) GetTimestampEnvio() *timestamppb.Timestamp {
	if x != nil {
		return x.TimestampEnvio
	}
	return nil
}

type LoginRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Cabecalho   *Cabecalho `protobuf:"bytes,1,opt,name=cabecalho,proto3" json:"cabecalho,omitempty"`
	NomeUsuario string     `protobuf:"bytes,2,opt,name=nome_usuario,json=nomeUsuario,proto3" json:"nome_usuario,omitempty"`
}

func (x *LoginRequest) Reset() {
	*x = LoginRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_contrato_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LoginRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LoginRequest) ProtoMessage() {}

func (x *LoginRequest) ProtoReflect() protoreflect.Message {
	mi := &file_contrato_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*LoginRequest) Descriptor() ([]byte, []int) {
	return file_contrato_proto_rawDescGZIP(), []int{1}
}

func (x *LoginRequest) GetCabecalho() *Cabecalho {
	if x != nil {
		return x.Cabecalho
	}
	return nil
}

func (x *LoginRequest) GetNomeUsuario() string {
	if x != nil {
		return x.NomeUsuario
	}
	return ""
}

type LoginResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Cabecalho *Cabecalho `protobuf:"bytes,1,opt,name=cabecalho,proto3" json:"cabecalho,omitempty"`
	Status    Status     `protobuf:"varint,2,opt,name=status,proto3,enum=chat.Status" json:"status,omitempty"`
	ErroMsg   string     `protobuf:"bytes,3,opt,name=erro_msg,json=erroMsg,proto3" json:"erro_msg,omitempty"`
}

func (x *LoginResponse) Reset() {
	*x = LoginResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_contrato_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LoginResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LoginResponse) ProtoMessage() {}

func (x *LoginResponse) ProtoReflect() protoreflect.Message {
	mi := &file_contrato_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*LoginResponse) Descriptor() ([]byte, []int) {
	return file_contrato_proto_rawDescGZIP(), []int{2}
}

func (x *LoginResponse) GetCabecalho() *Cabecalho {
	if x != nil {
		return x.Cabecalho
	}
	return nil
}

func (x *LoginResponse) GetStatus() Status {
	if x != nil {
		return x.Status
	}
	return Status_STATUS_SUCESSO
}

func (x *LoginResponse) GetErroMsg() string {
	if x != nil {
		return x.ErroMsg
	}
	return ""
}

type CreateChannelRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Cabecalho *Cabecalho `protobuf:"bytes,1,opt,name=cabecalho,proto3" json:"cabecalho,omitempty"`
	NomeCanal string     `protobuf:"bytes,2,opt,name=nome_canal,json=nomeCanal,proto3" json:"nome_canal,omitempty"`
}

func (x *CreateChannelRequest) Reset() {
	*x = CreateChannelRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_contrato_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateChannelRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateChannelRequest) ProtoMessage() {}

func (x *CreateChannelRequest) ProtoReflect() protoreflect.Message {
	mi := &file_contrato_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*CreateChannelRequest) Descriptor() ([]byte, []int) {
	return file_contrato_proto_rawDescGZIP(), []int{3}
}

func (x *CreateChannelRequest) GetCabecalho() *Cabecalho {
	if x != nil {
		return x.Cabecalho
	}
	return nil
}

func (x *CreateChannelRequest) GetNomeCanal() string {
	if x != nil {
		return x.NomeCanal
	}
	return ""
}

type CreateChannelResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Cabecalho *Cabecalho `protobuf:"bytes,1,opt,name=cabecalho,proto3" json:"cabecalho,omitempty"`
	Status    Status     `protobuf:"varint,2,opt,name=status,proto3,enum=chat.Status" json:"status,omitempty"`
	ErroMsg   string     `protobuf:"bytes,3,opt,name=erro_msg,json=erroMsg,proto3" json:"erro_msg,omitempty"`
}

func (x *CreateChannelResponse) Reset() {
	*x = CreateChannelResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_contrato_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateChannelResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateChannelResponse) ProtoMessage() {}

func (x *CreateChannelResponse) ProtoReflect() protoreflect.Message {
	mi := &file_contrato_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*CreateChannelResponse) Descriptor() ([]byte, []int) {
	return file_contrato_proto_rawDescGZIP(), []int{4}
}

func (x *CreateChannelResponse) GetCabecalho() *Cabecalho {
	if x != nil {
		return x.Cabecalho
	}
	return nil
}

func (x *CreateChannelResponse) GetStatus() Status {
	if x != nil {
		return x.Status
	}
	return Status_STATUS_SUCESSO
}

func (x *CreateChannelResponse) GetErroMsg() string {
	if x != nil {
		return x.ErroMsg
	}
	return ""
}

type ListChannelsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Cabecalho *Cabecalho `protobuf:"bytes,1,opt,name=cabecalho,proto3" json:"cabecalho,omitempty"`
}

func (x *ListChannelsRequest) Reset() {
	*x = ListChannelsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_contrato_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListChannelsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListChannelsRequest) ProtoMessage() {}

func (x *ListChannelsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_contrato_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ListChannelsRequest) Descriptor() ([]byte, []int) {
	return file_contrato_proto_rawDescGZIP(), []int{5}
}

func (x *ListChannelsRequest) GetCabecalho() *Cabecalho {
	if x != nil {
		return x.Cabecalho
	}
	return nil
}

type ListChannelsResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Cabecalho *Cabecalho `protobuf:"bytes,1,opt,name=cabecalho,proto3" json:"cabecalho,omitempty"`
	Canais    []string   `protobuf:"bytes,2,rep,name=canais,proto3" json:"canais,omitempty"`
}

func (x *ListChannelsResponse) Reset() {
	*x = ListChannelsResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_contrato_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListChannelsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListChannelsResponse) ProtoMessage() {}

func (x *ListChannelsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_contrato_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*ListChannelsResponse) Descriptor() ([]byte, []int) {
	return file_contrato_proto_rawDescGZIP(), []int{6}
}

func (x *ListChannelsResponse) GetCabecalho() *Cabecalho {
	if x != nil {
		return x.Cabecalho
	}
	return nil
}

func (x *ListChannelsResponse) GetCanais() []string {
	if x != nil {
		return x.Canais
	}
	return nil
}

type Envelope struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Cabecalho *Cabecalho            `protobuf:"bytes,1,opt,name=cabecalho,proto3" json:"cabecalho,omitempty"`
	Conteudo  isEnvelope_Conteudo   `protobuf_oneof:"conteudo"`
}

func (x *Envelope) Reset() {
	*x = Envelope{}
	if protoimpl.UnsafeEnabled {
		mi := &file_contrato_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Envelope) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Envelope) ProtoMessage() {}

func (x *Envelope) ProtoReflect() protoreflect.Message {
	mi := &file_contrato_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

func (*Envelope) Descriptor() ([]byte, []int) {
	return file_contrato_proto_rawDescGZIP(), []int{7}
}

func (x *Envelope) GetCabecalho() *Cabecalho {
	if x != nil {
		return x.Cabecalho
	}
	return nil
}

func (m *Envelope) GetConteudo() isEnvelope_Conteudo {
	if m != nil {
		return m.Conteudo
	}
	return nil
}

func (x *Envelope) GetLoginReq() *LoginRequest {
	if x, ok := x.GetConteudo().(*Envelope_LoginReq); ok {
		return x.LoginReq
	}
	return nil
}

func (x *Envelope) GetLoginRes() *LoginResponse {
	if x, ok := x.GetConteudo().(*Envelope_LoginRes); ok {
		return x.LoginRes
	}
	return nil
}

func (x *Envelope) GetCreateChannelReq() *CreateChannelRequest {
	if x, ok := x.GetConteudo().(*Envelope_CreateChannelReq); ok {
		return x.CreateChannelReq
	}
	return nil
}

func (x *Envelope) GetCreateChannelRes() *CreateChannelResponse {
	if x, ok := x.GetConteudo().(*Envelope_CreateChannelRes); ok {
		return x.CreateChannelRes
	}
	return nil
}

func (x *Envelope) GetListChannelsReq() *ListChannelsRequest {
	if x, ok := x.GetConteudo().(*Envelope_ListChannelsReq); ok {
		return x.ListChannelsReq
	}
	return nil
}

func (x *Envelope) GetListChannelsRes() *ListChannelsResponse {
	if x, ok := x.GetConteudo().(*Envelope_ListChannelsRes); ok {
		return x.ListChannelsRes
	}
	return nil
}

type isEnvelope_Conteudo interface {
	isEnvelope_Conteudo()
}

type Envelope_LoginReq struct {
	LoginReq *LoginRequest `protobuf:"bytes,2,opt,name=login_req,json=loginReq,proto3,oneof"`
}

type Envelope_LoginRes struct {
	LoginRes *LoginResponse `protobuf:"bytes,3,opt,name=login_res,json=loginRes,proto3,oneof"`
}

type Envelope_CreateChannelReq struct {
	CreateChannelReq *CreateChannelRequest `protobuf:"bytes,4,opt,name=create_channel_req,json=createChannelReq,proto3,oneof"`
}

type Envelope_CreateChannelRes struct {
	CreateChannelRes *CreateChannelResponse `protobuf:"bytes,5,opt,name=create_channel_res,json=createChannelRes,proto3,oneof"`
}

type Envelope_ListChannelsReq struct {
	ListChannelsReq *ListChannelsRequest `protobuf:"bytes,6,opt,name=list_channels_req,json=listChannelsReq,proto3,oneof"`
}

type Envelope_ListChannelsRes struct {
	ListChannelsRes *ListChannelsResponse `protobuf:"bytes,7,opt,name=list_channels_res,json=listChannelsRes,proto3,oneof"`
}

func (*Envelope_LoginReq) isEnvelope_Conteudo() {}

func (*Envelope_LoginRes) isEnvelope_Conteudo() {}

func (*Envelope_CreateChannelReq) isEnvelope_Conteudo() {}

func (*Envelope_CreateChannelRes) isEnvelope_Conteudo() {}

func (*Envelope_ListChannelsReq) isEnvelope_Conteudo() {}

func (*Envelope_ListChannelsRes) isEnvelope_Conteudo() {}

var File_contrato_proto protoreflect.FileDescriptor

var file_contrato_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x61, 0x74, 0x6f, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x04, 0x63, 0x68, 0x61, 0x74, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x9e, 0x01, 0x0a, 0x09, 0x43, 0x61, 0x62, 0x65,
	0x63, 0x61, 0x6c, 0x68, 0x6f, 0x12, 0x21, 0x0a, 0x0c, 0x69, 0x64, 0x5f, 0x74, 0x72, 0x61, 0x6e,
	0x73, 0x61, 0x63, 0x61, 0x6f, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0b, 0x69, 0x64, 0x54,
	0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x61, 0x6f, 0x12, 0x29, 0x0a, 0x10, 0x6c, 0x69, 0x6e, 0x67,
	0x75, 0x61, 0x67, 0x65, 0x6d, 0x5f, 0x6f, 0x72, 0x69, 0x67, 0x65, 0x6d, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0f, 0x6c, 0x69, 0x6e, 0x67, 0x75, 0x61, 0x67, 0x65, 0x6d, 0x4f, 0x72, 0x69,
	0x67, 0x65, 0x6d, 0x12, 0x43, 0x0a, 0x0f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
	0x5f, 0x65, 0x6e, 0x76, 0x69, 0x6f, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0e, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x45, 0x6e, 0x76, 0x69, 0x6f, 0x22, 0x60, 0x0a, 0x0c, 0x4c, 0x6f, 0x67, 0x69,
	0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x2d, 0x0a, 0x09, 0x63, 0x61, 0x62, 0x65,
	0x63, 0x61, 0x6c, 0x68, 0x6f, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x63, 0x68,
	0x61, 0x74, 0x2e, 0x43, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x52, 0x09, 0x63, 0x61,
	0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x12, 0x21, 0x0a, 0x0c, 0x6e, 0x6f, 0x6d, 0x65, 0x5f,
	0x75, 0x73, 0x75, 0x61, 0x72, 0x69, 0x6f, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x6e,
	0x6f, 0x6d, 0x65, 0x55, 0x73, 0x75, 0x61, 0x72, 0x69, 0x6f, 0x22, 0x7f, 0x0a, 0x0d, 0x4c, 0x6f,
	0x67, 0x69, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2d, 0x0a, 0x09, 0x63,
	0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f,
	0x2e, 0x63, 0x68, 0x61, 0x74, 0x2e, 0x43, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x52,
	0x09, 0x63, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x12, 0x24, 0x0a, 0x06, 0x73, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0c, 0x2e, 0x63, 0x68, 0x61,
	0x74, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x12, 0x19, 0x0a, 0x08, 0x65, 0x72, 0x72, 0x6f, 0x5f, 0x6d, 0x73, 0x67, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x07, 0x65, 0x72, 0x72, 0x6f, 0x4d, 0x73, 0x67, 0x22, 0x64, 0x0a, 0x14, 0x43,
	0x72, 0x65, 0x61, 0x74, 0x65, 0x43, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x2d, 0x0a, 0x09, 0x63, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x63, 0x68, 0x61, 0x74, 0x2e, 0x43, 0x61,
	0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x52, 0x09, 0x63, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c,
	0x68, 0x6f, 0x12, 0x1d, 0x0a, 0x0a, 0x6e, 0x6f, 0x6d, 0x65, 0x5f, 0x63, 0x61, 0x6e, 0x61, 0x6c,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x6e, 0x6f, 0x6d, 0x65, 0x43, 0x61, 0x6e, 0x61,
	0x6c, 0x22, 0x87, 0x01, 0x0a, 0x15, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x43, 0x68, 0x61, 0x6e,
	0x6e, 0x65, 0x6c, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2d, 0x0a, 0x09, 0x63,
	0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f,
	0x2e, 0x63, 0x68, 0x61, 0x74, 0x2e, 0x43, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x52,
	0x09, 0x63, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x12, 0x24, 0x0a, 0x06, 0x73, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0c, 0x2e, 0x63, 0x68, 0x61,
	0x74, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x12, 0x19, 0x0a, 0x08, 0x65, 0x72, 0x72, 0x6f, 0x5f, 0x6d, 0x73, 0x67, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x07, 0x65, 0x72, 0x72, 0x6f, 0x4d, 0x73, 0x67, 0x22, 0x44, 0x0a, 0x13, 0x4c,
	0x69, 0x73, 0x74, 0x43, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x2d, 0x0a, 0x09, 0x63, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x63, 0x68, 0x61, 0x74, 0x2e, 0x43, 0x61, 0x62,
	0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x52, 0x09, 0x63, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68,
	0x6f, 0x22, 0x5d, 0x0a, 0x14, 0x4c, 0x69, 0x73, 0x74, 0x43, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c,
	0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2d, 0x0a, 0x09, 0x63, 0x61, 0x62,
	0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x63,
	0x68, 0x61, 0x74, 0x2e, 0x43, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x52, 0x09, 0x63,
	0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x12, 0x16, 0x0a, 0x06, 0x63, 0x61, 0x6e, 0x61,
	0x69, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x06, 0x63, 0x61, 0x6e, 0x61, 0x69, 0x73,
	0x22, 0xd8, 0x03, 0x0a, 0x08, 0x45, 0x6e, 0x76, 0x65, 0x6c, 0x6f, 0x70, 0x65, 0x12, 0x2d, 0x0a,
	0x09, 0x63, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x0f, 0x2e, 0x63, 0x68, 0x61, 0x74, 0x2e, 0x43, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68,
	0x6f, 0x52, 0x09, 0x63, 0x61, 0x62, 0x65, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x12, 0x31, 0x0a, 0x09,
	0x6c, 0x6f, 0x67, 0x69, 0x6e, 0x5f, 0x72, 0x65, 0x71, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x12, 0x2e, 0x63, 0x68, 0x61, 0x74, 0x2e, 0x4c, 0x6f, 0x67, 0x69, 0x6e, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x48, 0x00, 0x52, 0x08, 0x6c, 0x6f, 0x67, 0x69, 0x6e, 0x52, 0x65, 0x71, 0x12,
	0x32, 0x0a, 0x09, 0x6c, 0x6f, 0x67, 0x69, 0x6e, 0x5f, 0x72, 0x65, 0x73, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x13, 0x2e, 0x63, 0x68, 0x61, 0x74, 0x2e, 0x4c, 0x6f, 0x67, 0x69, 0x6e, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x48, 0x00, 0x52, 0x08, 0x6c, 0x6f, 0x67, 0x69, 0x6e,
	0x52, 0x65, 0x73, 0x12, 0x4a, 0x0a, 0x12, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x5f, 0x63, 0x68,
	0x61, 0x6e, 0x6e, 0x65, 0x6c, 0x5f, 0x72, 0x65, 0x71, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x1a, 0x2e, 0x63, 0x68, 0x61, 0x74, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x43, 0x68, 0x61,
	0x6e, 0x6e, 0x65, 0x6c, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x48, 0x00, 0x52, 0x10, 0x63,
	0x72, 0x65, 0x61, 0x74, 0x65, 0x43, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c, 0x52, 0x65, 0x71, 0x12,
	0x4b, 0x0a, 0x12, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x5f, 0x63, 0x68, 0x61, 0x6e, 0x6e, 0x65,
	0x6c, 0x5f, 0x72, 0x65, 0x73, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x63, 0x68,
	0x61, 0x74, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x43, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x48, 0x00, 0x52, 0x10, 0x63, 0x72, 0x65, 0x61,
	0x74, 0x65, 0x43, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c, 0x52, 0x65, 0x73, 0x12, 0x47, 0x0a, 0x11,
	0x6c, 0x69, 0x73, 0x74, 0x5f, 0x63, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c, 0x73, 0x5f, 0x72, 0x65,
	0x71, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x63, 0x68, 0x61, 0x74, 0x2e, 0x4c,
	0x69, 0x73, 0x74, 0x43, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x48, 0x00, 0x52, 0x0f, 0x6c, 0x69, 0x73, 0x74, 0x43, 0x68, 0x61, 0x6e, 0x6e, 0x65,
	0x6c, 0x73, 0x52, 0x65, 0x71, 0x12, 0x48, 0x0a, 0x11, 0x6c, 0x69, 0x73, 0x74, 0x5f, 0x63, 0x68,
	0x61, 0x6e, 0x6e, 0x65, 0x6c, 0x73, 0x5f, 0x72, 0x65, 0x73, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x1a, 0x2e, 0x63, 0x68, 0x61, 0x74, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x43, 0x68, 0x61, 0x6e,
	0x6e, 0x65, 0x6c, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x48, 0x00, 0x52, 0x0f,
	0x6c, 0x69, 0x73, 0x74, 0x43, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c, 0x73, 0x52, 0x65, 0x73, 0x42,
	0x0a, 0x0a, 0x08, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x75, 0x64, 0x6f, 0x2a, 0x2d, 0x0a, 0x06, 0x53,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x12, 0x0a, 0x0e, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f,
	0x53, 0x55, 0x43, 0x45, 0x53, 0x53, 0x4f, 0x10, 0x00, 0x12, 0x0f, 0x0a, 0x0b, 0x53, 0x54, 0x41,
	0x54, 0x55, 0x53, 0x5f, 0x45, 0x52, 0x52, 0x4f, 0x10, 0x01, 0x42, 0x25, 0x5a, 0x23, 0x70, 0x72,
	0x6f, 0x6a, 0x65, 0x74, 0x6f, 0x2d, 0x6d, 0x65, 0x6e, 0x73, 0x61, 0x67, 0x65, 0x72, 0x69, 0x61,
	0x2f, 0x73, 0x68, 0x61, 0x72, 0x65, 0x64, 0x2f, 0x67, 0x6f, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_contrato_proto_rawDescOnce sync.Once
	file_contrato_proto_rawDescData = file_contrato_proto_rawDesc
)

func file_contrato_proto_rawDescGZIP() []byte {
	file_contrato_proto_rawDescOnce.Do(func() {
		file_contrato_proto_rawDescData = protoimpl.X.CompressGZIP(file_contrato_proto_rawDescData)
	})
	return file_contrato_proto_rawDescData
}

var file_contrato_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_contrato_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_contrato_proto_goTypes = []interface{}{
	(Status)(0),                   // 0: chat.Status
	(*Cabecalho)(nil),             // 1: chat.Cabecalho
	(*LoginRequest)(nil),          // 2: chat.LoginRequest
	(*LoginResponse)(nil),         // 3: chat.LoginResponse
	(*CreateChannelRequest)(nil),  // 4: chat.CreateChannelRequest
	(*CreateChannelResponse)(nil), // 5: chat.CreateChannelResponse
	(*ListChannelsRequest)(nil),   // 6: chat.ListChannelsRequest
	(*ListChannelsResponse)(nil),  // 7: chat.ListChannelsResponse
	(*Envelope)(nil),              // 8: chat.Envelope
	(*timestamppb.Timestamp)(nil), // 9: google.protobuf.Timestamp
}
var file_contrato_proto_depIdxs = []int32{
	9,  // 0: chat.Cabecalho.timestamp_envio:type_name -> google.protobuf.Timestamp
	1,  // 1: chat.LoginRequest.cabecalho:type_name -> chat.Cabecalho
	1,  // 2: chat.LoginResponse.cabecalho:type_name -> chat.Cabecalho
	0,  // 3: chat.LoginResponse.status:type_name -> chat.Status
	1,  // 4: chat.CreateChannelRequest.cabecalho:type_name -> chat.Cabecalho
	1,  // 5: chat.CreateChannelResponse.cabecalho:type_name -> chat.Cabecalho
	0,  // 6: chat.CreateChannelResponse.status:type_name -> chat.Status
	1,  // 7: chat.ListChannelsRequest.cabecalho:type_name -> chat.Cabecalho
	1,  // 8: chat.ListChannelsResponse.cabecalho:type_name -> chat.Cabecalho
	1,  // 9: chat.Envelope.cabecalho:type_name -> chat.Cabecalho
	2,  // 10: chat.Envelope.login_req:type_name -> chat.LoginRequest
	3,  // 11: chat.Envelope.login_res:type_name -> chat.LoginResponse
	4,  // 12: chat.Envelope.create_channel_req:type_name -> chat.CreateChannelRequest
	5,  // 13: chat.Envelope.create_channel_res:type_name -> chat.CreateChannelResponse
	6,  // 14: chat.Envelope.list_channels_req:type_name -> chat.ListChannelsRequest
	7,  // 15: chat.Envelope.list_channels_res:type_name -> chat.ListChannelsResponse
	16, // [16:16] is the sub-list for method output_type
	16, // [16:16] is the sub-list for method input_type
	16, // [16:16] is the sub-list for extension type_name
	16, // [16:16] is the sub-list for extension extendee
	0,  // [0:16] is the sub-list for field type_name
}

func init() { file_contrato_proto_init() }
func file_contrato_proto_init() {
	if File_contrato_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_contrato_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Cabecalho); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_contrato_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LoginRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_contrato_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LoginResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_contrato_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreateChannelRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_contrato_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreateChannelResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_contrato_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListChannelsRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_contrato_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListChannelsResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_contrato_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Envelope); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_contrato_proto_msgTypes[7].OneofWrappers = []interface{}{
		(*Envelope_LoginReq)(nil),
		(*Envelope_LoginRes)(nil),
		(*Envelope_CreateChannelReq)(nil),
		(*Envelope_CreateChannelRes)(nil),
		(*Envelope_ListChannelsReq)(nil),
		(*Envelope_ListChannelsRes)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_contrato_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_contrato_proto_goTypes,
		DependencyIndexes: file_contrato_proto_depIdxs,
		EnumInfos:         file_contrato_proto_enumTypes,
		MessageInfos:      file_contrato_proto_msgTypes,
	}.Build()
	File_contrato_proto = out.File
	file_contrato_proto_rawDesc = nil
	file_contrato_proto_goTypes = nil
	file_contrato_proto_depIdxs = nil
}
