from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import message as _message

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x13\x63ontratos/contrato.proto\x12\x04\x63hat\x1a\x1fgoogle/protobuf/timestamp.proto\"(\n\x06Status\x12\x12\n\x0eSTATUS_SUCESSO\x10\x00\x12\x0f\n\x0bSTATUS_ERRO\x10\x01\"Z\n\tCabecalho\x12\x14\n\x0cid_transacao\x18\x01 \x01(\x05\x12\x19\n\x11linguagem_origem\x18\x02 \x01(\t\x12+\n\x0ftimestamp_envio\x18\x03 \x01(\x0b\x32\x12.google.protobuf.Timestamp\"?\n\x0cLoginRequest\x12\x1f\n\tcabecalho\x18\x01 \x01(\x0b\x32\x0c.chat.Cabecalho\x12\x14\n\x0cnome_usuario\x18\x02 \x01(\t\"`\n\rLoginResponse\x12\x1f\n\tcabecalho\x18\x01 \x01(\x0b\x32\x0c.chat.Cabecalho\x12\x1e\n\x06status\x18\x02 \x01(\x0e\x32\x0e.chat.Status\x12\x10\n\x08\x65rro_msg\x18\x03 \x01(\t\"P\n\x14\x43reateChannelRequest\x12\x1f\n\tcabecalho\x18\x01 \x01(\x0b\x32\x0c.chat.Cabecalho\x12\x12\n\nnome_canal\x18\x02 \x01(\t\"i\n\x15\x43reateChannelResponse\x12\x1f\n\tcabecalho\x18\x01 \x01(\x0b\x32\x0c.chat.Cabecalho\x12\x1e\n\x06status\x18\x02 \x01(\x0e\x32\x0e.chat.Status\x12\x10\n\x08\x65rro_msg\x18\x03 \x01(\t\"I\n\x13ListChannelsRequest\x12\x1f\n\tcabecalho\x18\x01 \x01(\x0b\x32\x0c.chat.Cabecalho\"U\n\x14ListChannelsResponse\x12\x1f\n\tcabecalho\x18\x01 \x01(\x0b\x32\x0c.chat.Cabecalho\x12\x0e\n\x06\x63anais\x18\x02 \x03(\t\"\xf8\x03\n\x08\x45nvelope\x12\x1f\n\tcabecalho\x18\x01 \x01(\x0b\x32\x0c.chat.Cabecalho\x12/\n\tlogin_req\x18\x02 \x01(\x0b\x32\x12.chat.LoginRequestH\x00\x12\x31\n\tlogin_res\x18\x03 \x01(\x0b\x32\x14.chat.LoginResponseH\x00\x12>\n\x12\x63reate_channel_req\x18\x04 \x01(\x0b\x32 .chat.CreateChannelRequestH\x00\x12@\n\x12\x63reate_channel_res\x18\x05 \x01(\x0b\x32\".chat.CreateChannelResponseH\x00\x12>\n\x11list_channels_req\x18\x06 \x01(\x0b\x32!.chat.ListChannelsRequestH\x00\x12@\n\x11list_channels_res\x18\x07 \x01(\x0b\x32#.chat.ListChannelsResponseH\x00\x42\t\n\x08\x63onteudo*b\n\x06Status\x12\x12\n\x0eSTATUS_SUCESSO\x10\x00\x12\x0f\n\x0bSTATUS_ERRO\x10\x01\x42\x32Z0github.com/seu-usuario/seu-repo/contratos;contratosb\x06proto3'
)


class Cabecalho(_message.Message):
    __slots__ = ()

    DESCRIPTOR = DESCRIPTOR.message_types_by_name["Cabecalho"]


class LoginRequest(_message.Message):
    __slots__ = ()

    DESCRIPTOR = DESCRIPTOR.message_types_by_name["LoginRequest"]


class LoginResponse(_message.Message):
    __slots__ = ()

    DESCRIPTOR = DESCRIPTOR.message_types_by_name["LoginResponse"]


class CreateChannelRequest(_message.Message):
    __slots__ = ()

    DESCRIPTOR = DESCRIPTOR.message_types_by_name["CreateChannelRequest"]


class CreateChannelResponse(_message.Message):
    __slots__ = ()

    DESCRIPTOR = DESCRIPTOR.message_types_by_name["CreateChannelResponse"]


class ListChannelsRequest(_message.Message):
    __slots__ = ()

    DESCRIPTOR = DESCRIPTOR.message_types_by_name["ListChannelsRequest"]


class ListChannelsResponse(_message.Message):
    __slots__ = ()

    DESCRIPTOR = DESCRIPTOR.message_types_by_name["ListChannelsResponse"]


class Envelope(_message.Message):
    __slots__ = ()

    DESCRIPTOR = DESCRIPTOR.message_types_by_name["Envelope"]


_sym_db.RegisterMessage(Cabecalho)
_sym_db.RegisterMessage(LoginRequest)
_sym_db.RegisterMessage(LoginResponse)
_sym_db.RegisterMessage(CreateChannelRequest)
_sym_db.RegisterMessage(CreateChannelResponse)
_sym_db.RegisterMessage(ListChannelsRequest)
_sym_db.RegisterMessage(ListChannelsResponse)
_sym_db.RegisterMessage(Envelope)

