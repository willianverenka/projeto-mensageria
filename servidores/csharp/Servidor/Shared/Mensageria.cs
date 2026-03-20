using Chat;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Shared;

public static class Mensageria
{
    private static int _idCounter = 0;

    public static Cabecalho NovoCabecalho(string origem)
    {
        return new Cabecalho
        {
            IdTransacao = Interlocked.Increment(ref _idCounter),
            LinguagemOrigem = origem,
            TimestampEnvio = Timestamp.FromDateTime(DateTime.UtcNow)
        };
    }

    public static byte[] EnvelopeBytes(Envelope env) => env.ToByteArray();

    public static Envelope EnvelopeFromBytes(byte[] data) => Envelope.Parser.ParseFrom(data);

    public static string OrigemLabel(string role) => $"csharp-{role}";
}