using Chat;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Shared;

public static class Mensageria
{
    private static int _idCounter = 0;

    public static long AgoraNs()
    {
        return (DateTime.UtcNow.Ticks - DateTime.UnixEpoch.Ticks) * 100L;
    }

    public static Timestamp TimestampFromNs(long ns)
    {
        return new Timestamp
        {
            Seconds = ns / 1_000_000_000L,
            Nanos = (int)(ns % 1_000_000_000L)
        };
    }

    public static string TimestampTexto(Timestamp? ts)
    {
        if (ts is null)
        {
            return "0.000000000";
        }

        return $"{ts.Seconds}.{ts.Nanos:D9}";
    }

    public static string CabecalhoTexto(Cabecalho? cab)
    {
        if (cab is null)
        {
            return "tx=0 origem=desconhecida ts=0.000000000 relogio=0";
        }

        return $"tx={cab.IdTransacao} origem={cab.LinguagemOrigem} ts={TimestampTexto(cab.TimestampEnvio)} relogio={cab.RelogioLogico}";
    }

    public static Cabecalho NovoCabecalho(string origem, RelogioProcesso? relogio = null)
    {
        var timestamp = relogio?.NowCorrigidoTimestamp() ?? TimestampFromNs(AgoraNs());
        var relogioLogico = relogio?.BeforeSend() ?? 0L;

        return new Cabecalho
        {
            IdTransacao = Interlocked.Increment(ref _idCounter),
            LinguagemOrigem = origem,
            TimestampEnvio = timestamp,
            RelogioLogico = relogioLogico
        };
    }

    public static byte[] EnvelopeBytes(Envelope env) => env.ToByteArray();

    public static Envelope EnvelopeFromBytes(byte[] data) => Envelope.Parser.ParseFrom(data);

    public static string OrigemLabel(string role) => $"csharp-{role}";
}
