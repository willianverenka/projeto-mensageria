using Google.Protobuf.WellKnownTypes;

namespace Shared;

public class RelogioProcesso
{
    private readonly object _sync = new();
    private long _logical = 0;
    private long _offsetNs = 0;

    public long BeforeSend()
    {
        lock (_sync)
        {
            _logical += 1;
            return _logical;
        }
    }

    public long OnReceive(long recebido)
    {
        lock (_sync)
        {
            _logical = Math.Max(_logical, recebido);
            return _logical;
        }
    }

    public long Valor()
    {
        lock (_sync)
        {
            return _logical;
        }
    }

    public Timestamp NowCorrigidoTimestamp()
    {
        lock (_sync)
        {
            return Mensageria.TimestampFromNs(Mensageria.AgoraNs() + _offsetNs);
        }
    }

    public long AtualizarOffset(Timestamp referencia, long envioNs, long recebimentoNs)
    {
        var pontoMedio = (envioNs + recebimentoNs) / 2L;
        var refNs = referencia.Seconds * 1_000_000_000L + referencia.Nanos;
        lock (_sync)
        {
            _offsetNs = refNs - pontoMedio;
            return _offsetNs;
        }
    }

    public double OffsetMillis()
    {
        lock (_sync)
        {
            return _offsetNs / 1_000_000.0;
        }
    }
}
