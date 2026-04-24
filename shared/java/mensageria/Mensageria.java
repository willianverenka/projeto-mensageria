package shared.mensageria;

import chat.Contrato;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class Mensageria {
  private static final AtomicInteger ID_COUNTER = new AtomicInteger(0);

  public static long agoraNs() {
    Instant now = Instant.now();
    return now.getEpochSecond() * 1_000_000_000L + now.getNano();
  }

  public static Timestamp timestampFromNs(long ns) {
    long seconds = ns / 1_000_000_000L;
    int nanos = (int) (ns % 1_000_000_000L);
    return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
  }

  public static String timestampTexto(Timestamp ts) {
    if (ts == null) {
      return "0.000000000";
    }
    return ts.getSeconds() + "." + String.format("%09d", ts.getNanos());
  }

  public static String cabecalhoTexto(Contrato.Cabecalho cab) {
    if (cab == null) {
      return "tx=0 origem=desconhecida ts=0.000000000 relogio=0";
    }
    return "tx=" + cab.getIdTransacao()
        + " origem=" + cab.getLinguagemOrigem()
        + " ts=" + timestampTexto(cab.getTimestampEnvio())
        + " relogio=" + cab.getRelogioLogico();
  }

  public static Contrato.Cabecalho novoCabecalho(String origem, RelogioProcesso relogio) {
    Timestamp timestamp = timestampFromNs(agoraNs());
    long relogioLogico = 0L;
    if (relogio != null) {
      timestamp = relogio.nowCorrigidoTimestamp();
      relogioLogico = relogio.beforeSend();
    }
    return Contrato.Cabecalho.newBuilder()
        .setIdTransacao(ID_COUNTER.incrementAndGet())
        .setLinguagemOrigem(origem)
        .setTimestampEnvio(timestamp)
        .setRelogioLogico(relogioLogico)
        .build();
  }

  public static String origemLabel(String role) {
    return "java-" + role;
  }

  public static byte[] envelopeBytes(Contrato.Envelope env) {
    return env.toByteArray();
  }

  public static Contrato.Envelope envelopeFromBytes(byte[] data) throws Exception {
    return Contrato.Envelope.parseFrom(data);
  }
}
