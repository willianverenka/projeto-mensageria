package shared.mensageria;

import chat.Contrato;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class Mensageria {
  private static final AtomicInteger ID_COUNTER = new AtomicInteger(0);

  public static Contrato.Cabecalho novoCabecalho(String origem) {
    Instant now = Instant.now();
    return Contrato.Cabecalho.newBuilder()
        .setIdTransacao(ID_COUNTER.incrementAndGet())
        .setLinguagemOrigem(origem)
        .setTimestampEnvio(
            Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build()
        )
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

