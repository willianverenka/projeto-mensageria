package shared.mensageria;

import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public class RelogioProcesso {
  private final AtomicLong logical = new AtomicLong(0L);
  private final AtomicLong offsetNs = new AtomicLong(0L);

  public long beforeSend() {
    return logical.incrementAndGet();
  }

  public long onReceive(long recebido) {
    while (true) {
      long atual = logical.get();
      if (recebido <= atual) {
        return atual;
      }
      if (logical.compareAndSet(atual, recebido)) {
        return recebido;
      }
    }
  }

  public long valor() {
    return logical.get();
  }

  public Timestamp nowCorrigidoTimestamp() {
    long ns = Mensageria.agoraNs() + offsetNs.get();
    return Mensageria.timestampFromNs(ns);
  }

  public long atualizarOffset(Timestamp referencia, long envioNs, long recebimentoNs) {
    if (referencia == null) {
      return offsetNs.get();
    }
    long pontoMedio = (envioNs + recebimentoNs) / 2L;
    long refNs = referencia.getSeconds() * 1_000_000_000L + referencia.getNanos();
    long offset = refNs - pontoMedio;
    offsetNs.set(offset);
    return offset;
  }

  public double offsetMillis() {
    return offsetNs.get() / 1_000_000.0;
  }
}
