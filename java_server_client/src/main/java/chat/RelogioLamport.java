package chat;

import java.util.concurrent.atomic.AtomicLong;

public final class RelogioLamport {
    private final AtomicLong valor = new AtomicLong();

    public long antesDeEnviar() {
        return valor.incrementAndGet();
    }

    public long aoReceber(long recebido) {
        return valor.updateAndGet(atual -> Math.max(atual, recebido) + 1);
    }
}
