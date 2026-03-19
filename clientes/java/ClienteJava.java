import chat.Contrato;
import java.util.ArrayList;
import java.util.List;
import org.zeromq.ZMQ;
import shared.mensageria.Mensageria;

public class ClienteJava {
  private final ZMQ.Socket socket;

  private final String orqEndpoint;
  private final String nomeUsuario;
  private final String nomeCanal;

  public ClienteJava(String orqEndpoint, String nomeUsuario, String nomeCanal) {
    this.orqEndpoint = orqEndpoint;
    this.nomeUsuario = nomeUsuario;
    this.nomeCanal = nomeCanal;

    ZMQ.Context context = ZMQ.context(1);
    this.socket = context.socket(ZMQ.DEALER);
    this.socket.connect(this.orqEndpoint);
    System.out.println("[CLIENTE] Conectado ao orquestrador em " + this.orqEndpoint);
  }

  private Contrato.Envelope enviarEAguardar(Contrato.Envelope env) throws Exception {
    socket.send(Mensageria.envelopeBytes(env), 0);
    byte[] reply = socket.recv(0);
    if (reply == null) {
      throw new IllegalStateException("Resposta nula ao aguardar mensagem do servidor.");
    }
    return Mensageria.envelopeFromBytes(reply);
  }

  private boolean fazerLogin(String nomeUsuario) throws Exception {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(Mensageria.origemLabel("cliente"));

    Contrato.LoginRequest loginReq = Contrato.LoginRequest.newBuilder()
        .setCabecalho(cab)
        .setNomeUsuario(nomeUsuario)
        .build();

    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setLoginReq(loginReq)
        .build();

    System.out.println("[CLIENTE] Enviando LoginRequest para usuario '" + nomeUsuario + "'");
    Contrato.Envelope respEnv = enviarEAguardar(env);

    if (respEnv.getConteudoCase() != Contrato.Envelope.ConteudoCase.LOGIN_RES) {
      System.err.println("[CLIENTE] Resposta inesperada ao login: " + respEnv.getConteudoCase());
      return false;
    }

    Contrato.LoginResponse res = respEnv.getLoginRes();
    if (res.getStatus() == Contrato.Status.STATUS_SUCESSO) {
      System.out.println("[CLIENTE] Login bem-sucedido para '" + nomeUsuario + "'");
      return true;
    }

    System.out.println("[CLIENTE] Falha no login: " + res.getErroMsg());
    return false;
  }

  private void criarCanal(String nomeCanal) throws Exception {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(Mensageria.origemLabel("cliente"));

    Contrato.CreateChannelRequest req = Contrato.CreateChannelRequest.newBuilder()
        .setCabecalho(cab)
        .setNomeCanal(nomeCanal)
        .build();

    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setCreateChannelReq(req)
        .build();

    System.out.println("[CLIENTE] Solicitando criacao do canal '" + nomeCanal + "'");
    Contrato.Envelope respEnv = enviarEAguardar(env);

    if (respEnv.getConteudoCase() != Contrato.Envelope.ConteudoCase.CREATE_CHANNEL_RES) {
      System.err.println(
          "[CLIENTE] Resposta inesperada a criacao de canal: " + respEnv.getConteudoCase()
      );
      return;
    }

    Contrato.CreateChannelResponse res = respEnv.getCreateChannelRes();
    if (res.getStatus() == Contrato.Status.STATUS_SUCESSO) {
      System.out.println("[CLIENTE] Canal '" + nomeCanal + "' criado com sucesso");
    } else {
      System.out.println("[CLIENTE] Falha ao criar canal '" + nomeCanal + "': " + res.getErroMsg());
    }
  }

  private List<String> listarCanais() throws Exception {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(Mensageria.origemLabel("cliente"));

    Contrato.ListChannelsRequest req = Contrato.ListChannelsRequest.newBuilder()
        .setCabecalho(cab)
        .build();

    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setListChannelsReq(req)
        .build();

    System.out.println("[CLIENTE] Solicitando listagem de canais");
    Contrato.Envelope respEnv = enviarEAguardar(env);

    if (respEnv.getConteudoCase() != Contrato.Envelope.ConteudoCase.LIST_CHANNELS_RES) {
      System.err.println(
          "[CLIENTE] Resposta inesperada a listagem de canais: " + respEnv.getConteudoCase()
      );
      return new ArrayList<>();
    }

    Contrato.ListChannelsResponse res = respEnv.getListChannelsRes();
    List<String> canais = new ArrayList<>(res.getCanaisList());
    if (canais.isEmpty()) {
      System.out.println("[CLIENTE] Canais existentes: (nenhum)");
    } else {
      System.out.println("[CLIENTE] Canais existentes: " + String.join(", ", canais));
    }
    return canais;
  }

  public void executar() throws Exception {
    for (int i = 0; i < 3; i++) {
      if (fazerLogin(nomeUsuario)) {
        break;
      }
      Thread.sleep(1000L);
    }

    criarCanal(nomeCanal);

    for (int i = 0; i < 100; i++) {
      listarCanais();
      Thread.sleep(500L);
    }
  }

  private static String getEnv(String name, String defaultValue) {
    String v = System.getenv(name);
    return (v == null || v.isEmpty()) ? defaultValue : v;
  }

  public static void main(String[] args) throws Exception {
    String orqEndpoint = getEnv("ORQ_ENDPOINT", "tcp://orquestrador:5555");
    String nomeUsuario = getEnv("CLIENTE_NOME", "cliente_java");
    String nomeCanal = getEnv("CLIENTE_CANAL", "canal_java");

    ClienteJava cliente = new ClienteJava(orqEndpoint, nomeUsuario, nomeCanal);
    cliente.executar();
  }
}

