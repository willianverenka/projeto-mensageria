import chat.Contrato;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.zeromq.ZMQ;
import shared.mensageria.Mensageria;
import shared.mensageria.RelogioProcesso;

public class ClienteJava {
  private final ZMQ.Socket socket;
  private final ZMQ.Socket subSocket;

  private final String orqEndpoint;
  private final String proxySubEndpoint;
  private final String nomeUsuario;
  private final String nomeCanal;
  private final String origem;
  private final Set<String> subscribedChannels = new HashSet<>();
  private final Random random = new Random();
  private final RelogioProcesso relogio = new RelogioProcesso();

  public ClienteJava(
      String orqEndpoint,
      String proxySubEndpoint,
      String nomeUsuario,
      String nomeCanal
  ) {
    this.orqEndpoint = orqEndpoint;
    this.proxySubEndpoint = proxySubEndpoint;
    this.nomeUsuario = nomeUsuario;
    this.nomeCanal = nomeCanal;
    this.origem = Mensageria.origemLabel("cliente");

    ZMQ.Context context = ZMQ.context(1);
    this.socket = context.socket(ZMQ.DEALER);
    this.socket.connect(this.orqEndpoint);
    this.subSocket = context.socket(ZMQ.SUB);
    this.subSocket.connect(this.proxySubEndpoint);
    System.out.println("[CLIENTE] Conectado ao orquestrador em " + this.orqEndpoint);
    System.out.println("[CLIENTE] Conectado ao proxy sub em " + this.proxySubEndpoint);
  }

  private Contrato.Envelope enviarEAguardar(Contrato.Envelope env) throws Exception {
    System.out.println(
        "[CLIENTE] Enviando " + env.getConteudoCase() + " " + Mensageria.cabecalhoTexto(env.getCabecalho())
    );
    socket.send(Mensageria.envelopeBytes(env), 0);
    byte[] reply = socket.recv(0);
    if (reply == null) {
      throw new IllegalStateException("Resposta nula ao aguardar mensagem do servidor.");
    }
    Contrato.Envelope resp = Mensageria.envelopeFromBytes(reply);
    relogio.onReceive(resp.getCabecalho().getRelogioLogico());
    System.out.println(
        "[CLIENTE] Recebido " + resp.getConteudoCase() + " " + Mensageria.cabecalhoTexto(resp.getCabecalho())
    );
    return resp;
  }

  private boolean fazerLogin(String nomeUsuario) throws Exception {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);

    Contrato.LoginRequest loginReq = Contrato.LoginRequest.newBuilder()
        .setCabecalho(cab)
        .setNomeUsuario(nomeUsuario)
        .build();

    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setLoginReq(loginReq)
        .build();

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
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);

    Contrato.CreateChannelRequest req = Contrato.CreateChannelRequest.newBuilder()
        .setCabecalho(cab)
        .setNomeCanal(nomeCanal)
        .build();

    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setCreateChannelReq(req)
        .build();

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
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);

    Contrato.ListChannelsRequest req = Contrato.ListChannelsRequest.newBuilder()
        .setCabecalho(cab)
        .build();

    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setListChannelsReq(req)
        .build();

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

  private boolean publicar(String canal, String mensagem) throws Exception {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);
    Contrato.PublishRequest req = Contrato.PublishRequest.newBuilder()
        .setCabecalho(cab)
        .setCanal(canal)
        .setMensagem(mensagem)
        .build();
    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setPublishReq(req)
        .build();

    Contrato.Envelope respEnv = enviarEAguardar(env);
    if (respEnv.getConteudoCase() != Contrato.Envelope.ConteudoCase.PUBLISH_RES) {
      return false;
    }
    return respEnv.getPublishRes().getStatus() == Contrato.Status.STATUS_SUCESSO;
  }

  private void inscrever(String canal) {
    if (subscribedChannels.contains(canal)) {
      return;
    }
    subSocket.subscribe(canal.getBytes(ZMQ.CHARSET));
    subscribedChannels.add(canal);
    System.out.println("[CLIENTE] Inscrito no canal '" + canal + "'");
  }

  private void iniciarReceptor() {
    Thread t = new Thread(() -> {
      while (true) {
        byte[] topicBytes = subSocket.recv(0);
        byte[] payload = subSocket.recv(0);
        if (topicBytes == null || payload == null) {
          continue;
        }
        try {
          String topic = new String(topicBytes, ZMQ.CHARSET);
          Contrato.ChannelMessage msg = Contrato.ChannelMessage.parseFrom(payload);
          relogio.onReceive(msg.getRelogioLogico());
          System.out.println(
              "[CLIENTE] [CANAL=" + topic + "] msg='" + msg.getMensagem() + "' remetente="
                  + msg.getRemetente()
                  + " envio=" + Mensageria.timestampTexto(msg.getTimestampEnvio())
                  + " recebimento=" + Mensageria.timestampTexto(Mensageria.timestampFromNs(Mensageria.agoraNs()))
                  + " relogio=" + msg.getRelogioLogico()
          );
        } catch (Exception e) {
          System.err.println("[CLIENTE] Erro ao receber pub/sub: " + e.getMessage());
        }
      }
    });
    t.setDaemon(true);
    t.start();
  }

  public void executar() throws Exception {
    for (int i = 0; i < 3; i++) {
      if (fazerLogin(nomeUsuario)) {
        break;
      }
      Thread.sleep(1000L);
    }

    iniciarReceptor();
    List<String> canais = listarCanais();
    if (canais.size() < 5) {
      criarCanal(nomeCanal + "_" + random.nextInt(10_000));
      canais = listarCanais();
    }

    while (subscribedChannels.size() < 3 && subscribedChannels.size() < canais.size()) {
      String canal = canais.get(random.nextInt(canais.size()));
      inscrever(canal);
    }

    while (true) {
      canais = listarCanais();
      if (canais.isEmpty()) {
        Thread.sleep(1000L);
        continue;
      }
      String canal = canais.get(random.nextInt(canais.size()));
      for (int i = 0; i < 10; i++) {
        publicar(canal, "msg_" + random.nextInt(1_000_000));
        Thread.sleep(1000L);
      }
    }
  }

  private static String getEnv(String name, String defaultValue) {
    String v = System.getenv(name);
    return (v == null || v.isEmpty()) ? defaultValue : v;
  }

  public static void main(String[] args) throws Exception {
    String orqEndpoint = getEnv("ORQ_ENDPOINT", "tcp://orquestrador:5555");
    String proxySubEndpoint = getEnv("PROXY_SUB_ENDPOINT", "tcp://proxy:5558");
    String nomeUsuario = getEnv("CLIENTE_NOME", "cliente_java");
    String nomeCanal = getEnv("CLIENTE_CANAL", "canal_java");

    ClienteJava cliente = new ClienteJava(orqEndpoint, proxySubEndpoint, nomeUsuario, nomeCanal);
    cliente.executar();
  }
}
