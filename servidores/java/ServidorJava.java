import chat.Contrato;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.zeromq.ZMQ;
import shared.mensageria.Mensageria;
import shared.mensageria.RelogioProcesso;

public class ServidorJava {
  private final ZMQ.Socket socket;
  private final ZMQ.Socket pubSocket;
  private final ZMQ.Socket refSocket;
  private final Path dataDir;
  private final Path loginsPath;
  private final Path canaisPath;
  private final Path publicacoesPath;
  private final String origem;
  private final String serverName;
  private final RelogioProcesso relogio = new RelogioProcesso();

  private int requestsProcessadas = 0;

  private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

  public ServidorJava(
      String orqEndpoint,
      String proxyPubEndpoint,
      String referenceEndpoint,
      String dataDirStr,
      String serverName
  ) throws Exception {
    ZMQ.Context context = ZMQ.context(1);
    this.socket = context.socket(ZMQ.DEALER);
    this.socket.connect(orqEndpoint);
    this.pubSocket = context.socket(ZMQ.PUB);
    this.pubSocket.connect(proxyPubEndpoint);
    this.refSocket = context.socket(ZMQ.REQ);
    this.refSocket.connect(referenceEndpoint);
    this.origem = Mensageria.origemLabel("servidor");
    this.serverName = serverName;

    this.dataDir = Path.of(dataDirStr);
    this.loginsPath = this.dataDir.resolve("logins.jsonl");
    this.canaisPath = this.dataDir.resolve("canais.json");
    this.publicacoesPath = this.dataDir.resolve("publicacoes.jsonl");

    System.out.println("[SERVIDOR] Conectado ao orquestrador em " + orqEndpoint);
    System.out.println("[SERVIDOR] Conectado ao proxy pub em " + proxyPubEndpoint);
    System.out.println("[SERVIDOR] Conectado à referência em " + referenceEndpoint);
    initStorage();
    registrarNaReferencia();
    listarServidoresReferencia();
  }

  private void initStorage() throws IOException {
    Files.createDirectories(dataDir);

    if (!Files.exists(loginsPath)) {
      Files.write(loginsPath, new byte[0], StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }
    if (!Files.exists(canaisPath)) {
      Files.writeString(canaisPath, "[]", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }
    if (!Files.exists(publicacoesPath)) {
      Files.write(publicacoesPath, new byte[0], StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }
  }

  private Contrato.Envelope enviarParaReferencia(Contrato.Envelope env, boolean atualizarOffset) throws Exception {
    System.out.println(
        "[SERVIDOR] Enviando " + env.getConteudoCase() + " para referência " + Mensageria.cabecalhoTexto(env.getCabecalho())
    );
    long envioNs = Mensageria.agoraNs();
    refSocket.send(Mensageria.envelopeBytes(env), 0);
    byte[] reply = refSocket.recv(0);
    if (reply == null) {
      throw new IllegalStateException("Resposta nula da referência.");
    }
    long recebimentoNs = Mensageria.agoraNs();
    Contrato.Envelope resp = Mensageria.envelopeFromBytes(reply);
    relogio.onReceive(resp.getCabecalho().getRelogioLogico());
    if (atualizarOffset) {
      relogio.atualizarOffset(resp.getCabecalho().getTimestampEnvio(), envioNs, recebimentoNs);
      System.out.println("[SERVIDOR] Offset físico atualizado para " + relogio.offsetMillis() + " ms");
    }
    System.out.println(
        "[SERVIDOR] Recebido " + resp.getConteudoCase() + " da referência " + Mensageria.cabecalhoTexto(resp.getCabecalho())
    );
    return resp;
  }

  private void registrarNaReferencia() throws Exception {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);
    Contrato.RegisterServerRequest req = Contrato.RegisterServerRequest.newBuilder()
        .setCabecalho(cab)
        .setNomeServidor(serverName)
        .build();
    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setRegisterServerReq(req)
        .build();

    Contrato.Envelope resp = enviarParaReferencia(env, true);
    if (resp.getConteudoCase() != Contrato.Envelope.ConteudoCase.REGISTER_SERVER_RES) {
      throw new IllegalStateException("Resposta inesperada ao registrar na referência: " + resp.getConteudoCase());
    }
    Contrato.RegisterServerResponse res = resp.getRegisterServerRes();
    if (res.getStatus() != Contrato.Status.STATUS_SUCESSO) {
      throw new IllegalStateException("Falha ao registrar na referência: " + res.getErroMsg());
    }
    System.out.println("[SERVIDOR] Servidor " + serverName + " registrado na referência com rank=" + res.getRank());
  }

  private List<Contrato.ServerInfo> listarServidoresReferencia() throws Exception {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);
    Contrato.ListServersRequest req = Contrato.ListServersRequest.newBuilder()
        .setCabecalho(cab)
        .build();
    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setListServersReq(req)
        .build();
    Contrato.Envelope resp = enviarParaReferencia(env, true);
    if (resp.getConteudoCase() != Contrato.Envelope.ConteudoCase.LIST_SERVERS_RES) {
      System.err.println("[SERVIDOR] Resposta inesperada ao listar servidores: " + resp.getConteudoCase());
      return new ArrayList<>();
    }
    List<Contrato.ServerInfo> servidores = new ArrayList<>(resp.getListServersRes().getServidoresList());
    if (servidores.isEmpty()) {
      System.out.println("[SERVIDOR] Servidores disponíveis na referência: (nenhum)");
    } else {
      List<String> partes = new ArrayList<>();
      for (Contrato.ServerInfo servidor : servidores) {
        partes.add(servidor.getNome() + "(rank=" + servidor.getRank() + ")");
      }
      System.out.println("[SERVIDOR] Servidores disponíveis na referência: " + String.join(", ", partes));
    }
    return servidores;
  }

  private void heartbeatESincronizar() throws Exception {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);
    Contrato.HeartbeatRequest req = Contrato.HeartbeatRequest.newBuilder()
        .setCabecalho(cab)
        .setNomeServidor(serverName)
        .build();
    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setHeartbeatReq(req)
        .build();
    Contrato.Envelope resp = enviarParaReferencia(env, true);
    if (resp.getConteudoCase() != Contrato.Envelope.ConteudoCase.HEARTBEAT_RES) {
      System.err.println("[SERVIDOR] Resposta inesperada ao heartbeat: " + resp.getConteudoCase());
      return;
    }
    Contrato.HeartbeatResponse res = resp.getHeartbeatRes();
    if (res.getStatus() != Contrato.Status.STATUS_SUCESSO) {
      System.err.println("[SERVIDOR] Heartbeat rejeitado: " + res.getErroMsg());
      return;
    }
    System.out.println("[SERVIDOR] Heartbeat enviado com sucesso para " + serverName);
    listarServidoresReferencia();
  }

  private List<String> lerCanais() {
    try {
      String content = Files.readString(canaisPath, StandardCharsets.UTF_8);
      if (content == null || content.isBlank()) {
        return new ArrayList<>();
      }
      Type listType = new TypeToken<List<String>>() {}.getType();
      List<String> canais = gson.fromJson(content, listType);
      return canais == null ? new ArrayList<>() : canais;
    } catch (Exception e) {
      return new ArrayList<>();
    }
  }

  private void salvarCanais(List<String> canais) {
    try {
      String json = gson.toJson(canais);
      Files.writeString(
          canaisPath,
          json,
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE
      );
    } catch (Exception e) {
    }
  }

  private void registrarLogin(String nomeUsuario, String timestampIso) {
    Map<String, String> obj = new HashMap<>();
    obj.put("usuario", nomeUsuario);
    obj.put("timestamp", timestampIso);
    String line = gson.toJson(obj);
    try {
      Files.writeString(
          loginsPath,
          line + "\n",
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND
      );
    } catch (Exception e) {
    }
  }

  private void registrarPublicacao(String canal, String mensagem, String remetente, String timestampIso) {
    Map<String, String> obj = new HashMap<>();
    obj.put("canal", canal);
    obj.put("mensagem", mensagem);
    obj.put("remetente", remetente);
    obj.put("timestamp", timestampIso);
    String line = gson.toJson(obj);
    try {
      Files.writeString(
          publicacoesPath,
          line + "\n",
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND
      );
    } catch (Exception e) {
    }
  }

  private Contrato.LoginResponse processarLogin(Contrato.LoginRequest req) {
    Contrato.LoginResponse.Builder res = Contrato.LoginResponse.newBuilder();

    String nome = req.getNomeUsuario().trim();
    if (nome.isEmpty()) {
      res.setStatus(Contrato.Status.STATUS_ERRO);
      res.setErroMsg("nome de usuário vazio");
      return res.build();
    }

    registrarLogin(nome, Mensageria.timestampTexto(req.getCabecalho().getTimestampEnvio()));
    res.setStatus(Contrato.Status.STATUS_SUCESSO);
    return res.build();
  }

  private Contrato.CreateChannelResponse processarCreateChannel(Contrato.CreateChannelRequest req) {
    Contrato.CreateChannelResponse.Builder res = Contrato.CreateChannelResponse.newBuilder();

    String nome = req.getNomeCanal().trim();
    if (nome.isEmpty()) {
      res.setStatus(Contrato.Status.STATUS_ERRO);
      res.setErroMsg("nome de canal vazio");
      return res.build();
    }

    List<String> canais = lerCanais();
    for (String c : canais) {
      if (c.equals(nome)) {
        res.setStatus(Contrato.Status.STATUS_ERRO);
        res.setErroMsg("canal já existe");
        return res.build();
      }
    }

    canais.add(nome);
    salvarCanais(canais);

    res.setStatus(Contrato.Status.STATUS_SUCESSO);
    return res.build();
  }

  private Contrato.ListChannelsResponse processarListChannels() {
    return Contrato.ListChannelsResponse.newBuilder()
        .addAllCanais(lerCanais())
        .build();
  }

  private Contrato.PublishResponse processarPublish(
      Contrato.PublishRequest req,
      Contrato.Cabecalho envelopeCabecalho
  ) {
    Contrato.PublishResponse.Builder res = Contrato.PublishResponse.newBuilder();

    String canal = req.getCanal().trim();
    String mensagem = req.getMensagem().trim();
    if (canal.isEmpty()) {
      res.setStatus(Contrato.Status.STATUS_ERRO);
      res.setErroMsg("canal vazio");
      return res.build();
    }
    if (mensagem.isEmpty()) {
      res.setStatus(Contrato.Status.STATUS_ERRO);
      res.setErroMsg("mensagem vazia");
      return res.build();
    }

    List<String> canais = lerCanais();
    if (!canais.contains(canal)) {
      res.setStatus(Contrato.Status.STATUS_ERRO);
      res.setErroMsg("canal inexistente");
      return res.build();
    }

    String remetente = envelopeCabecalho.getLinguagemOrigem().isBlank()
        ? "desconhecido"
        : envelopeCabecalho.getLinguagemOrigem();

    Contrato.Cabecalho cabPub = Mensageria.novoCabecalho(origem, relogio);
    Contrato.ChannelMessage channelMessage = Contrato.ChannelMessage.newBuilder()
        .setCanal(canal)
        .setMensagem(mensagem)
        .setRemetente(remetente)
        .setTimestampEnvio(cabPub.getTimestampEnvio())
        .setRelogioLogico(cabPub.getRelogioLogico())
        .build();

    System.out.println(
        "[SERVIDOR] Publicando em " + canal
            + " ts=" + Mensageria.timestampTexto(channelMessage.getTimestampEnvio())
            + " relogio=" + channelMessage.getRelogioLogico()
    );
    pubSocket.sendMore(canal.getBytes(StandardCharsets.UTF_8));
    pubSocket.send(channelMessage.toByteArray(), 0);

    registrarPublicacao(canal, mensagem, remetente, Mensageria.timestampTexto(channelMessage.getTimestampEnvio()));

    res.setStatus(Contrato.Status.STATUS_SUCESSO);
    return res.build();
  }

  public void loop() throws Exception {
    System.out.println("[SERVIDOR] Servidor iniciado. Aguardando mensagens...");
    while (true) {
      byte[] data = socket.recv(0);
      if (data == null) {
        continue;
      }

      Contrato.Envelope env;
      try {
        env = Contrato.Envelope.parseFrom(data);
      } catch (Exception e) {
        System.err.println("[SERVIDOR] Envelope invalido: " + e.getMessage());
        continue;
      }

      relogio.onReceive(env.getCabecalho().getRelogioLogico());
      Contrato.Envelope.ConteudoCase tipo = env.getConteudoCase();
      System.out.println("[SERVIDOR] Recebido " + tipo + " " + Mensageria.cabecalhoTexto(env.getCabecalho()));

      Object resposta = switch (tipo) {
        case LOGIN_REQ -> processarLogin(env.getLoginReq());
        case CREATE_CHANNEL_REQ -> processarCreateChannel(env.getCreateChannelReq());
        case LIST_CHANNELS_REQ -> processarListChannels();
        case PUBLISH_REQ -> processarPublish(env.getPublishReq(), env.getCabecalho());
        default -> null;
      };

      if (resposta == null) {
        System.err.println("[SERVIDOR] Tipo de mensagem nao suportado: " + tipo);
        continue;
      }

      Contrato.Cabecalho outerCab = Mensageria.novoCabecalho(origem, relogio);
      Contrato.Envelope.Builder respEnv = Contrato.Envelope.newBuilder().setCabecalho(outerCab);

      if (resposta instanceof Contrato.LoginResponse lr) {
        respEnv.setLoginRes(lr.toBuilder().setCabecalho(outerCab).build());
      } else if (resposta instanceof Contrato.CreateChannelResponse cr) {
        respEnv.setCreateChannelRes(cr.toBuilder().setCabecalho(outerCab).build());
      } else if (resposta instanceof Contrato.ListChannelsResponse lcr) {
        respEnv.setListChannelsRes(lcr.toBuilder().setCabecalho(outerCab).build());
      } else if (resposta instanceof Contrato.PublishResponse pr) {
        respEnv.setPublishRes(pr.toBuilder().setCabecalho(outerCab).build());
      }

      System.out.println("[SERVIDOR] Enviando " + respEnv.getConteudoCase() + " " + Mensageria.cabecalhoTexto(outerCab));
      socket.send(Mensageria.envelopeBytes(respEnv.build()), 0);

      requestsProcessadas += 1;
      if (requestsProcessadas % 10 == 0) {
        heartbeatESincronizar();
      }
    }
  }

  private static String getEnv(String name, String defaultValue) {
    String v = System.getenv(name);
    return (v == null || v.isEmpty()) ? defaultValue : v;
  }

  public static void main(String[] args) throws Exception {
    String orqEndpoint = getEnv("ORQ_ENDPOINT_SERVIDOR", "tcp://orquestrador:5556");
    String proxyPubEndpoint = getEnv("PROXY_PUB_ENDPOINT", "tcp://proxy:5557");
    String referenceEndpoint = getEnv("REFERENCE_ENDPOINT", "tcp://referencia:5559");
    String dataDir = getEnv("DATA_DIR", "/data");
    String serverName = getEnv("SERVER_NAME", "servidor_java");

    ServidorJava servidor = new ServidorJava(orqEndpoint, proxyPubEndpoint, referenceEndpoint, dataDir, serverName);
    servidor.loop();
  }
}
