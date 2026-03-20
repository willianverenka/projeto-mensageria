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

public class ServidorJava {
  private final ZMQ.Socket socket;
  private final Path dataDir;
  private final Path loginsPath;
  private final Path canaisPath;

  private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

  public ServidorJava(String orqEndpoint, String dataDirStr) throws IOException {
    ZMQ.Context context = ZMQ.context(1);
    this.socket = context.socket(ZMQ.DEALER);
    this.socket.connect(orqEndpoint);

    this.dataDir = Path.of(dataDirStr);
    this.loginsPath = this.dataDir.resolve("logins.jsonl");
    this.canaisPath = this.dataDir.resolve("canais.json");

    System.out.println("[SERVIDOR] Conectado ao orquestrador em " + orqEndpoint);
    initStorage();
    registrarNoOrquestrador();
  }

  private void initStorage() throws IOException {
    Files.createDirectories(dataDir);

    if (!Files.exists(loginsPath)) {
      Files.write(loginsPath, new byte[0], StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }
    if (!Files.exists(canaisPath)) {
      Files.writeString(canaisPath, "[]", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }
  }

  private void registrarNoOrquestrador() {
    try {
      Contrato.Cabecalho cab = Mensageria.novoCabecalho(Mensageria.origemLabel("servidor"));
      Contrato.ListChannelsRequest req =
          Contrato.ListChannelsRequest.newBuilder().setCabecalho(cab).build();

      Contrato.Envelope env = Contrato.Envelope.newBuilder()
          .setCabecalho(cab)
          .setListChannelsReq(req)
          .build();

      byte[] data = Mensageria.envelopeBytes(env);
      socket.send(data, ZMQ.DONTWAIT);
      System.out.println("[SERVIDOR] Registro inicial enviado ao orquestrador.");
    } catch (Exception e) {
      System.out.println(
          "[SERVIDOR] Aviso: nao foi possivel enviar mensagem de registro ao orquestrador."
      );
    }
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

  private Contrato.LoginResponse processarLogin(Contrato.LoginRequest req) {
    Contrato.LoginResponse.Builder res = Contrato.LoginResponse.newBuilder()
        .setCabecalho(req.getCabecalho());

    String nome = req.getNomeUsuario().trim();
    if (nome.isEmpty()) {
      res.setStatus(Contrato.Status.STATUS_ERRO);
      res.setErroMsg("nome de usu\\u00e1rio vazio");
      return res.build();
    }

    com.google.protobuf.Timestamp ts = req.getCabecalho().getTimestampEnvio();
    String tsIso = "";
    if (ts.getSeconds() != 0 || ts.getNanos() != 0) {
      tsIso = ts.getSeconds() + "." + String.format("%09d", ts.getNanos());
    }
    registrarLogin(nome, tsIso);

    res.setStatus(Contrato.Status.STATUS_SUCESSO);
    return res.build();
  }

  private Contrato.CreateChannelResponse processarCreateChannel(Contrato.CreateChannelRequest req) {
    Contrato.CreateChannelResponse.Builder res = Contrato.CreateChannelResponse.newBuilder()
        .setCabecalho(req.getCabecalho());

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
        res.setErroMsg("canal j\\u00e1 existe");
        return res.build();
      }
    }

    canais.add(nome);
    salvarCanais(canais);

    res.setStatus(Contrato.Status.STATUS_SUCESSO);
    return res.build();
  }

  private Contrato.ListChannelsResponse processarListChannels(Contrato.ListChannelsRequest req) {
    return Contrato.ListChannelsResponse.newBuilder()
        .setCabecalho(req.getCabecalho())
        .addAllCanais(lerCanais())
        .build();
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

      Contrato.Envelope.ConteudoCase tipo = env.getConteudoCase();
      Contrato.Cabecalho outerCab = Mensageria.novoCabecalho(Mensageria.origemLabel("servidor"));
      Contrato.Envelope.Builder respEnv = Contrato.Envelope.newBuilder().setCabecalho(outerCab);

      switch (tipo) {
        case LOGIN_REQ:
          System.out.println("[SERVIDOR] Servidor java processando mensagem: login_req");
          respEnv.setLoginRes(processarLogin(env.getLoginReq()));
          break;
        case CREATE_CHANNEL_REQ:
          System.out.println("[SERVIDOR] Servidor java processando mensagem: create_channel_req");
          respEnv.setCreateChannelRes(processarCreateChannel(env.getCreateChannelReq()));
          break;
        case LIST_CHANNELS_REQ:
          System.out.println("[SERVIDOR] Servidor java processando mensagem: list_channels_req");
          respEnv.setListChannelsRes(processarListChannels(env.getListChannelsReq()));
          break;
        default:
          System.err.println("[SERVIDOR] Tipo de mensagem nao suportado: " + tipo);
          continue;
      }

      socket.send(Mensageria.envelopeBytes(respEnv.build()), 0);
    }
  }

  private static String getEnv(String name, String defaultValue) {
    String v = System.getenv(name);
    return (v == null || v.isEmpty()) ? defaultValue : v;
  }

  public static void main(String[] args) throws Exception {
    String orqEndpoint = getEnv("ORQ_ENDPOINT_SERVIDOR", "tcp://orquestrador:5556");
    String dataDir = getEnv("DATA_DIR", "/data");

    ServidorJava servidor = new ServidorJava(orqEndpoint, dataDir);
    servidor.loop();
  }
}

