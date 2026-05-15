import chat.Contrato;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.zeromq.ZMQ;
import shared.mensageria.Mensageria;
import shared.mensageria.RelogioProcesso;

public class ServidorJava {
  private static final String TOPICO_COORDENADOR = "servers";
  private static final String TOPICO_REPLICA = "__replica__";
  private static final int CLOCK_PORT = 5560;
  private static final int ELECTION_PORT = 5561;
  private static final int SNAPSHOT_PORT = 5562;
  private static final int REQUEST_TIMEOUT_MS = 2000;
  private static final int SNAPSHOT_TIMEOUT_MS = 10000;
  private static final String LOG_MODE = getEnv("SERVER_LOG_MODE", "presentation").trim().toLowerCase();

  private final ZMQ.Context context;
  private final ZMQ.Socket socket;
  private final ZMQ.Socket pubSocket;
  private final ZMQ.Socket announceSocket;
  private final ZMQ.Socket refSocket;
  private final ZMQ.Socket subSocket;
  private final Path dataDir;
  private final Path loginsPath;
  private final Path canaisPath;
  private final Path publicacoesPath;
  private final String origem;
  private final String serverName;
  private final RelogioProcesso relogio = new RelogioProcesso();
  private final Object stateLock = new Object();
  private final Object referenceLock = new Object();
  private final Object storageLock = new Object();
  private final Map<String, Integer> servidoresAtivos = new HashMap<>();

  private int requestsProcessadas = 0;
  private String coordenadorAtual = "";
  private int meuRank = 0;
  private boolean electionRunning = false;

  private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

  private static boolean isVerbose() {
    return "verbose".equals(LOG_MODE);
  }

  private static void logVerbose(String message) {
    if (isVerbose()) {
      System.out.println(message);
    }
  }

  private static void logEleicao(String message) {
    System.out.println("[ELEICAO] " + message);
  }

  public ServidorJava(
      String orqEndpoint,
      String proxyPubEndpoint,
      String proxySubEndpoint,
      String referenceEndpoint,
      String dataDirStr,
      String serverName
  ) throws Exception {
    this.context = ZMQ.context(1);
    this.socket = this.context.socket(ZMQ.DEALER);
    this.socket.connect(orqEndpoint);
    this.pubSocket = this.context.socket(ZMQ.PUB);
    this.pubSocket.connect(proxyPubEndpoint);
    this.announceSocket = this.context.socket(ZMQ.PUB);
    this.announceSocket.connect(proxyPubEndpoint);
    this.refSocket = this.context.socket(ZMQ.REQ);
    this.refSocket.connect(referenceEndpoint);
    this.subSocket = this.context.socket(ZMQ.SUB);
    this.subSocket.connect(proxySubEndpoint);
    this.subSocket.subscribe(TOPICO_COORDENADOR.getBytes(ZMQ.CHARSET));
    this.subSocket.subscribe(TOPICO_REPLICA.getBytes(ZMQ.CHARSET));
    this.origem = Mensageria.origemLabel("servidor");
    this.serverName = serverName;

    this.dataDir = Path.of(dataDirStr);
    this.loginsPath = this.dataDir.resolve("logins.jsonl");
    this.canaisPath = this.dataDir.resolve("canais.json");
    this.publicacoesPath = this.dataDir.resolve("publicacoes.jsonl");

    logVerbose("[SERVIDOR] Conectado ao orquestrador em " + orqEndpoint);
    logVerbose("[SERVIDOR] Conectado ao proxy pub em " + proxyPubEndpoint);
    logVerbose("[SERVIDOR] Conectado ao proxy sub em " + proxySubEndpoint);
    logVerbose("[SERVIDOR] Conectado à referência em " + referenceEndpoint);
    initStorage();
    registrarNaReferencia();
    List<Contrato.ServerInfo> servidores = listarServidoresReferencia();
    atualizarServidoresAtivos(servidores);
    iniciarServicosInternos();
    sincronizarReplicas();
    iniciarEleicaoAsync("inicializacao");
    System.out.println("[SERVIDOR] Servidor pronto. Rank local=" + meuRank + " coordenador=" + coordenadorAtual());
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

  private Contrato.Envelope enviarParaReferencia(Contrato.Envelope env) throws Exception {
    logVerbose(
        "[SERVIDOR] Enviando " + env.getConteudoCase() + " para referência " + Mensageria.cabecalhoTexto(env.getCabecalho())
    );
    byte[] reply;
    synchronized (referenceLock) {
      refSocket.send(Mensageria.envelopeBytes(env), 0);
      reply = refSocket.recv(0);
    }
    if (reply == null) {
      throw new IllegalStateException("Resposta nula da referência.");
    }
    Contrato.Envelope resp = Mensageria.envelopeFromBytes(reply);
    relogio.onReceive(resp.getCabecalho().getRelogioLogico());
    logVerbose(
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

    Contrato.Envelope resp = enviarParaReferencia(env);
    if (resp.getConteudoCase() != Contrato.Envelope.ConteudoCase.REGISTER_SERVER_RES) {
      throw new IllegalStateException("Resposta inesperada ao registrar na referência: " + resp.getConteudoCase());
    }
    Contrato.RegisterServerResponse res = resp.getRegisterServerRes();
    if (res.getStatus() != Contrato.Status.STATUS_SUCESSO) {
      throw new IllegalStateException("Falha ao registrar na referência: " + res.getErroMsg());
    }
    meuRank = res.getRank();
    atualizarServidoresAtivos(List.of(
        Contrato.ServerInfo.newBuilder()
            .setNome(serverName)
            .setRank(meuRank)
            .build()
    ));
    logVerbose("[SERVIDOR] Servidor " + serverName + " registrado na referência com rank=" + res.getRank());
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
    Contrato.Envelope resp = enviarParaReferencia(env);
    if (resp.getConteudoCase() != Contrato.Envelope.ConteudoCase.LIST_SERVERS_RES) {
      System.err.println("[SERVIDOR] Resposta inesperada ao listar servidores: " + resp.getConteudoCase());
      return new ArrayList<>();
    }
    List<Contrato.ServerInfo> servidores = new ArrayList<>(resp.getListServersRes().getServidoresList());
    atualizarServidoresAtivos(servidores);
    if (servidores.isEmpty()) {
      logVerbose("[SERVIDOR] Servidores disponíveis na referência: (nenhum)");
    } else {
      List<String> partes = new ArrayList<>();
      for (Contrato.ServerInfo servidor : servidores) {
        partes.add(servidor.getNome() + "(rank=" + servidor.getRank() + ")");
      }
      logVerbose("[SERVIDOR] Servidores disponíveis na referência: " + String.join(", ", partes));
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
    Contrato.Envelope resp = enviarParaReferencia(env);
    if (resp.getConteudoCase() != Contrato.Envelope.ConteudoCase.HEARTBEAT_RES) {
      System.err.println("[SERVIDOR] Resposta inesperada ao heartbeat: " + resp.getConteudoCase());
      return;
    }
    Contrato.HeartbeatResponse res = resp.getHeartbeatRes();
    if (res.getStatus() != Contrato.Status.STATUS_SUCESSO) {
      System.err.println("[SERVIDOR] Heartbeat rejeitado: " + res.getErroMsg());
      return;
    }
    logVerbose("[SERVIDOR] Heartbeat enviado com sucesso para " + serverName);
    listarServidoresReferencia();
    avaliarEleicaoAposAtualizacao("heartbeat");
    sincronizarReplicas();
  }

  private List<String> lerCanais() {
    synchronized (storageLock) {
      try {
        String content = Files.readString(canaisPath, StandardCharsets.UTF_8);
        if (content == null || content.isBlank()) {
          return new ArrayList<>();
        }
        Type listType = new TypeToken<List<String>>() {}.getType();
        List<String> canais = gson.fromJson(content, listType);
        return normalizarCanais(canais == null ? new ArrayList<>() : canais);
      } catch (Exception e) {
        return new ArrayList<>();
      }
    }
  }

  private void salvarCanais(List<String> canais) {
    synchronized (storageLock) {
      try {
        String json = gson.toJson(normalizarCanais(canais));
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
  }

  private List<String> normalizarCanais(List<String> canais) {
    TreeSet<String> normalizados = new TreeSet<>();
    for (String canal : canais) {
      if (canal != null && !canal.trim().isEmpty()) {
        normalizados.add(canal.trim());
      }
    }
    return new ArrayList<>(normalizados);
  }

  private boolean adicionarCanalIdempotente(String nome) {
    synchronized (storageLock) {
      List<String> canais = lerCanais();
      if (canais.contains(nome)) {
        return false;
      }
      canais.add(nome);
      salvarCanais(canais);
      return true;
    }
  }

  private List<Map<String, String>> lerJsonl(Path path) {
    List<Map<String, String>> registros = new ArrayList<>();
    try {
      for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
        if (line == null || line.isBlank()) {
          continue;
        }
        Type mapType = new TypeToken<Map<String, String>>() {}.getType();
        Map<String, String> item = gson.fromJson(line, mapType);
        if (item != null) {
          registros.add(new HashMap<>(item));
        }
      }
    } catch (Exception e) {
    }
    return registros;
  }

  private void salvarJsonl(Path path, List<Map<String, String>> registros) {
    StringBuilder builder = new StringBuilder();
    for (Map<String, String> registro : registros) {
      builder.append(gson.toJson(registro)).append("\n");
    }
    try {
      Files.writeString(
          path,
          builder.toString(),
          StandardCharsets.UTF_8,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE
      );
    } catch (Exception e) {
    }
  }

  private void registrarLogin(String nomeUsuario, String timestampIso) {
    synchronized (storageLock) {
      List<Map<String, String>> registros = lerJsonl(loginsPath);
      for (Map<String, String> item : registros) {
        if (nomeUsuario.equals(item.get("usuario")) && timestampIso.equals(item.get("timestamp"))) {
          return;
        }
      }
      Map<String, String> obj = new LinkedHashMap<>();
      obj.put("usuario", nomeUsuario);
      obj.put("timestamp", timestampIso);
      registros.add(obj);
      registros.sort(Comparator
          .comparing((Map<String, String> item) -> item.getOrDefault("timestamp", ""))
          .thenComparing(item -> item.getOrDefault("usuario", "")));
      salvarJsonl(loginsPath, registros);
    }
  }

  private void registrarPublicacao(String canal, String mensagem, String remetente, String timestampIso) {
    synchronized (storageLock) {
      List<Map<String, String>> registros = lerJsonl(publicacoesPath);
      for (Map<String, String> item : registros) {
        if (canal.equals(item.get("canal"))
            && mensagem.equals(item.get("mensagem"))
            && remetente.equals(item.get("remetente"))
            && timestampIso.equals(item.get("timestamp"))) {
          return;
        }
      }
      Map<String, String> obj = new LinkedHashMap<>();
      obj.put("canal", canal);
      obj.put("mensagem", mensagem);
      obj.put("remetente", remetente);
      obj.put("timestamp", timestampIso);
      registros.add(obj);
      registros.sort(Comparator
          .comparing((Map<String, String> item) -> item.getOrDefault("timestamp", ""))
          .thenComparing(item -> item.getOrDefault("canal", ""))
          .thenComparing(item -> item.getOrDefault("remetente", ""))
          .thenComparing(item -> item.getOrDefault("mensagem", "")));
      salvarJsonl(publicacoesPath, registros);
    }
  }

  private String replicaOrigem() {
    return "replica:" + serverName;
  }

  private com.google.protobuf.Timestamp timestampTextoParaTimestamp(String texto) {
    try {
      String[] partes = texto.split("\\.", 2);
      long segundos = Long.parseLong(partes[0]);
      String nanosTexto = partes.length > 1 ? partes[1] : "0";
      if (nanosTexto.length() > 9) {
        nanosTexto = nanosTexto.substring(0, 9);
      }
      while (nanosTexto.length() < 9) {
        nanosTexto += "0";
      }
      int nanos = Integer.parseInt(nanosTexto);
      return com.google.protobuf.Timestamp.newBuilder()
          .setSeconds(segundos)
          .setNanos(nanos)
          .build();
    } catch (Exception e) {
      return com.google.protobuf.Timestamp.newBuilder().setSeconds(0).setNanos(0).build();
    }
  }

  private Contrato.Envelope.Builder novoEnvelopeReplicacao() {
    return Contrato.Envelope.newBuilder()
        .setCabecalho(Mensageria.novoCabecalho(replicaOrigem(), relogio));
  }

  private void publicarReplicacao(Contrato.Envelope env) {
    try {
      pubSocket.sendMore(TOPICO_REPLICA.getBytes(StandardCharsets.UTF_8));
      pubSocket.send(env.toByteArray(), 0);
    } catch (Exception e) {
      System.out.println("[SERVIDOR] Falha ao publicar réplica: " + e.getMessage());
    }
  }

  private void replicarLogin(Contrato.LoginRequest req) {
    publicarReplicacao(novoEnvelopeReplicacao().setLoginReq(req).build());
  }

  private void replicarCreateChannel(Contrato.CreateChannelRequest req) {
    publicarReplicacao(novoEnvelopeReplicacao().setCreateChannelReq(req).build());
  }

  private void replicarPublicacao(
      String canal,
      String mensagem,
      String remetente,
      Contrato.Cabecalho cabPublicacao
  ) {
    Contrato.Cabecalho cab = cabPublicacao.toBuilder()
        .setLinguagemOrigem(remetente)
        .build();
    Contrato.PublishRequest req = Contrato.PublishRequest.newBuilder()
        .setCabecalho(cab)
        .setCanal(canal)
        .setMensagem(mensagem)
        .build();
    publicarReplicacao(novoEnvelopeReplicacao().setPublishReq(req).build());
  }

  private List<Contrato.Envelope> operacoesSnapshot() {
    synchronized (storageLock) {
      List<Contrato.Envelope> operacoes = new ArrayList<>();
      for (Map<String, String> item : lerJsonl(loginsPath)) {
        String nome = item.getOrDefault("usuario", "").trim();
        if (nome.isEmpty()) {
          continue;
        }
        Contrato.Cabecalho cab = Contrato.Cabecalho.newBuilder()
            .setLinguagemOrigem("snapshot")
            .setTimestampEnvio(timestampTextoParaTimestamp(item.getOrDefault("timestamp", "0.000000000")))
            .build();
        Contrato.LoginRequest req = Contrato.LoginRequest.newBuilder()
            .setCabecalho(cab)
            .setNomeUsuario(nome)
            .build();
        operacoes.add(novoEnvelopeReplicacao().setLoginReq(req).build());
      }

      for (String canal : lerCanais()) {
        Contrato.Cabecalho cab = Contrato.Cabecalho.newBuilder()
            .setLinguagemOrigem("snapshot")
            .setTimestampEnvio(timestampTextoParaTimestamp("0.000000000"))
            .build();
        Contrato.CreateChannelRequest req = Contrato.CreateChannelRequest.newBuilder()
            .setCabecalho(cab)
            .setNomeCanal(canal)
            .build();
        operacoes.add(novoEnvelopeReplicacao().setCreateChannelReq(req).build());
      }

      for (Map<String, String> item : lerJsonl(publicacoesPath)) {
        String canal = item.getOrDefault("canal", "").trim();
        String mensagem = item.getOrDefault("mensagem", "").trim();
        if (canal.isEmpty() || mensagem.isEmpty()) {
          continue;
        }
        String remetente = item.getOrDefault("remetente", "").trim();
        if (remetente.isEmpty()) {
          remetente = "desconhecido";
        }
        Contrato.Cabecalho cab = Contrato.Cabecalho.newBuilder()
            .setLinguagemOrigem(remetente)
            .setTimestampEnvio(timestampTextoParaTimestamp(item.getOrDefault("timestamp", "0.000000000")))
            .build();
        Contrato.PublishRequest req = Contrato.PublishRequest.newBuilder()
            .setCabecalho(cab)
            .setCanal(canal)
            .setMensagem(mensagem)
            .build();
        operacoes.add(novoEnvelopeReplicacao().setPublishReq(req).build());
      }
      return operacoes;
    }
  }

  private void aplicarOperacaoReplicada(Contrato.Envelope env) {
    switch (env.getConteudoCase()) {
      case LOGIN_REQ -> {
        String nome = env.getLoginReq().getNomeUsuario().trim();
        if (!nome.isEmpty()) {
          registrarLogin(nome, Mensageria.timestampTexto(env.getLoginReq().getCabecalho().getTimestampEnvio()));
        }
      }
      case CREATE_CHANNEL_REQ -> {
        String nome = env.getCreateChannelReq().getNomeCanal().trim();
        if (!nome.isEmpty()) {
          adicionarCanalIdempotente(nome);
        }
      }
      case PUBLISH_REQ -> {
        Contrato.PublishRequest req = env.getPublishReq();
        String canal = req.getCanal().trim();
        String mensagem = req.getMensagem().trim();
        if (canal.isEmpty() || mensagem.isEmpty()) {
          return;
        }
        adicionarCanalIdempotente(canal);
        String remetente = req.getCabecalho().getLinguagemOrigem().trim();
        if (remetente.isEmpty()) {
          remetente = "desconhecido";
        }
        registrarPublicacao(
            canal,
            mensagem,
            remetente,
            Mensageria.timestampTexto(req.getCabecalho().getTimestampEnvio())
        );
      }
      default -> {
      }
    }
  }

  private String chaveLogin(Map<String, String> item) {
    return item.getOrDefault("usuario", "") + "\u0000" + item.getOrDefault("timestamp", "");
  }

  private String chavePublicacao(Map<String, String> item) {
    return item.getOrDefault("canal", "")
        + "\u0000" + item.getOrDefault("mensagem", "")
        + "\u0000" + item.getOrDefault("remetente", "")
        + "\u0000" + item.getOrDefault("timestamp", "");
  }

  private void aplicarOperacoesReplicadas(List<Contrato.Envelope> operacoes) {
    synchronized (storageLock) {
      TreeSet<String> canais = new TreeSet<>(lerCanais());

      Map<String, Map<String, String>> loginsPorChave = new HashMap<>();
      for (Map<String, String> item : lerJsonl(loginsPath)) {
        loginsPorChave.put(chaveLogin(item), item);
      }

      Map<String, Map<String, String>> publicacoesPorChave = new HashMap<>();
      for (Map<String, String> item : lerJsonl(publicacoesPath)) {
        publicacoesPorChave.put(chavePublicacao(item), item);
      }

      for (Contrato.Envelope env : operacoes) {
        switch (env.getConteudoCase()) {
          case LOGIN_REQ -> {
            String nome = env.getLoginReq().getNomeUsuario().trim();
            if (!nome.isEmpty()) {
              Map<String, String> item = new LinkedHashMap<>();
              item.put("usuario", nome);
              item.put("timestamp", Mensageria.timestampTexto(env.getLoginReq().getCabecalho().getTimestampEnvio()));
              loginsPorChave.put(chaveLogin(item), item);
            }
          }
          case CREATE_CHANNEL_REQ -> {
            String nome = env.getCreateChannelReq().getNomeCanal().trim();
            if (!nome.isEmpty()) {
              canais.add(nome);
            }
          }
          case PUBLISH_REQ -> {
            Contrato.PublishRequest req = env.getPublishReq();
            String canal = req.getCanal().trim();
            String mensagem = req.getMensagem().trim();
            if (canal.isEmpty() || mensagem.isEmpty()) {
              continue;
            }
            canais.add(canal);
            String remetente = req.getCabecalho().getLinguagemOrigem().trim();
            if (remetente.isEmpty()) {
              remetente = "desconhecido";
            }
            Map<String, String> item = new LinkedHashMap<>();
            item.put("canal", canal);
            item.put("mensagem", mensagem);
            item.put("remetente", remetente);
            item.put("timestamp", Mensageria.timestampTexto(req.getCabecalho().getTimestampEnvio()));
            publicacoesPorChave.put(chavePublicacao(item), item);
          }
          default -> {
          }
        }
      }

      salvarCanais(new ArrayList<>(canais));

      List<Map<String, String>> logins = new ArrayList<>(loginsPorChave.values());
      logins.sort(Comparator
          .comparing((Map<String, String> item) -> item.getOrDefault("timestamp", ""))
          .thenComparing(item -> item.getOrDefault("usuario", "")));
      salvarJsonl(loginsPath, logins);

      List<Map<String, String>> publicacoes = new ArrayList<>(publicacoesPorChave.values());
      publicacoes.sort(Comparator
          .comparing((Map<String, String> item) -> item.getOrDefault("timestamp", ""))
          .thenComparing(item -> item.getOrDefault("canal", ""))
          .thenComparing(item -> item.getOrDefault("remetente", ""))
          .thenComparing(item -> item.getOrDefault("mensagem", "")));
      salvarJsonl(publicacoesPath, publicacoes);
    }
  }

  private Contrato.Envelope respostaSnapshotStatus(Contrato.Status status, String erroMsg) {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);
    Contrato.HeartbeatResponse resposta = Contrato.HeartbeatResponse.newBuilder()
        .setCabecalho(cab)
        .setStatus(status)
        .setErroMsg(erroMsg)
        .build();
    return Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setHeartbeatRes(resposta)
        .build();
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
    replicarLogin(req);
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

    if (!adicionarCanalIdempotente(nome)) {
      res.setStatus(Contrato.Status.STATUS_ERRO);
      res.setErroMsg("canal já existe");
      return res.build();
    }

    replicarCreateChannel(req);
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

    logVerbose(
        "[SERVIDOR] Publicando em " + canal
            + " ts=" + Mensageria.timestampTexto(channelMessage.getTimestampEnvio())
            + " relogio=" + channelMessage.getRelogioLogico()
    );
    pubSocket.sendMore(canal.getBytes(StandardCharsets.UTF_8));
    pubSocket.send(channelMessage.toByteArray(), 0);

    registrarPublicacao(canal, mensagem, remetente, Mensageria.timestampTexto(channelMessage.getTimestampEnvio()));
    replicarPublicacao(canal, mensagem, remetente, cabPub);

    res.setStatus(Contrato.Status.STATUS_SUCESSO);
    return res.build();
  }

  private void iniciarServicosInternos() {
    Thread clockThread = new Thread(this::loopRelogio, "servidor-java-clock");
    clockThread.setDaemon(true);
    clockThread.start();

    Thread electionThread = new Thread(this::loopEleicao, "servidor-java-election");
    electionThread.setDaemon(true);
    electionThread.start();

    Thread snapshotThread = new Thread(this::loopSnapshot, "servidor-java-snapshot");
    snapshotThread.setDaemon(true);
    snapshotThread.start();

    Thread announcementThread = new Thread(this::loopAnunciosCoordenador, "servidor-java-announcements");
    announcementThread.setDaemon(true);
    announcementThread.start();
  }

  private void loopSnapshot() {
    try (ZMQ.Socket sock = context.socket(ZMQ.REP)) {
      sock.bind("tcp://*:" + SNAPSHOT_PORT);
      logVerbose("[SERVIDOR] Serviço de snapshot ouvindo em tcp://*:" + SNAPSHOT_PORT);
      while (true) {
        byte[] data = sock.recv(0);
        if (data == null) {
          continue;
        }

        Contrato.Envelope env;
        try {
          env = Contrato.Envelope.parseFrom(data);
        } catch (Exception e) {
          System.out.println("[SERVIDOR] Snapshot request inválido: " + e.getMessage());
          continue;
        }
        relogio.onReceive(env.getCabecalho().getRelogioLogico());

        Contrato.Envelope status = respostaSnapshotStatus(Contrato.Status.STATUS_SUCESSO, "");
        if (env.getConteudoCase() != Contrato.Envelope.ConteudoCase.HEARTBEAT_REQ) {
          status = respostaSnapshotStatus(Contrato.Status.STATUS_ERRO, "tipo nao suportado para snapshot");
        }

        List<byte[]> frames = new ArrayList<>();
        frames.add(status.toByteArray());
        if (status.getHeartbeatRes().getStatus() == Contrato.Status.STATUS_SUCESSO) {
          for (Contrato.Envelope op : operacoesSnapshot()) {
            frames.add(op.toByteArray());
          }
        }
        for (int i = 0; i < frames.size(); i++) {
          if (i < frames.size() - 1) {
            sock.sendMore(frames.get(i));
          } else {
            sock.send(frames.get(i), 0);
          }
        }
      }
    } catch (Exception e) {
      System.out.println("[SERVIDOR] Erro no serviço de snapshot: " + e.getMessage());
    }
  }

  private void sincronizarReplicas() {
    List<String> candidatos = new ArrayList<>();
    String coordenador = coordenadorAtual();
    if (!coordenador.isBlank() && !coordenador.equals(serverName)) {
      candidatos.add(coordenador);
    }
    for (Contrato.ServerInfo servidor : servidoresAtivosSnapshot()) {
      String nome = servidor.getNome();
      if (!nome.equals(serverName) && !candidatos.contains(nome)) {
        candidatos.add(nome);
      }
    }
    for (String nome : candidatos) {
      solicitarSnapshot(nome);
    }
  }

  private boolean solicitarSnapshot(String nomeServidor) {
    try (ZMQ.Socket sock = context.socket(ZMQ.REQ)) {
      sock.setReceiveTimeOut(SNAPSHOT_TIMEOUT_MS);
      sock.setSendTimeOut(SNAPSHOT_TIMEOUT_MS);
      sock.setLinger(0);
      sock.connect("tcp://" + nomeServidor + ":" + SNAPSHOT_PORT);

      Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);
      Contrato.HeartbeatRequest req = Contrato.HeartbeatRequest.newBuilder()
          .setCabecalho(cab)
          .setNomeServidor(serverName)
          .build();
      Contrato.Envelope env = Contrato.Envelope.newBuilder()
          .setCabecalho(cab)
          .setHeartbeatReq(req)
          .build();
      sock.send(env.toByteArray(), 0);

      List<byte[]> frames = new ArrayList<>();
      byte[] frame = sock.recv(0);
      if (frame == null) {
        return false;
      }
      frames.add(frame);
      while (sock.hasReceiveMore()) {
        byte[] next = sock.recv(0);
        if (next != null) {
          frames.add(next);
        }
      }

      Contrato.Envelope statusEnv = Contrato.Envelope.parseFrom(frames.get(0));
      relogio.onReceive(statusEnv.getCabecalho().getRelogioLogico());
      if (statusEnv.getConteudoCase() != Contrato.Envelope.ConteudoCase.HEARTBEAT_RES
          || statusEnv.getHeartbeatRes().getStatus() != Contrato.Status.STATUS_SUCESSO) {
        return false;
      }

      List<Contrato.Envelope> operacoes = new ArrayList<>();
      for (int i = 1; i < frames.size(); i++) {
        Contrato.Envelope op = Contrato.Envelope.parseFrom(frames.get(i));
        relogio.onReceive(op.getCabecalho().getRelogioLogico());
        operacoes.add(op);
      }
      if (!operacoes.isEmpty()) {
        aplicarOperacoesReplicadas(operacoes);
      }
      int aplicadas = operacoes.size();
      if (aplicadas > 0) {
        logVerbose("[SERVIDOR] Snapshot aplicado de " + nomeServidor + " com " + aplicadas + " operações");
      }
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void loopRelogio() {
    try (ZMQ.Socket sock = context.socket(ZMQ.REP)) {
      sock.bind("tcp://*:" + CLOCK_PORT);
      logVerbose("[SERVIDOR] Serviço de relógio interno ouvindo em tcp://*:" + CLOCK_PORT);
      while (true) {
        byte[] data = sock.recv(0);
        if (data == null) {
          continue;
        }

        Contrato.Envelope env;
        try {
          env = Contrato.Envelope.parseFrom(data);
        } catch (Exception e) {
          System.out.println("[SERVIDOR] Envelope inválido no relógio interno: " + e.getMessage());
          continue;
        }

        relogio.onReceive(env.getCabecalho().getRelogioLogico());
        Contrato.HeartbeatResponse resposta;
        if (env.getConteudoCase() == Contrato.Envelope.ConteudoCase.HEARTBEAT_REQ) {
          resposta = processarPedidoRelogio(env.getHeartbeatReq());
        } else {
          resposta = Contrato.HeartbeatResponse.newBuilder()
              .setStatus(Contrato.Status.STATUS_ERRO)
              .setErroMsg("tipo nao suportado")
              .build();
        }

        Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);
        Contrato.Envelope respostaEnv = Contrato.Envelope.newBuilder()
            .setCabecalho(cab)
            .setHeartbeatRes(resposta.toBuilder().setCabecalho(cab).build())
            .build();
        sock.send(Mensageria.envelopeBytes(respostaEnv), 0);
      }
    } catch (Exception e) {
      System.out.println("[SERVIDOR] Erro no serviço interno de relógio: " + e.getMessage());
    }
  }

  private void loopEleicao() {
    try (ZMQ.Socket sock = context.socket(ZMQ.REP)) {
      sock.bind("tcp://*:" + ELECTION_PORT);
      logVerbose("[SERVIDOR] Serviço de eleição interno ouvindo em tcp://*:" + ELECTION_PORT);
      while (true) {
        byte[] data = sock.recv(0);
        if (data == null) {
          continue;
        }

        Contrato.Envelope env;
        try {
          env = Contrato.Envelope.parseFrom(data);
        } catch (Exception e) {
          System.out.println("[SERVIDOR] Envelope inválido na eleição interna: " + e.getMessage());
          continue;
        }

        relogio.onReceive(env.getCabecalho().getRelogioLogico());
        Contrato.RegisterServerResponse resposta;
        if (env.getConteudoCase() == Contrato.Envelope.ConteudoCase.REGISTER_SERVER_REQ) {
          resposta = processarPedidoEleicao(env.getRegisterServerReq());
        } else {
          resposta = Contrato.RegisterServerResponse.newBuilder()
              .setStatus(Contrato.Status.STATUS_ERRO)
              .setErroMsg("tipo nao suportado")
              .build();
        }

        Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);
        Contrato.Envelope respostaEnv = Contrato.Envelope.newBuilder()
            .setCabecalho(cab)
            .setRegisterServerRes(resposta.toBuilder().setCabecalho(cab).build())
            .build();
        sock.send(Mensageria.envelopeBytes(respostaEnv), 0);
      }
    } catch (Exception e) {
      System.out.println("[SERVIDOR] Erro no serviço interno de eleição: " + e.getMessage());
    }
  }

  private void loopAnunciosCoordenador() {
    logVerbose("[SERVIDOR] Escutando tópicos internos " + TOPICO_COORDENADOR + " e " + TOPICO_REPLICA);
    while (true) {
      try {
        byte[] topicBytes = subSocket.recv(0);
        byte[] payload = subSocket.recv(0);
        if (topicBytes == null || payload == null) {
          continue;
        }

        String topic = new String(topicBytes, ZMQ.CHARSET);
        if (topic.equals(TOPICO_REPLICA)) {
          Contrato.Envelope env = Contrato.Envelope.parseFrom(payload);
          if (env.getCabecalho().getLinguagemOrigem().equals(replicaOrigem())) {
            continue;
          }
          relogio.onReceive(env.getCabecalho().getRelogioLogico());
          aplicarOperacaoReplicada(env);
          logVerbose("[SERVIDOR] Operação replicada recebida: " + env.getConteudoCase());
          continue;
        }
        if (!topic.equals(TOPICO_COORDENADOR)) {
          continue;
        }

        Contrato.ChannelMessage channelMessage = Contrato.ChannelMessage.parseFrom(payload);
        relogio.onReceive(channelMessage.getRelogioLogico());
        String coordenador = channelMessage.getMensagem().trim();
        if (coordenador.isEmpty()) {
          continue;
        }
        String remetente = channelMessage.getRemetente().trim();
        if (remetente.equals(serverName) && coordenador.equals(serverName)) {
          continue;
        }
        if (remetente.isEmpty()) {
          remetente = "desconhecido";
        }
        processarAnuncioCoordenador(coordenador, remetente);
      } catch (Exception e) {
        System.out.println("[SERVIDOR] Erro ao ler anúncios de coordenador: " + e.getMessage());
      }
    }
  }

  private List<Contrato.ServerInfo> servidoresAtivosSnapshot() {
    synchronized (stateLock) {
      List<Contrato.ServerInfo> servidores = new ArrayList<>();
      for (Map.Entry<String, Integer> entry : servidoresAtivos.entrySet()) {
        servidores.add(Contrato.ServerInfo.newBuilder()
            .setNome(entry.getKey())
            .setRank(entry.getValue())
            .build());
      }
      servidores.sort((a, b) -> Integer.compare((int) a.getRank(), (int) b.getRank()));
      return servidores;
    }
  }

  private String resumoServidores(List<Contrato.ServerInfo> servidores) {
    if (servidores.isEmpty()) {
      return "(nenhum)";
    }
    List<String> partes = new ArrayList<>();
    for (Contrato.ServerInfo servidor : servidores) {
      partes.add(servidor.getNome() + "(rank=" + servidor.getRank() + ")");
    }
    return String.join(", ", partes);
  }

  private void atualizarServidoresAtivos(List<Contrato.ServerInfo> servidores) {
    synchronized (stateLock) {
      if (servidores != null && !servidores.isEmpty()) {
        servidoresAtivos.clear();
        for (Contrato.ServerInfo servidor : servidores) {
          servidoresAtivos.put(servidor.getNome(), (int) servidor.getRank());
        }
      } else if (servidoresAtivos.isEmpty() && meuRank > 0) {
        servidoresAtivos.put(serverName, meuRank);
      }

      if (meuRank > 0) {
        servidoresAtivos.put(serverName, meuRank);
      }
    }
    logVerbose("[SERVIDOR] Lista ativa local atualizada: " + resumoServidores(servidoresAtivosSnapshot()));
  }

  private String maiorServidorAtivo() {
    List<Contrato.ServerInfo> servidores = servidoresAtivosSnapshot();
    if (servidores.isEmpty()) {
      return serverName;
    }
    return servidores.get(servidores.size() - 1).getNome();
  }

  private String coordenadorAtual() {
    synchronized (stateLock) {
      return coordenadorAtual;
    }
  }

  private String setCoordenador(String nome) {
    String anterior;
    synchronized (stateLock) {
      anterior = coordenadorAtual;
      coordenadorAtual = nome;
    }
    return anterior;
  }

  private void avaliarEleicaoAposAtualizacao(String motivo) {
    List<Contrato.ServerInfo> servidores = servidoresAtivosSnapshot();
    if (servidores.isEmpty()) {
      return;
    }
    Contrato.ServerInfo maior = servidores.get(servidores.size() - 1);
    String coordenador = coordenadorAtual();
    int rankCoordenador = coordenador.isEmpty() ? -1 : rankServidor(coordenador);
    if (coordenador.isEmpty() || rankCoordenador < 0 || maior.getRank() > rankCoordenador) {
      iniciarEleicaoAsync(motivo);
    }
  }

  private boolean isCoordenador() {
    return serverName.equals(coordenadorAtual());
  }

  private void iniciarEleicaoAsync(String motivo) {
    Thread t = new Thread(() -> executarEleicao(motivo), "servidor-java-election-async");
    t.setDaemon(true);
    t.start();
  }

  private void executarEleicao(String motivo) {
    synchronized (stateLock) {
      if (electionRunning) {
        logVerbose("[SERVIDOR] Eleição já em andamento; ignorando gatilho (" + motivo + ")");
        return;
      }
      electionRunning = true;
    }

    try {
      logEleicao("inicio motivo=" + motivo);
      listarServidoresReferencia();
      List<Contrato.ServerInfo> servidores = servidoresAtivosSnapshot();
      List<Contrato.ServerInfo> superiores = new ArrayList<>();
      for (Contrato.ServerInfo item : servidores) {
        if (item.getRank() > meuRank && !item.getNome().equals(serverName)) {
          superiores.add(item);
        }
      }

      superiores.sort((a, b) -> Integer.compare((int) b.getRank(), (int) a.getRank()));
      if (superiores.isEmpty()) {
        tornarCoordenador("sem_servidor_maior");
        return;
      }

      boolean recebeuOk = false;
      for (Contrato.ServerInfo item : superiores) {
        if (consultarEleicaoServidor(item.getNome(), true)) {
          recebeuOk = true;
        }
      }

      if (!recebeuOk) {
        tornarCoordenador("sem_resposta_de_servidor_maior");
        return;
      }

      logEleicao("delegada motivo=" + motivo);
    } catch (Exception e) {
      System.out.println("[SERVIDOR] Erro durante eleição: " + e.getMessage());
    } finally {
      synchronized (stateLock) {
        electionRunning = false;
      }
    }
  }

  private void tornarCoordenador(String motivo) {
    setCoordenador(serverName);
    logEleicao("eleito coordenador=" + serverName + " rank=" + meuRank + " motivo=" + motivo);
    anunciarCoordenador();
  }

  private void anunciarCoordenador() {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);
    Contrato.ChannelMessage channelMessage = Contrato.ChannelMessage.newBuilder()
        .setCanal(TOPICO_COORDENADOR)
        .setMensagem(serverName)
        .setRemetente(serverName)
        .setTimestampEnvio(cab.getTimestampEnvio())
        .setRelogioLogico(cab.getRelogioLogico())
        .build();

    try {
      announceSocket.sendMore(TOPICO_COORDENADOR.getBytes(ZMQ.CHARSET));
      announceSocket.send(channelMessage.toByteArray(), 0);
      logEleicao("anuncio publicado coordenador=" + serverName + " rank=" + meuRank);
    } catch (Exception e) {
      System.out.println("[SERVIDOR] Falha ao anunciar coordenador: " + e.getMessage());
    }
  }

  private void processarAnuncioCoordenador(String coordenador, String remetente) throws Exception {
    listarServidoresReferencia();
    int rankAnunciado = rankServidor(coordenador);
    if (coordenador.equals(serverName)) {
      rankAnunciado = meuRank;
    }
    if (rankAnunciado < 0) {
      logEleicao("anuncio ignorado coordenador=" + coordenador + " rank=desconhecido motivo=servidor_desconhecido");
      return;
    }

    String atual = coordenadorAtual();
    int rankAtual = atual.isEmpty() ? -1 : rankServidor(atual);
    boolean aceitar = false;
    String motivo = "sem_coordenador";
    if (atual.isEmpty() || atual.equals(coordenador)) {
      aceitar = true;
      if (atual.equals(coordenador)) {
        motivo = "mesmo_coordenador";
      }
    } else if (rankAtual < 0) {
      aceitar = true;
      motivo = "coordenador_atual_desconhecido";
    } else if (rankAnunciado >= rankAtual) {
      aceitar = true;
      motivo = "rank_maior_ou_igual";
    } else if (!atual.equals(serverName) && !consultarEleicaoServidor(atual, false)) {
      aceitar = true;
      motivo = "coordenador_atual_indisponivel";
    }

    if (aceitar) {
      setCoordenador(coordenador);
      logEleicao(
          "anuncio aceito coordenador=" + coordenador
              + " rank=" + rankAnunciado
              + " origem=" + remetente
              + " motivo=" + motivo
      );
      return;
    }

    logEleicao(
        "anuncio ignorado coordenador=" + coordenador
            + " rank=" + rankAnunciado
            + " motivo=rank_menor_que_atual atual=" + atual
    );
  }

  private long timestampToNs(com.google.protobuf.Timestamp timestamp) {
    return timestamp.getSeconds() * 1_000_000_000L + timestamp.getNanos();
  }

  private PeerReply enviarParaServidor(String nomeServidor, int porta, Contrato.Envelope env, int timeoutMs) {
    ZMQ.Socket sock = context.socket(ZMQ.REQ);
    try {
      sock.setReceiveTimeOut(timeoutMs);
      sock.setSendTimeOut(timeoutMs);
      sock.setLinger(0);
      sock.connect("tcp://" + nomeServidor + ":" + porta);

      long envioNs = Mensageria.agoraNs();
      sock.send(Mensageria.envelopeBytes(env), 0);
      byte[] reply = sock.recv(0);
      if (reply == null) {
        return null;
      }
      long recebimentoNs = Mensageria.agoraNs();
      Contrato.Envelope resp = Mensageria.envelopeFromBytes(reply);
      relogio.onReceive(resp.getCabecalho().getRelogioLogico());
      return new PeerReply(resp, envioNs, recebimentoNs);
    } catch (Exception e) {
      logVerbose("[SERVIDOR] Falha ao comunicar com " + nomeServidor + ":" + porta + " - " + e.getMessage());
      return null;
    } finally {
      sock.close();
    }
  }

  private PeerReply consultarRelogioServidor(String nomeServidor) {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);
    Contrato.HeartbeatRequest req = Contrato.HeartbeatRequest.newBuilder()
        .setCabecalho(cab)
        .setNomeServidor(serverName)
        .build();
    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setHeartbeatReq(req)
        .build();
    logVerbose("[SERVIDOR] Enviando heartbeat_req para " + nomeServidor + " " + Mensageria.cabecalhoTexto(cab));
    PeerReply reply = enviarParaServidor(nomeServidor, CLOCK_PORT, env, REQUEST_TIMEOUT_MS);
    if (reply == null) {
      logVerbose("[SERVIDOR] Sem resposta de relógio de " + nomeServidor);
      return null;
    }
    if (reply.envelope.getConteudoCase() != Contrato.Envelope.ConteudoCase.HEARTBEAT_RES) {
      logVerbose("[SERVIDOR] Resposta inesperada de relógio de " + nomeServidor + ": " + reply.envelope.getConteudoCase());
      return null;
    }
    Contrato.HeartbeatResponse resposta = reply.envelope.getHeartbeatRes();
    if (resposta.getStatus() != Contrato.Status.STATUS_SUCESSO) {
      logVerbose("[SERVIDOR] Servidor " + nomeServidor + " rejeitou pedido de relógio: " + resposta.getErroMsg());
      return null;
    }
    logVerbose("[SERVIDOR] Recebido heartbeat_res de " + nomeServidor + " " + Mensageria.cabecalhoTexto(reply.envelope.getCabecalho()));
    return reply;
  }

  private boolean consultarEleicaoServidor(String nomeServidor, boolean registrarLogs) {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);
    Contrato.RegisterServerRequest req = Contrato.RegisterServerRequest.newBuilder()
        .setCabecalho(cab)
        .setNomeServidor(serverName)
        .build();
    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setRegisterServerReq(req)
        .build();
    if (registrarLogs) {
      logEleicao("req destino=" + nomeServidor);
    }
    PeerReply reply = enviarParaServidor(nomeServidor, ELECTION_PORT, env, REQUEST_TIMEOUT_MS);
    if (reply == null) {
      if (registrarLogs) {
        logEleicao("sem_resposta destino=" + nomeServidor);
      }
      return false;
    }
    if (reply.envelope.getConteudoCase() != Contrato.Envelope.ConteudoCase.REGISTER_SERVER_RES) {
      if (registrarLogs) {
        logEleicao("resposta_invalida destino=" + nomeServidor);
      }
      return false;
    }
    Contrato.RegisterServerResponse resposta = reply.envelope.getRegisterServerRes();
    if (resposta.getStatus() != Contrato.Status.STATUS_SUCESSO) {
      if (registrarLogs) {
        logEleicao("rejeitada destino=" + nomeServidor + " motivo=" + resposta.getErroMsg());
      }
      return false;
    }
    if (registrarLogs) {
      logEleicao("ok origem=" + nomeServidor + " rank=" + resposta.getRank());
    }
    return true;
  }

  private boolean sincronizarComoSeguidor(String coordenador) {
    if (coordenador == null || coordenador.isBlank()) {
      return false;
    }

    PeerReply reply = consultarRelogioServidor(coordenador);
    if (reply == null) {
      return false;
    }

    Contrato.HeartbeatResponse resposta = reply.envelope.getHeartbeatRes();
    if (resposta.getStatus() != Contrato.Status.STATUS_SUCESSO) {
      return false;
    }

    relogio.atualizarOffset(reply.envelope.getCabecalho().getTimestampEnvio(), reply.envioNs, reply.recebimentoNs);
    logVerbose("[SERVIDOR] Offset físico atualizado para " + relogio.offsetMillis() + " ms usando coordenador " + coordenador);
    return true;
  }

  private void sincronizarComoCoordenador() {
    List<Contrato.ServerInfo> servidores = servidoresAtivosSnapshot();
    List<Long> amostras = new ArrayList<>();
    List<String> participantes = new ArrayList<>();
    List<String> falhas = new ArrayList<>();

    amostras.add(timestampToNs(relogio.nowCorrigidoTimestamp()));
    participantes.add(serverName);

    for (Contrato.ServerInfo servidor : servidores) {
      if (servidor.getNome().equals(serverName)) {
        continue;
      }
      PeerReply reply = consultarRelogioServidor(servidor.getNome());
      if (reply == null) {
        falhas.add(servidor.getNome());
        continue;
      }
      Contrato.HeartbeatResponse resposta = reply.envelope.getHeartbeatRes();
      if (resposta.getStatus() != Contrato.Status.STATUS_SUCESSO) {
        falhas.add(servidor.getNome());
        continue;
      }
      amostras.add(timestampToNs(reply.envelope.getCabecalho().getTimestampEnvio()));
      participantes.add(servidor.getNome());
    }

    long soma = 0L;
    for (Long amostra : amostras) {
      soma += amostra;
    }
    long mediaNs = soma / amostras.size();
    long agoraNs = Mensageria.agoraNs();
    relogio.atualizarOffset(Mensageria.timestampFromNs(mediaNs), agoraNs, agoraNs);
    logVerbose(
        "[SERVIDOR] Berkeley aplicado pelo coordenador " + serverName
            + " com participantes=" + String.join(", ", participantes)
            + " falhas=" + String.join(", ", falhas)
            + " media=" + Mensageria.timestampTexto(Mensageria.timestampFromNs(mediaNs))
            + " offset=" + relogio.offsetMillis() + " ms"
    );
  }

  private void sincronizarRelogioFisico() {
    String coordenador = coordenadorAtual();
    if (coordenador.isBlank()) {
      iniciarEleicaoAsync("coordenador_desconhecido");
      return;
    }

    if (isCoordenador()) {
      sincronizarComoCoordenador();
      return;
    }

    boolean coordenadorAtivo = false;
    for (Contrato.ServerInfo servidor : servidoresAtivosSnapshot()) {
      if (servidor.getNome().equals(coordenador)) {
        coordenadorAtivo = true;
        break;
      }
    }

    if (!coordenador.isBlank() && !coordenadorAtivo) {
      iniciarEleicaoAsync("coordenador_ausente_da_lista");
      return;
    }

    if (!coordenador.isBlank() && sincronizarComoSeguidor(coordenador)) {
      return;
    }

    iniciarEleicaoAsync("falha_sincronizar_com_" + coordenador);
  }

  private Contrato.HeartbeatResponse processarPedidoRelogio(Contrato.HeartbeatRequest req) {
    Contrato.HeartbeatResponse.Builder resposta = Contrato.HeartbeatResponse.newBuilder();
    String solicitante = req.getNomeServidor().trim();
    if (solicitante.isEmpty()) {
      solicitante = "desconhecido";
    }

    String coordenador = coordenadorAtual();
    if (isCoordenador()) {
      resposta.setStatus(Contrato.Status.STATUS_SUCESSO);
      logVerbose("[SERVIDOR] Respondendo relógio ao servidor " + solicitante + " como coordenador");
      return resposta.build();
    }

    if (!coordenador.isEmpty() && solicitante.equals(coordenador)) {
      resposta.setStatus(Contrato.Status.STATUS_SUCESSO);
      logVerbose("[SERVIDOR] Respondendo relógio ao coordenador " + solicitante);
      return resposta.build();
    }

    resposta.setStatus(Contrato.Status.STATUS_ERRO);
    resposta.setErroMsg("nao sou coordenador");
    logVerbose("[SERVIDOR] Pedido de relógio de " + solicitante + " rejeitado; coordenador atual=" + coordenador);
    return resposta.build();
  }

  private Contrato.RegisterServerResponse processarPedidoEleicao(Contrato.RegisterServerRequest req) {
    Contrato.RegisterServerResponse.Builder resposta = Contrato.RegisterServerResponse.newBuilder();
    String solicitante = req.getNomeServidor().trim();
    if (solicitante.isEmpty()) {
      solicitante = "desconhecido";
    }

    int rankSolicitante = rankServidor(solicitante);
    if (meuRank > rankSolicitante) {
      resposta.setStatus(Contrato.Status.STATUS_SUCESSO);
      resposta.setRank(meuRank);
      logEleicao("ok enviado destino=" + solicitante + " rank=" + meuRank + " solicitante_rank=" + rankSolicitante);
      if (isCoordenador()) {
        anunciarCoordenador();
      } else {
        iniciarEleicaoAsync("pedido de eleição recebido de " + solicitante);
      }
      return resposta.build();
    }

    resposta.setStatus(Contrato.Status.STATUS_ERRO);
    resposta.setErroMsg("rank inferior");
    resposta.setRank(meuRank);
    logEleicao("pedido ignorado origem=" + solicitante + " motivo=rank_inferior solicitante_rank=" + rankSolicitante + " meu_rank=" + meuRank);
    return resposta.build();
  }

  private int rankServidor(String nome) {
    synchronized (stateLock) {
      return servidoresAtivos.getOrDefault(nome, -1);
    }
  }

  private static final class PeerReply {
    private final Contrato.Envelope envelope;
    private final long envioNs;
    private final long recebimentoNs;

    private PeerReply(Contrato.Envelope envelope, long envioNs, long recebimentoNs) {
      this.envelope = envelope;
      this.envioNs = envioNs;
      this.recebimentoNs = recebimentoNs;
    }
  }

  public void loop() throws Exception {
    logVerbose("[SERVIDOR] Servidor iniciado. Aguardando mensagens...");
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
      logVerbose("[SERVIDOR] Recebido " + tipo + " " + Mensageria.cabecalhoTexto(env.getCabecalho()));

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

      logVerbose("[SERVIDOR] Enviando " + respEnv.getConteudoCase() + " " + Mensageria.cabecalhoTexto(outerCab));
      socket.send(Mensageria.envelopeBytes(respEnv.build()), 0);

      requestsProcessadas += 1;
      if (requestsProcessadas % 10 == 0) {
        heartbeatESincronizar();
      }
      if (requestsProcessadas % 15 == 0) {
        sincronizarRelogioFisico();
      }
    }
  }

  private static String getEnv(String name, String defaultValue) {
    String v = System.getenv(name);
    return (v == null || v.isEmpty()) ? defaultValue : v;
  }

  private static void configurarConsoleUtf8() {
    System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
    System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8));
  }

  public static void main(String[] args) throws Exception {
    configurarConsoleUtf8();

    String orqEndpoint = getEnv("ORQ_ENDPOINT_SERVIDOR", "tcp://orquestrador:5556");
    String proxyPubEndpoint = getEnv("PROXY_PUB_ENDPOINT", "tcp://proxy:5557");
    String proxySubEndpoint = getEnv("PROXY_SUB_ENDPOINT", "tcp://proxy:5558");
    String referenceEndpoint = getEnv("REFERENCE_ENDPOINT", "tcp://referencia:5559");
    String dataDir = getEnv("DATA_DIR", "/data");
    String serverName = getEnv("SERVER_NAME", "servidor_java");

    ServidorJava servidor = new ServidorJava(orqEndpoint, proxyPubEndpoint, proxySubEndpoint, referenceEndpoint, dataDir, serverName);
    servidor.loop();
  }
}
