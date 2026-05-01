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
  private static final String TOPICO_COORDENADOR = "servers";
  private static final int CLOCK_PORT = 5560;
  private static final int ELECTION_PORT = 5561;
  private static final int REQUEST_TIMEOUT_MS = 2000;
  private static final int ANNOUNCEMENT_TIMEOUT_MS = 3000;
  private static final int STARTUP_DELAY_MS = 200;

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
  private final Map<String, Integer> servidoresAtivos = new HashMap<>();

  private int requestsProcessadas = 0;
  private String coordenadorAtual = "";
  private long coordenadorVersao = 0L;
  private int meuRank = 0;
  private boolean electionRunning = false;

  private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

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
    this.origem = Mensageria.origemLabel("servidor");
    this.serverName = serverName;

    this.dataDir = Path.of(dataDirStr);
    this.loginsPath = this.dataDir.resolve("logins.jsonl");
    this.canaisPath = this.dataDir.resolve("canais.json");
    this.publicacoesPath = this.dataDir.resolve("publicacoes.jsonl");

    System.out.println("[SERVIDOR] Conectado ao orquestrador em " + orqEndpoint);
    System.out.println("[SERVIDOR] Conectado ao proxy pub em " + proxyPubEndpoint);
    System.out.println("[SERVIDOR] Conectado ao proxy sub em " + proxySubEndpoint);
    System.out.println("[SERVIDOR] Conectado à referência em " + referenceEndpoint);
    initStorage();
    registrarNaReferencia();
    List<Contrato.ServerInfo> servidores = listarServidoresReferencia();
    definirCoordenadorTentativo(servidores);
    iniciarServicosInternos();
    Thread.sleep(STARTUP_DELAY_MS);
    executarEleicao("startup");
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
    System.out.println(
        "[SERVIDOR] Enviando " + env.getConteudoCase() + " para referência " + Mensageria.cabecalhoTexto(env.getCabecalho())
    );
    refSocket.send(Mensageria.envelopeBytes(env), 0);
    byte[] reply = refSocket.recv(0);
    if (reply == null) {
      throw new IllegalStateException("Resposta nula da referência.");
    }
    Contrato.Envelope resp = Mensageria.envelopeFromBytes(reply);
    relogio.onReceive(resp.getCabecalho().getRelogioLogico());
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
    Contrato.Envelope resp = enviarParaReferencia(env);
    if (resp.getConteudoCase() != Contrato.Envelope.ConteudoCase.LIST_SERVERS_RES) {
      System.err.println("[SERVIDOR] Resposta inesperada ao listar servidores: " + resp.getConteudoCase());
      return new ArrayList<>();
    }
    List<Contrato.ServerInfo> servidores = new ArrayList<>(resp.getListServersRes().getServidoresList());
    atualizarServidoresAtivos(servidores);
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
    System.out.println("[SERVIDOR] Heartbeat enviado com sucesso para " + serverName);
    List<Contrato.ServerInfo> servidores = listarServidoresReferencia();
    String coordenador = coordenadorAtual();
    boolean encontrado = false;
    for (Contrato.ServerInfo servidor : servidores) {
      if (servidor.getNome().equals(coordenador)) {
        encontrado = true;
        break;
      }
    }
    if (!coordenador.isEmpty() && !encontrado) {
      System.out.println("[SERVIDOR] Coordenador " + coordenador + " ausente da lista ativa; iniciando eleição");
      iniciarEleicaoAsync("coordenador ausente da lista ativa");
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

  private void iniciarServicosInternos() {
    Thread clockThread = new Thread(this::loopRelogio, "servidor-java-clock");
    clockThread.setDaemon(true);
    clockThread.start();

    Thread electionThread = new Thread(this::loopEleicao, "servidor-java-election");
    electionThread.setDaemon(true);
    electionThread.start();

    Thread announcementThread = new Thread(this::loopAnunciosCoordenador, "servidor-java-announcements");
    announcementThread.setDaemon(true);
    announcementThread.start();
  }

  private void loopRelogio() {
    try (ZMQ.Socket sock = context.socket(ZMQ.REP)) {
      sock.bind("tcp://*:" + CLOCK_PORT);
      System.out.println("[SERVIDOR] Serviço de relógio interno ouvindo em tcp://*:" + CLOCK_PORT);
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
      System.out.println("[SERVIDOR] Serviço de eleição interno ouvindo em tcp://*:" + ELECTION_PORT);
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
    System.out.println("[SERVIDOR] Escutando anúncios de coordenador no tópico " + TOPICO_COORDENADOR);
    while (true) {
      try {
        byte[] topicBytes = subSocket.recv(0);
        byte[] payload = subSocket.recv(0);
        if (topicBytes == null || payload == null) {
          continue;
        }

        String topic = new String(topicBytes, ZMQ.CHARSET);
        Contrato.ChannelMessage channelMessage = Contrato.ChannelMessage.parseFrom(payload);
        relogio.onReceive(channelMessage.getRelogioLogico());
        String coordenador = channelMessage.getMensagem().trim();
        if (coordenador.isEmpty()) {
          continue;
        }
        setCoordenador(coordenador, "anúncio publicado por " + channelMessage.getRemetente(), true);
        System.out.println("[SERVIDOR] Anúncio de coordenador recebido em " + topic + ": " + coordenador);
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
    System.out.println("[SERVIDOR] Lista ativa local atualizada: " + resumoServidores(servidoresAtivosSnapshot()));
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

  private long versaoCoordenador() {
    synchronized (stateLock) {
      return coordenadorVersao;
    }
  }

  private void setCoordenador(String nome, String motivo, boolean incrementarVersao) {
    String anterior;
    long versao;
    synchronized (stateLock) {
      anterior = coordenadorAtual;
      coordenadorAtual = nome;
      if (incrementarVersao) {
        coordenadorVersao += 1;
      }
      versao = coordenadorVersao;
    }

    if (anterior.equals(nome)) {
      System.out.println("[SERVIDOR] Coordenador mantido em " + nome + " (" + motivo + ", versao=" + versao + ")");
    } else {
      System.out.println("[SERVIDOR] Coordenador atualizado de " + anterior + " para " + nome + " (" + motivo + ", versao=" + versao + ")");
    }
  }

  private void definirCoordenadorTentativo(List<Contrato.ServerInfo> servidores) {
    if (!coordenadorAtual().isEmpty()) {
      return;
    }
    String candidato = maiorServidorAtivo();
    setCoordenador(candidato, "coordenador tentativo inicial", false);
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
        System.out.println("[SERVIDOR] Eleição já em andamento; ignorando gatilho (" + motivo + ")");
        return;
      }
      electionRunning = true;
    }

    try {
      System.out.println("[SERVIDOR] Iniciando eleição (" + motivo + ")");
      long versaoInicial = versaoCoordenador();
      List<Contrato.ServerInfo> servidores = servidoresAtivosSnapshot();
      List<Contrato.ServerInfo> superiores = new ArrayList<>();
      for (Contrato.ServerInfo item : servidores) {
        if (item.getRank() > meuRank && !item.getNome().equals(serverName)) {
          superiores.add(item);
        }
      }

      superiores.sort((a, b) -> Integer.compare((int) b.getRank(), (int) a.getRank()));
      if (superiores.isEmpty()) {
        if (versaoCoordenador() > versaoInicial) {
          System.out.println("[SERVIDOR] Eleição concluída durante a coleta de respostas");
          return;
        }
        tornarCoordenador("nenhum servidor de maior rank respondeu");
        return;
      }

      List<Contrato.ServerInfo> respostasOk = new ArrayList<>();
      for (Contrato.ServerInfo item : superiores) {
        if (consultarEleicaoServidor(item.getNome())) {
          respostasOk.add(item);
        }
      }

      if (versaoCoordenador() > versaoInicial) {
        System.out.println("[SERVIDOR] Eleição concluída por anúncio recebido durante a coleta");
        return;
      }

      if (respostasOk.isEmpty()) {
        tornarCoordenador("nenhum servidor de maior rank respondeu");
        return;
      }

      long deadline = System.currentTimeMillis() + ANNOUNCEMENT_TIMEOUT_MS;
      while (System.currentTimeMillis() < deadline) {
        if (versaoCoordenador() > versaoInicial) {
          System.out.println("[SERVIDOR] Eleição concluída por anúncio de coordenador");
          return;
        }
        Thread.sleep(100);
      }

      Contrato.ServerInfo candidato = respostasOk.get(0);
      for (Contrato.ServerInfo item : respostasOk) {
        if (item.getRank() > candidato.getRank()) {
          candidato = item;
        }
      }
      setCoordenador(
          candidato.getNome(),
          "coordenador inferido após timeout de anúncio (" + motivo + ")",
          true
      );
    } catch (Exception e) {
      System.out.println("[SERVIDOR] Erro durante eleição: " + e.getMessage());
    } finally {
      synchronized (stateLock) {
        electionRunning = false;
      }
    }
  }

  private void tornarCoordenador(String motivo) {
    setCoordenador(serverName, motivo, true);
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
      System.out.println("[SERVIDOR] Anunciado coordenador em servers: " + serverName);
    } catch (Exception e) {
      System.out.println("[SERVIDOR] Falha ao anunciar coordenador: " + e.getMessage());
    }
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
      System.out.println("[SERVIDOR] Falha ao comunicar com " + nomeServidor + ":" + porta + " - " + e.getMessage());
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
    System.out.println("[SERVIDOR] Enviando heartbeat_req para " + nomeServidor + " " + Mensageria.cabecalhoTexto(cab));
    PeerReply reply = enviarParaServidor(nomeServidor, CLOCK_PORT, env, REQUEST_TIMEOUT_MS);
    if (reply == null) {
      System.out.println("[SERVIDOR] Sem resposta de relógio de " + nomeServidor);
      return null;
    }
    if (reply.envelope.getConteudoCase() != Contrato.Envelope.ConteudoCase.HEARTBEAT_RES) {
      System.out.println("[SERVIDOR] Resposta inesperada de relógio de " + nomeServidor + ": " + reply.envelope.getConteudoCase());
      return null;
    }
    Contrato.HeartbeatResponse resposta = reply.envelope.getHeartbeatRes();
    if (resposta.getStatus() != Contrato.Status.STATUS_SUCESSO) {
      System.out.println("[SERVIDOR] Servidor " + nomeServidor + " rejeitou pedido de relógio: " + resposta.getErroMsg());
      return null;
    }
    System.out.println("[SERVIDOR] Recebido heartbeat_res de " + nomeServidor + " " + Mensageria.cabecalhoTexto(reply.envelope.getCabecalho()));
    return reply;
  }

  private boolean consultarEleicaoServidor(String nomeServidor) {
    Contrato.Cabecalho cab = Mensageria.novoCabecalho(origem, relogio);
    Contrato.RegisterServerRequest req = Contrato.RegisterServerRequest.newBuilder()
        .setCabecalho(cab)
        .setNomeServidor(serverName)
        .build();
    Contrato.Envelope env = Contrato.Envelope.newBuilder()
        .setCabecalho(cab)
        .setRegisterServerReq(req)
        .build();
    System.out.println("[SERVIDOR] Enviando pedido de eleição para " + nomeServidor + " " + Mensageria.cabecalhoTexto(cab));
    PeerReply reply = enviarParaServidor(nomeServidor, ELECTION_PORT, env, REQUEST_TIMEOUT_MS);
    if (reply == null) {
      System.out.println("[SERVIDOR] Sem resposta de eleição de " + nomeServidor);
      return false;
    }
    if (reply.envelope.getConteudoCase() != Contrato.Envelope.ConteudoCase.REGISTER_SERVER_RES) {
      System.out.println("[SERVIDOR] Resposta inesperada de eleição de " + nomeServidor + ": " + reply.envelope.getConteudoCase());
      return false;
    }
    Contrato.RegisterServerResponse resposta = reply.envelope.getRegisterServerRes();
    if (resposta.getStatus() != Contrato.Status.STATUS_SUCESSO) {
      System.out.println("[SERVIDOR] Servidor " + nomeServidor + " rejeitou eleição: " + resposta.getErroMsg());
      return false;
    }
    System.out.println("[SERVIDOR] Servidor " + nomeServidor + " respondeu OK à eleição (rank=" + resposta.getRank() + ")");
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
    System.out.println("[SERVIDOR] Offset físico atualizado para " + relogio.offsetMillis() + " ms usando coordenador " + coordenador);
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
    System.out.println(
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
      System.out.println("[SERVIDOR] Coordenador desconhecido; iniciando eleição");
      executarEleicao("coordenador desconhecido");
      coordenador = coordenadorAtual();
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
      System.out.println("[SERVIDOR] Coordenador " + coordenador + " ausente da lista ativa; iniciando eleição");
      executarEleicao("coordenador ausente da lista ativa");
      coordenador = coordenadorAtual();
      if (isCoordenador()) {
        sincronizarComoCoordenador();
        return;
      }
    }

    if (!coordenador.isBlank() && sincronizarComoSeguidor(coordenador)) {
      return;
    }

    System.out.println("[SERVIDOR] Falha ao sincronizar com coordenador " + coordenador + "; iniciando eleição");
    executarEleicao("falha ao sincronizar com coordenador");
    coordenador = coordenadorAtual();
    if (isCoordenador()) {
      sincronizarComoCoordenador();
      return;
    }
    if (!coordenador.isBlank() && !sincronizarComoSeguidor(coordenador)) {
      System.out.println("[SERVIDOR] Ainda não foi possível sincronizar com " + coordenador + " após a eleição");
    }
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
      System.out.println("[SERVIDOR] Respondendo relógio ao servidor " + solicitante + " como coordenador");
      return resposta.build();
    }

    if (!coordenador.isEmpty() && solicitante.equals(coordenador)) {
      resposta.setStatus(Contrato.Status.STATUS_SUCESSO);
      System.out.println("[SERVIDOR] Respondendo relógio ao coordenador " + solicitante);
      return resposta.build();
    }

    resposta.setStatus(Contrato.Status.STATUS_ERRO);
    resposta.setErroMsg("nao sou coordenador");
    System.out.println("[SERVIDOR] Pedido de relógio de " + solicitante + " rejeitado; coordenador atual=" + coordenador);
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
      System.out.println("[SERVIDOR] Respondendo eleição OK para " + solicitante + " (solicitante rank=" + rankSolicitante + ", meu rank=" + meuRank + ")");
      if (!isCoordenador()) {
        iniciarEleicaoAsync("pedido de eleição recebido de " + solicitante);
      }
      return resposta.build();
    }

    resposta.setStatus(Contrato.Status.STATUS_ERRO);
    resposta.setErroMsg("rank inferior");
    resposta.setRank(meuRank);
    System.out.println("[SERVIDOR] Pedido de eleição de " + solicitante + " rejeitado (solicitante rank=" + rankSolicitante + ", meu rank=" + meuRank + ")");
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
      if (requestsProcessadas % 15 == 0) {
        sincronizarRelogioFisico();
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
    String proxySubEndpoint = getEnv("PROXY_SUB_ENDPOINT", "tcp://proxy:5558");
    String referenceEndpoint = getEnv("REFERENCE_ENDPOINT", "tcp://referencia:5559");
    String dataDir = getEnv("DATA_DIR", "/data");
    String serverName = getEnv("SERVER_NAME", "servidor_java");

    ServidorJava servidor = new ServidorJava(orqEndpoint, proxyPubEndpoint, proxySubEndpoint, referenceEndpoint, dataDir, serverName);
    servidor.loop();
  }
}
