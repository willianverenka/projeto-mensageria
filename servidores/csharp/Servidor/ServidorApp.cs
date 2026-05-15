using System.Text.Json;
using Chat;
using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;
using Shared;

namespace Servidor;

public class ServidorApp
{
    private const string TopicoCoordenador = "servers";
    private const string TopicoReplica = "__replica__";
    private const string ErroServidorNaoRegistrado = "servidor não registrado";
    private const int ClockPort = 5560;
    private const int ElectionPort = 5561;
    private const int SnapshotPort = 5562;
    private const int RequestTimeoutMs = 2000;
    private const int SnapshotTimeoutMs = 10000;
    private static readonly string LogMode =
        (Environment.GetEnvironmentVariable("SERVER_LOG_MODE") ?? "presentation").Trim().ToLowerInvariant();
    private static readonly int TimeoutTesteCoordenador = GetEnvInt("TIMEOUT_TESTE_COORDENADOR", 500);

    private readonly string _orqEndpoint;
    private readonly string _proxyPubEndpoint;
    private readonly string _proxySubEndpoint;
    private readonly string _referenceEndpoint;
    private readonly string _serverName;
    private readonly string _origem;
    private readonly string _dataDir;
    private readonly string _loginsPath;
    private readonly string _canaisPath;
    private readonly string _publicacoesPath;
    private readonly DealerSocket _socket;
    private readonly PublisherSocket _pubSocket;
    private readonly PublisherSocket _announceSocket;
    private readonly RequestSocket _referenceSocket;
    private readonly SubscriberSocket _subSocket;
    private readonly object _stateLock = new();
    private readonly object _referenceLock = new();
    private readonly object _storageLock = new();
    private readonly RelogioProcesso _relogio = new();
    private readonly Dictionary<string, int> _servidoresAtivos = new();
    private readonly Dictionary<string, int> _ranksConhecidos = new();
    private string _coordenadorAtual = "";
    private int _meuRank = 0;
    private bool _electionRunning = false;
    private int _requestsProcessadas = 0;

    private static bool IsVerbose() => LogMode == "verbose";

    private static void LogVerbose(string message)
    {
        if (IsVerbose())
        {
            Console.WriteLine(message);
        }
    }

    private static void LogEleicao(string message)
    {
        Console.WriteLine($"[ELEICAO] {message}");
    }

    private static void LogEleicaoVerbose(string message)
    {
        LogVerbose($"[ELEICAO] {message}");
    }

    private static int GetEnvInt(string name, int defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return int.TryParse(value, out var parsed) ? parsed : defaultValue;
    }

    public ServidorApp()
    {
        _orqEndpoint = Environment.GetEnvironmentVariable("ORQ_ENDPOINT_SERVIDOR") ?? "tcp://localhost:5556";
        _proxyPubEndpoint = Environment.GetEnvironmentVariable("PROXY_PUB_ENDPOINT") ?? "tcp://proxy:5557";
        _proxySubEndpoint = Environment.GetEnvironmentVariable("PROXY_SUB_ENDPOINT") ?? "tcp://proxy:5558";
        _referenceEndpoint = Environment.GetEnvironmentVariable("REFERENCE_ENDPOINT") ?? "tcp://referencia:5559";
        _serverName = Environment.GetEnvironmentVariable("SERVER_NAME") ?? "servidor_csharp";
        _origem = Mensageria.OrigemLabel("servidor");
        _dataDir = Environment.GetEnvironmentVariable("DATA_DIR") ?? "./data";
        
        Directory.CreateDirectory(_dataDir);
        _loginsPath = Path.Combine(_dataDir, "logins.jsonl");
        _canaisPath = Path.Combine(_dataDir, "canais.json");
        _publicacoesPath = Path.Combine(_dataDir, "publicacoes.jsonl");

        InitStorage();

        _socket = new DealerSocket();
        _socket.Connect(_orqEndpoint);
        LogVerbose($"[SERVIDOR] Conectado ao orquestrador em {_orqEndpoint}");
        _pubSocket = new PublisherSocket();
        _pubSocket.Connect(_proxyPubEndpoint);
        LogVerbose($"[SERVIDOR] Conectado ao proxy pub em {_proxyPubEndpoint}");
        _announceSocket = new PublisherSocket();
        _announceSocket.Connect(_proxyPubEndpoint);
        _referenceSocket = new RequestSocket();
        _referenceSocket.Connect(_referenceEndpoint);
        LogVerbose($"[SERVIDOR] Conectado à referência em {_referenceEndpoint}");
        _subSocket = new SubscriberSocket();
        _subSocket.Connect(_proxySubEndpoint);
        _subSocket.Subscribe(TopicoCoordenador);
        _subSocket.Subscribe(TopicoReplica);
        LogVerbose($"[SERVIDOR] Conectado ao proxy sub em {_proxySubEndpoint}");

        RegistrarNaReferencia();
        var servidores = ListarServidoresReferencia();
        AtualizarServidoresAtivos(servidores);
        IniciarServicosInternos();
        SincronizarReplicas();
        IniciarEleicaoAsync("inicializacao");
        Console.WriteLine($"[SERVIDOR] Servidor pronto. Rank local={_meuRank} coordenador={CoordenadorAtual()}");
    }

    private void InitStorage()
    {
        if (!File.Exists(_loginsPath)) File.WriteAllText(_loginsPath, "");
        if (!File.Exists(_canaisPath)) File.WriteAllText(_canaisPath, "[]");
        if (!File.Exists(_publicacoesPath)) File.WriteAllText(_publicacoesPath, "");
    }

    private Envelope EnviarParaReferencia(Envelope env)
    {
        LogVerbose($"[SERVIDOR] Enviando {env.ConteudoCase} para referência {Mensageria.CabecalhoTexto(env.Cabecalho)}");
        byte[] data;
        lock (_referenceLock)
        {
            _referenceSocket.SendFrame(env.ToByteArray());
            data = _referenceSocket.ReceiveFrameBytes();
        }
        var resp = Mensageria.EnvelopeFromBytes(data);
        _relogio.OnReceive(resp.Cabecalho.RelogioLogico);
        LogVerbose($"[SERVIDOR] Recebido {resp.ConteudoCase} da referência {Mensageria.CabecalhoTexto(resp.Cabecalho)}");
        return resp;
    }

    private void RegistrarNaReferencia()
    {
        var cab = Mensageria.NovoCabecalho(_origem, _relogio);
        var env = new Envelope
        {
            Cabecalho = cab,
            RegisterServerReq = new RegisterServerRequest
            {
                Cabecalho = cab.Clone(),
                NomeServidor = _serverName
            }
        };

        var resp = EnviarParaReferencia(env);
        if (resp.ConteudoCase != Envelope.ConteudoOneofCase.RegisterServerRes)
        {
            throw new InvalidOperationException($"Resposta inesperada ao registrar na referência: {resp.ConteudoCase}");
        }

        if (resp.RegisterServerRes.Status != Status.Sucesso)
        {
            throw new InvalidOperationException($"Falha ao registrar na referência: {resp.RegisterServerRes.ErroMsg}");
        }

        var rankAnterior = _meuRank;
        _meuRank = resp.RegisterServerRes.Rank;
        if (rankAnterior > 0 && rankAnterior != _meuRank)
        {
            LogVerbose($"[SERVIDOR] Rank local atualizado de {rankAnterior} para {_meuRank} após registro na referência");
        }
        AtualizarServidoresAtivos([new ServerInfo { Nome = _serverName, Rank = _meuRank }]);
        LogVerbose($"[SERVIDOR] Servidor {_serverName} registrado na referência com rank={resp.RegisterServerRes.Rank}");
    }

    private bool RegistrarNovamenteAposHeartbeat()
    {
        try
        {
            RegistrarNaReferencia();
            LogVerbose($"[SERVIDOR] Heartbeat recuperado: servidor registrado novamente rank={_meuRank}");
            return true;
        }
        catch (Exception ex)
        {
            LogVerbose($"[SERVIDOR] Falha ao registrar novamente após heartbeat rejeitado: {ex.Message}");
            return false;
        }
    }

    private List<ServerInfo> ListarServidoresReferencia()
    {
        var cab = Mensageria.NovoCabecalho(_origem, _relogio);
        var env = new Envelope
        {
            Cabecalho = cab,
            ListServersReq = new ListServersRequest
            {
                Cabecalho = cab.Clone()
            }
        };

        var resp = EnviarParaReferencia(env);
        if (resp.ConteudoCase != Envelope.ConteudoOneofCase.ListServersRes)
        {
            Console.WriteLine($"[SERVIDOR] Resposta inesperada ao listar servidores: {resp.ConteudoCase}");
            return [];
        }

        var servidores = resp.ListServersRes.Servidores.ToList();
        AtualizarServidoresAtivos(servidores);
        var resumo = servidores.Count == 0
            ? "(nenhum)"
            : string.Join(", ", servidores.Select(item => $"{item.Nome}(rank={item.Rank})"));
        LogVerbose($"[SERVIDOR] Servidores disponíveis na referência: {resumo}");
        return servidores;
    }

    private void HeartbeatESincronizar()
    {
        var cab = Mensageria.NovoCabecalho(_origem, _relogio);
        var env = new Envelope
        {
            Cabecalho = cab,
            HeartbeatReq = new HeartbeatRequest
            {
                Cabecalho = cab.Clone(),
                NomeServidor = _serverName
            }
        };

        var resp = EnviarParaReferencia(env);
        if (resp.ConteudoCase != Envelope.ConteudoOneofCase.HeartbeatRes)
        {
            LogVerbose($"[SERVIDOR] Resposta inesperada ao heartbeat: {resp.ConteudoCase}");
            return;
        }

        if (resp.HeartbeatRes.Status != Status.Sucesso)
        {
            if (resp.HeartbeatRes.ErroMsg == ErroServidorNaoRegistrado)
            {
                LogVerbose("[SERVIDOR] Heartbeat rejeitado: servidor não registrado; registrando novamente");
                if (!RegistrarNovamenteAposHeartbeat())
                {
                    return;
                }
            }
            else
            {
                LogVerbose($"[SERVIDOR] Heartbeat rejeitado: {resp.HeartbeatRes.ErroMsg}");
                return;
            }
        }
        else
        {
            LogVerbose($"[SERVIDOR] Heartbeat enviado com sucesso para {_serverName}");
        }

        ListarServidoresReferencia();
        SincronizarReplicas();
    }

    private List<string> LerCanais()
    {
        lock (_storageLock)
        {
            try
            {
                var canais = JsonSerializer.Deserialize<List<string>>(File.ReadAllText(_canaisPath)) ?? [];
                return NormalizarCanais(canais);
            }
            catch
            {
                return [];
            }
        }
    }

    private void SalvarCanais(List<string> canais)
    {
        lock (_storageLock)
        {
            File.WriteAllText(_canaisPath, JsonSerializer.Serialize(NormalizarCanais(canais)));
        }
    }

    private static List<string> NormalizarCanais(IEnumerable<string> canais)
        => canais
            .Select(canal => canal.Trim())
            .Where(canal => !string.IsNullOrWhiteSpace(canal))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(canal => canal, StringComparer.Ordinal)
            .ToList();

    private bool AdicionarCanalIdempotente(string nome)
    {
        lock (_storageLock)
        {
            var canais = LerCanais();
            if (canais.Contains(nome))
            {
                return false;
            }

            canais.Add(nome);
            SalvarCanais(canais);
            return true;
        }
    }

    private List<Dictionary<string, string>> LerJsonl(string path)
    {
        var registros = new List<Dictionary<string, string>>();
        if (!File.Exists(path))
        {
            return registros;
        }

        foreach (var line in File.ReadAllLines(path))
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            try
            {
                var item = JsonSerializer.Deserialize<Dictionary<string, string>>(line);
                if (item is not null)
                {
                    registros.Add(item);
                }
            }
            catch
            {
            }
        }

        return registros;
    }

    private void SalvarJsonl(string path, List<Dictionary<string, string>> registros)
    {
        var lines = registros.Select(registro => JsonSerializer.Serialize(registro));
        File.WriteAllText(path, string.Join(Environment.NewLine, lines) + (registros.Count > 0 ? Environment.NewLine : ""));
    }

    private void RegistrarLogin(string usuario, string ts) 
    {
        lock (_storageLock)
        {
            var registros = LerJsonl(_loginsPath);
            if (registros.Any(item =>
                    item.GetValueOrDefault("usuario") == usuario &&
                    item.GetValueOrDefault("timestamp") == ts))
            {
                return;
            }

            registros.Add(new Dictionary<string, string>
            {
                ["usuario"] = usuario,
                ["timestamp"] = ts
            });
            registros = registros
                .OrderBy(item => item.GetValueOrDefault("timestamp"), StringComparer.Ordinal)
                .ThenBy(item => item.GetValueOrDefault("usuario"), StringComparer.Ordinal)
                .ToList();
            SalvarJsonl(_loginsPath, registros);
        }
    }

    private void RegistrarPublicacao(string canal, string mensagem, string remetente, string ts)
    {
        lock (_storageLock)
        {
            var registros = LerJsonl(_publicacoesPath);
            if (registros.Any(item =>
                    item.GetValueOrDefault("canal") == canal &&
                    item.GetValueOrDefault("mensagem") == mensagem &&
                    item.GetValueOrDefault("remetente") == remetente &&
                    item.GetValueOrDefault("timestamp") == ts))
            {
                return;
            }

            registros.Add(new Dictionary<string, string>
            {
                ["canal"] = canal,
                ["mensagem"] = mensagem,
                ["remetente"] = remetente,
                ["timestamp"] = ts
            });
            registros = registros
                .OrderBy(item => item.GetValueOrDefault("timestamp"), StringComparer.Ordinal)
                .ThenBy(item => item.GetValueOrDefault("canal"), StringComparer.Ordinal)
                .ThenBy(item => item.GetValueOrDefault("remetente"), StringComparer.Ordinal)
                .ThenBy(item => item.GetValueOrDefault("mensagem"), StringComparer.Ordinal)
                .ToList();
            SalvarJsonl(_publicacoesPath, registros);
        }
    }

    private string ReplicaOrigem() => $"replica:{_serverName}";

    private static global::Google.Protobuf.WellKnownTypes.Timestamp TimestampTextoParaTimestamp(string texto)
    {
        try
        {
            var partes = texto.Split('.', 2);
            var segundos = long.Parse(partes[0]);
            var nanosTexto = partes.Length > 1 ? partes[1] : "0";
            if (nanosTexto.Length > 9)
            {
                nanosTexto = nanosTexto[..9];
            }
            nanosTexto = nanosTexto.PadRight(9, '0');
            var nanos = int.Parse(nanosTexto);
            return new global::Google.Protobuf.WellKnownTypes.Timestamp
            {
                Seconds = segundos,
                Nanos = nanos
            };
        }
        catch
        {
            return new global::Google.Protobuf.WellKnownTypes.Timestamp { Seconds = 0, Nanos = 0 };
        }
    }

    private Envelope NovoEnvelopeReplicacao()
        => new() { Cabecalho = Mensageria.NovoCabecalho(ReplicaOrigem(), _relogio) };

    private void PublicarReplicacao(Envelope env)
    {
        try
        {
            _pubSocket.SendMoreFrame(TopicoReplica).SendFrame(env.ToByteArray());
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVIDOR] Falha ao publicar réplica: {ex.Message}");
        }
    }

    private void ReplicarLogin(LoginRequest req)
    {
        var env = NovoEnvelopeReplicacao();
        env.LoginReq = req.Clone();
        PublicarReplicacao(env);
    }

    private void ReplicarCreateChannel(CreateChannelRequest req)
    {
        var env = NovoEnvelopeReplicacao();
        env.CreateChannelReq = req.Clone();
        PublicarReplicacao(env);
    }

    private void ReplicarPublicacao(string canal, string mensagem, string remetente, Cabecalho cabPublicacao)
    {
        var cab = cabPublicacao.Clone();
        cab.LinguagemOrigem = remetente;
        var env = NovoEnvelopeReplicacao();
        env.PublishReq = new PublishRequest
        {
            Cabecalho = cab,
            Canal = canal,
            Mensagem = mensagem
        };
        PublicarReplicacao(env);
    }

    private List<Envelope> OperacoesSnapshot()
    {
        lock (_storageLock)
        {
            var operacoes = new List<Envelope>();
            foreach (var item in LerJsonl(_loginsPath))
            {
                var nome = item.GetValueOrDefault("usuario")?.Trim() ?? "";
                if (string.IsNullOrWhiteSpace(nome))
                {
                    continue;
                }

                var cab = new Cabecalho
                {
                    LinguagemOrigem = "snapshot",
                    TimestampEnvio = TimestampTextoParaTimestamp(item.GetValueOrDefault("timestamp") ?? "0.000000000")
                };
                var env = NovoEnvelopeReplicacao();
                env.LoginReq = new LoginRequest { Cabecalho = cab, NomeUsuario = nome };
                operacoes.Add(env);
            }

            foreach (var canal in LerCanais())
            {
                var cab = new Cabecalho
                {
                    LinguagemOrigem = "snapshot",
                    TimestampEnvio = TimestampTextoParaTimestamp("0.000000000")
                };
                var env = NovoEnvelopeReplicacao();
                env.CreateChannelReq = new CreateChannelRequest { Cabecalho = cab, NomeCanal = canal };
                operacoes.Add(env);
            }

            foreach (var item in LerJsonl(_publicacoesPath))
            {
                var canal = item.GetValueOrDefault("canal")?.Trim() ?? "";
                var mensagem = item.GetValueOrDefault("mensagem")?.Trim() ?? "";
                if (string.IsNullOrWhiteSpace(canal) || string.IsNullOrWhiteSpace(mensagem))
                {
                    continue;
                }

                var remetente = item.GetValueOrDefault("remetente")?.Trim() ?? "";
                if (string.IsNullOrWhiteSpace(remetente))
                {
                    remetente = "desconhecido";
                }

                var cab = new Cabecalho
                {
                    LinguagemOrigem = remetente,
                    TimestampEnvio = TimestampTextoParaTimestamp(item.GetValueOrDefault("timestamp") ?? "0.000000000")
                };
                var env = NovoEnvelopeReplicacao();
                env.PublishReq = new PublishRequest { Cabecalho = cab, Canal = canal, Mensagem = mensagem };
                operacoes.Add(env);
            }

            return operacoes;
        }
    }

    private void AplicarOperacaoReplicada(Envelope env)
    {
        switch (env.ConteudoCase)
        {
            case Envelope.ConteudoOneofCase.LoginReq:
                var usuario = env.LoginReq.NomeUsuario?.Trim() ?? "";
                if (!string.IsNullOrWhiteSpace(usuario))
                {
                    RegistrarLogin(usuario, Mensageria.TimestampTexto(env.LoginReq.Cabecalho.TimestampEnvio));
                }
                break;
            case Envelope.ConteudoOneofCase.CreateChannelReq:
                var nomeCanal = env.CreateChannelReq.NomeCanal?.Trim() ?? "";
                if (!string.IsNullOrWhiteSpace(nomeCanal))
                {
                    AdicionarCanalIdempotente(nomeCanal);
                }
                break;
            case Envelope.ConteudoOneofCase.PublishReq:
                var req = env.PublishReq;
                var canal = req.Canal?.Trim() ?? "";
                var mensagem = req.Mensagem?.Trim() ?? "";
                if (string.IsNullOrWhiteSpace(canal) || string.IsNullOrWhiteSpace(mensagem))
                {
                    return;
                }
                AdicionarCanalIdempotente(canal);
                var remetente = req.Cabecalho.LinguagemOrigem?.Trim() ?? "";
                if (string.IsNullOrWhiteSpace(remetente))
                {
                    remetente = "desconhecido";
                }
                RegistrarPublicacao(canal, mensagem, remetente, Mensageria.TimestampTexto(req.Cabecalho.TimestampEnvio));
                break;
        }
    }

    private static string ChaveLogin(Dictionary<string, string> item)
        => $"{item.GetValueOrDefault("usuario")}\0{item.GetValueOrDefault("timestamp")}";

    private static string ChavePublicacao(Dictionary<string, string> item)
        => $"{item.GetValueOrDefault("canal")}\0{item.GetValueOrDefault("mensagem")}\0{item.GetValueOrDefault("remetente")}\0{item.GetValueOrDefault("timestamp")}";

    private void AplicarOperacoesReplicadas(IEnumerable<Envelope> operacoes)
    {
        lock (_storageLock)
        {
            var canais = LerCanais().ToHashSet(StringComparer.Ordinal);

            var loginsPorChave = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
            foreach (var item in LerJsonl(_loginsPath))
            {
                loginsPorChave[ChaveLogin(item)] = item;
            }
            var publicacoesPorChave = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
            foreach (var item in LerJsonl(_publicacoesPath))
            {
                publicacoesPorChave[ChavePublicacao(item)] = item;
            }

            foreach (var env in operacoes)
            {
                switch (env.ConteudoCase)
                {
                    case Envelope.ConteudoOneofCase.LoginReq:
                        var usuario = env.LoginReq.NomeUsuario?.Trim() ?? "";
                        if (!string.IsNullOrWhiteSpace(usuario))
                        {
                            var item = new Dictionary<string, string>
                            {
                                ["usuario"] = usuario,
                                ["timestamp"] = Mensageria.TimestampTexto(env.LoginReq.Cabecalho.TimestampEnvio)
                            };
                            loginsPorChave[ChaveLogin(item)] = item;
                        }
                        break;
                    case Envelope.ConteudoOneofCase.CreateChannelReq:
                        var nomeCanal = env.CreateChannelReq.NomeCanal?.Trim() ?? "";
                        if (!string.IsNullOrWhiteSpace(nomeCanal))
                        {
                            canais.Add(nomeCanal);
                        }
                        break;
                    case Envelope.ConteudoOneofCase.PublishReq:
                        var req = env.PublishReq;
                        var canal = req.Canal?.Trim() ?? "";
                        var mensagem = req.Mensagem?.Trim() ?? "";
                        if (string.IsNullOrWhiteSpace(canal) || string.IsNullOrWhiteSpace(mensagem))
                        {
                            continue;
                        }
                        canais.Add(canal);
                        var remetente = req.Cabecalho.LinguagemOrigem?.Trim() ?? "";
                        if (string.IsNullOrWhiteSpace(remetente))
                        {
                            remetente = "desconhecido";
                        }
                        var publicacao = new Dictionary<string, string>
                        {
                            ["canal"] = canal,
                            ["mensagem"] = mensagem,
                            ["remetente"] = remetente,
                            ["timestamp"] = Mensageria.TimestampTexto(req.Cabecalho.TimestampEnvio)
                        };
                        publicacoesPorChave[ChavePublicacao(publicacao)] = publicacao;
                        break;
                }
            }

            SalvarCanais(canais.ToList());
            var logins = loginsPorChave.Values
                .OrderBy(item => item.GetValueOrDefault("timestamp"), StringComparer.Ordinal)
                .ThenBy(item => item.GetValueOrDefault("usuario"), StringComparer.Ordinal)
                .ToList();
            var publicacoes = publicacoesPorChave.Values
                .OrderBy(item => item.GetValueOrDefault("timestamp"), StringComparer.Ordinal)
                .ThenBy(item => item.GetValueOrDefault("canal"), StringComparer.Ordinal)
                .ThenBy(item => item.GetValueOrDefault("remetente"), StringComparer.Ordinal)
                .ThenBy(item => item.GetValueOrDefault("mensagem"), StringComparer.Ordinal)
                .ToList();
            SalvarJsonl(_loginsPath, logins);
            SalvarJsonl(_publicacoesPath, publicacoes);
        }
    }

    private Envelope RespostaSnapshotStatus(Status status, string erroMsg)
    {
        var cab = Mensageria.NovoCabecalho(_origem, _relogio);
        return new Envelope
        {
            Cabecalho = cab,
            HeartbeatRes = new HeartbeatResponse
            {
                Cabecalho = cab.Clone(),
                Status = status,
                ErroMsg = erroMsg
            }
        };
    }

    private void IniciarServicosInternos()
    {
        _ = Task.Run(LoopRelogio);
        _ = Task.Run(LoopEleicao);
        _ = Task.Run(LoopSnapshot);
        _ = Task.Run(LoopAnunciosCoordenador);
    }

    private void LoopSnapshot()
    {
        using var sock = new ResponseSocket();
        sock.Bind($"tcp://*:{SnapshotPort}");
        LogVerbose($"[SERVIDOR] Serviço de snapshot ouvindo em tcp://*:{SnapshotPort}");

        while (true)
        {
            try
            {
                var data = sock.ReceiveFrameBytes();
                var env = Mensageria.EnvelopeFromBytes(data);
                _relogio.OnReceive(env.Cabecalho.RelogioLogico);

                var status = env.ConteudoCase == Envelope.ConteudoOneofCase.HeartbeatReq
                    ? RespostaSnapshotStatus(Status.Sucesso, "")
                    : RespostaSnapshotStatus(Status.Erro, "tipo nao suportado para snapshot");

                var message = new NetMQMessage();
                message.Append(status.ToByteArray());
                if (status.HeartbeatRes.Status == Status.Sucesso)
                {
                    foreach (var op in OperacoesSnapshot())
                    {
                        message.Append(op.ToByteArray());
                    }
                }
                sock.SendMultipartMessage(message);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SERVIDOR] Erro no serviço de snapshot: {ex.Message}");
            }
        }
    }

    private void SincronizarReplicas()
    {
        var candidatos = new List<string>();
        var coordenador = CoordenadorAtual();
        if (!string.IsNullOrWhiteSpace(coordenador) && coordenador != _serverName)
        {
            candidatos.Add(coordenador);
        }
        foreach (var servidor in ServidoresAtivosSnapshot())
        {
            if (servidor.Nome != _serverName && !candidatos.Contains(servidor.Nome))
            {
                candidatos.Add(servidor.Nome);
            }
        }

        foreach (var nome in candidatos)
        {
            SolicitarSnapshot(nome);
        }
    }

    private bool SolicitarSnapshot(string nomeServidor)
    {
        try
        {
            using var sock = new RequestSocket();
            sock.Options.Linger = TimeSpan.Zero;
            sock.Connect($"tcp://{nomeServidor}:{SnapshotPort}");

            var cab = Mensageria.NovoCabecalho(_origem, _relogio);
            var env = new Envelope
            {
                Cabecalho = cab,
                HeartbeatReq = new HeartbeatRequest
                {
                    Cabecalho = cab.Clone(),
                    NomeServidor = _serverName
                }
            };

            var timeout = TimeSpan.FromMilliseconds(SnapshotTimeoutMs);
            if (!sock.TrySendFrame(timeout, env.ToByteArray(), false))
            {
                return false;
            }

            var message = new NetMQMessage();
            if (!sock.TryReceiveMultipartMessage(timeout, ref message) || message.FrameCount == 0)
            {
                return false;
            }

            var statusEnv = Mensageria.EnvelopeFromBytes(message[0].ToByteArray(false));
            _relogio.OnReceive(statusEnv.Cabecalho.RelogioLogico);
            if (statusEnv.ConteudoCase != Envelope.ConteudoOneofCase.HeartbeatRes ||
                statusEnv.HeartbeatRes.Status != Status.Sucesso)
            {
                return false;
            }

            var operacoes = new List<Envelope>();
            for (var i = 1; i < message.FrameCount; i++)
            {
                var op = Mensageria.EnvelopeFromBytes(message[i].ToByteArray(false));
                _relogio.OnReceive(op.Cabecalho.RelogioLogico);
                operacoes.Add(op);
            }
            if (operacoes.Count > 0)
            {
                AplicarOperacoesReplicadas(operacoes);
            }
            var aplicadas = operacoes.Count;
            if (aplicadas > 0)
            {
                LogVerbose($"[SERVIDOR] Snapshot aplicado de {nomeServidor} com {aplicadas} operações");
            }
            return true;
        }
        catch
        {
            return false;
        }
    }

    private void LoopRelogio()
    {
        using var sock = new ResponseSocket();
        sock.Bind($"tcp://*:{ClockPort}");
        LogVerbose($"[SERVIDOR] Serviço de relógio interno ouvindo em tcp://*:{ClockPort}");

        while (true)
        {
            try
            {
                var data = sock.ReceiveFrameBytes();
                var env = Mensageria.EnvelopeFromBytes(data);
                _relogio.OnReceive(env.Cabecalho.RelogioLogico);

                HeartbeatResponse resposta = env.ConteudoCase == Envelope.ConteudoOneofCase.HeartbeatReq
                    ? ProcessarPedidoRelogio(env.HeartbeatReq)
                    : new HeartbeatResponse
                    {
                        Status = Status.Erro,
                        ErroMsg = "tipo nao suportado"
                    };

                var cab = Mensageria.NovoCabecalho(_origem, _relogio);
                resposta.Cabecalho = cab.Clone();
                var respostaEnv = new Envelope
                {
                    Cabecalho = cab,
                    HeartbeatRes = resposta
                };
                sock.SendFrame(Mensageria.EnvelopeBytes(respostaEnv));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SERVIDOR] Erro no serviço interno de relógio: {ex.Message}");
            }
        }
    }

    private void LoopEleicao()
    {
        using var sock = new ResponseSocket();
        sock.Bind($"tcp://*:{ElectionPort}");
        LogVerbose($"[SERVIDOR] Serviço de eleição interno ouvindo em tcp://*:{ElectionPort}");

        while (true)
        {
            try
            {
                var data = sock.ReceiveFrameBytes();
                var env = Mensageria.EnvelopeFromBytes(data);
                _relogio.OnReceive(env.Cabecalho.RelogioLogico);

                RegisterServerResponse resposta = env.ConteudoCase == Envelope.ConteudoOneofCase.RegisterServerReq
                    ? ProcessarPedidoEleicao(env.RegisterServerReq)
                    : new RegisterServerResponse
                    {
                        Status = Status.Erro,
                        ErroMsg = "tipo nao suportado"
                    };

                var cab = Mensageria.NovoCabecalho(_origem, _relogio);
                resposta.Cabecalho = cab.Clone();
                var respostaEnv = new Envelope
                {
                    Cabecalho = cab,
                    RegisterServerRes = resposta
                };
                sock.SendFrame(Mensageria.EnvelopeBytes(respostaEnv));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SERVIDOR] Erro no serviço interno de eleição: {ex.Message}");
            }
        }
    }

    private void LoopAnunciosCoordenador()
    {
        LogVerbose($"[SERVIDOR] Escutando tópicos internos {TopicoCoordenador} e {TopicoReplica}");

        while (true)
        {
            try
            {
                var topic = _subSocket.ReceiveFrameString();
                var payload = _subSocket.ReceiveFrameBytes();
                if (topic == TopicoReplica)
                {
                    var env = Mensageria.EnvelopeFromBytes(payload);
                    if (env.Cabecalho.LinguagemOrigem == ReplicaOrigem())
                    {
                        continue;
                    }
                    _relogio.OnReceive(env.Cabecalho.RelogioLogico);
                    AplicarOperacaoReplicada(env);
                    LogVerbose($"[SERVIDOR] Operação replicada recebida: {env.ConteudoCase}");
                    continue;
                }
                if (topic != TopicoCoordenador)
                {
                    continue;
                }

                var channelMsg = ChannelMessage.Parser.ParseFrom(payload);
                _relogio.OnReceive(channelMsg.RelogioLogico);

                var coordenador = channelMsg.Mensagem.Trim();
                if (string.IsNullOrWhiteSpace(coordenador))
                {
                    continue;
                }

                var remetente = channelMsg.Remetente?.Trim() ?? "";
                if (remetente == _serverName && coordenador == _serverName)
                {
                    continue;
                }

                if (string.IsNullOrWhiteSpace(remetente))
                {
                    remetente = "desconhecido";
                }

                ProcessarAnuncioCoordenador(coordenador, remetente);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SERVIDOR] Erro ao ler anúncios de coordenador: {ex.Message}");
                Thread.Sleep(200);
            }
        }
    }

    private List<ServerInfo> ServidoresAtivosSnapshot()
    {
        lock (_stateLock)
        {
            return _servidoresAtivos
                .Select(item => new ServerInfo { Nome = item.Key, Rank = item.Value })
                .OrderBy(item => item.Rank)
                .ToList();
        }
    }

    private string ResumoServidores(IEnumerable<ServerInfo> servidores)
    {
        var lista = servidores.ToList();
        if (lista.Count == 0)
        {
            return "(nenhum)";
        }

        return string.Join(", ", lista.Select(item => $"{item.Nome}(rank={item.Rank})"));
    }

    private void AtualizarServidoresAtivos(IEnumerable<ServerInfo>? servidores)
    {
        lock (_stateLock)
        {
            if (servidores != null)
            {
                foreach (var servidor in servidores)
                {
                    _ranksConhecidos[servidor.Nome] = servidor.Rank;
                }
            }

            if (servidores != null && servidores.Any())
            {
                _servidoresAtivos.Clear();
                foreach (var servidor in servidores)
                {
                    _servidoresAtivos[servidor.Nome] = servidor.Rank;
                }
            }
            else if (_servidoresAtivos.Count == 0 && _meuRank > 0)
            {
                _servidoresAtivos[_serverName] = _meuRank;
            }

            if (_meuRank > 0)
            {
                _servidoresAtivos[_serverName] = _meuRank;
                _ranksConhecidos[_serverName] = _meuRank;
            }
        }

        LogVerbose($"[SERVIDOR] Lista ativa local atualizada: {ResumoServidores(ServidoresAtivosSnapshot())}");
    }

    private void RestaurarServidorAtivoLocal(string nome)
    {
        int rank;
        lock (_stateLock)
        {
            if (!_servidoresAtivos.TryGetValue(nome, out rank)
                && !_ranksConhecidos.TryGetValue(nome, out rank))
            {
                LogVerbose($"[SERVIDOR] Coordenador {nome} respondeu ao teste direto, mas rank local e desconhecido");
                return;
            }
            _servidoresAtivos[nome] = rank;
        }

        LogVerbose($"[SERVIDOR] Coordenador {nome} respondeu ao teste direto; restaurado na lista ativa local rank={rank}");
    }

    private string MaiorServidorAtivo()
    {
        var servidores = ServidoresAtivosSnapshot();
        return servidores.Count == 0 ? _serverName : servidores[^1].Nome;
    }

    private string CoordenadorAtual()
    {
        lock (_stateLock)
        {
            return _coordenadorAtual;
        }
    }

    private string SetCoordenador(string nome)
    {
        string anterior;
        lock (_stateLock)
        {
            anterior = _coordenadorAtual;
            _coordenadorAtual = nome;
        }
        return anterior;
    }

    private void AvaliarEleicaoAposAtualizacao(string motivo)
    {
        var servidores = ServidoresAtivosSnapshot();
        if (servidores.Count == 0)
        {
            return;
        }

        var maior = servidores[^1];
        var coordenador = CoordenadorAtual();
        var rankCoordenador = string.IsNullOrWhiteSpace(coordenador) ? -1 : RankServidor(coordenador);
        if (string.IsNullOrWhiteSpace(coordenador) || rankCoordenador < 0 || maior.Rank > rankCoordenador)
        {
            IniciarEleicaoAsync(motivo);
        }
    }

    private bool IsCoordenador() => CoordenadorAtual() == _serverName;

    private void IniciarEleicaoAsync(string motivo)
    {
        _ = Task.Run(() => ExecutarEleicao(motivo));
    }

    private void ExecutarEleicao(string motivo)
    {
        lock (_stateLock)
        {
            if (_electionRunning)
            {
                LogVerbose($"[SERVIDOR] Eleição já em andamento; ignorando gatilho ({motivo})");
                return;
            }

            _electionRunning = true;
        }

        try
        {
            LogEleicao($"inicio motivo={motivo}");
            ListarServidoresReferencia();
            var servidores = ServidoresAtivosSnapshot();
            var superiores = servidores
                .Where(item => item.Rank > _meuRank && item.Nome != _serverName)
                .OrderByDescending(item => item.Rank)
                .ToList();

            if (superiores.Count == 0)
            {
                TornarCoordenador("sem_servidor_maior");
                return;
            }

            var recebeuOk = false;
            foreach (var item in superiores)
            {
                if (ConsultarEleicaoServidor(item.Nome, true))
                {
                    recebeuOk = true;
                }
            }

            if (!recebeuOk)
            {
                TornarCoordenador("sem_resposta_de_servidor_maior");
                return;
            }

            LogEleicao($"delegada motivo={motivo}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVIDOR] Erro durante eleição: {ex.Message}");
        }
        finally
        {
            lock (_stateLock)
            {
                _electionRunning = false;
            }
        }
    }

    private void TornarCoordenador(string motivo)
    {
        SetCoordenador(_serverName);
        LogEleicao($"eleito coordenador={_serverName} rank={_meuRank} motivo={motivo}");
        AnunciarCoordenador();
    }

    private void AnunciarCoordenador()
    {
        var cab = Mensageria.NovoCabecalho(_origem, _relogio);
        var channelMsg = new ChannelMessage
        {
            Canal = TopicoCoordenador,
            Mensagem = _serverName,
            Remetente = _serverName,
            TimestampEnvio = cab.TimestampEnvio,
            RelogioLogico = cab.RelogioLogico
        };

        try
        {
            _announceSocket.SendMoreFrame(TopicoCoordenador).SendFrame(channelMsg.ToByteArray());
            LogEleicao($"anuncio publicado coordenador={_serverName} rank={_meuRank}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVIDOR] Falha ao anunciar coordenador: {ex.Message}");
        }
    }

    private void ProcessarAnuncioCoordenador(string coordenador, string remetente)
    {
        ListarServidoresReferencia();
        var rankAnunciado = RankServidor(coordenador);
        if (coordenador == _serverName)
        {
            rankAnunciado = _meuRank;
        }

        if (rankAnunciado < 0)
        {
            LogEleicaoVerbose($"anuncio ignorado coordenador={coordenador} rank=desconhecido motivo=servidor_desconhecido");
            return;
        }

        var atual = CoordenadorAtual();
        var rankAtual = string.IsNullOrWhiteSpace(atual) ? -1 : RankServidor(atual);
        var aceitar = false;
        var motivo = "sem_coordenador";
        if (string.IsNullOrWhiteSpace(atual) || atual == coordenador)
        {
            aceitar = true;
            motivo = string.IsNullOrWhiteSpace(atual) ? "sem_coordenador" : "mesmo_coordenador";
        }
        else if (rankAtual < 0)
        {
            aceitar = true;
            motivo = "coordenador_atual_desconhecido";
        }
        else if (rankAnunciado >= rankAtual)
        {
            aceitar = true;
            motivo = "rank_maior_ou_igual";
        }
        else if (atual != _serverName && !ConsultarEleicaoServidor(atual, false))
        {
            aceitar = true;
            motivo = "coordenador_atual_indisponivel";
        }

        if (aceitar)
        {
            SetCoordenador(coordenador);
            LogEleicao($"anuncio aceito coordenador={coordenador} rank={rankAnunciado} origem={remetente} motivo={motivo}");
            return;
        }

        LogEleicaoVerbose($"anuncio ignorado coordenador={coordenador} rank={rankAnunciado} motivo=rank_menor_que_atual atual={atual}");
    }

    private static long TimestampToNs(global::Google.Protobuf.WellKnownTypes.Timestamp timestamp)
        => timestamp.Seconds * 1_000_000_000L + timestamp.Nanos;

    private PeerReply? EnviarParaServidor(string nomeServidor, int porta, Envelope env, int timeoutMs)
    {
        try
        {
            using var sock = new RequestSocket();
            sock.Options.Linger = TimeSpan.Zero;
            sock.Connect($"tcp://{nomeServidor}:{porta}");

            var envioNs = Mensageria.AgoraNs();
            var timeout = TimeSpan.FromMilliseconds(timeoutMs);
            if (!sock.TrySendFrame(timeout, Mensageria.EnvelopeBytes(env), false))
            {
                return null;
            }

            if (!sock.TryReceiveFrameBytes(timeout, out var data) || data is null)
            {
                return null;
            }

            var recebimentoNs = Mensageria.AgoraNs();
            var resp = Mensageria.EnvelopeFromBytes(data);
            _relogio.OnReceive(resp.Cabecalho.RelogioLogico);
            return new PeerReply(resp, envioNs, recebimentoNs);
        }
        catch (Exception ex)
        {
            LogVerbose($"[SERVIDOR] Falha ao comunicar com {nomeServidor}:{porta} - {ex.Message}");
            return null;
        }
    }

    private PeerReply? ConsultarRelogioServidor(string nomeServidor, int timeoutMs)
    {
        var cab = Mensageria.NovoCabecalho(_origem, _relogio);
        var req = new HeartbeatRequest
        {
            Cabecalho = cab.Clone(),
            NomeServidor = _serverName
        };
        var env = new Envelope
        {
            Cabecalho = cab,
            HeartbeatReq = req
        };

        LogVerbose($"[SERVIDOR] Enviando heartbeat_req para {nomeServidor} {Mensageria.CabecalhoTexto(cab)}");
        var reply = EnviarParaServidor(nomeServidor, ClockPort, env, timeoutMs);
        if (reply is null)
        {
            LogVerbose($"[SERVIDOR] Sem resposta de relógio de {nomeServidor}");
            return null;
        }

        if (reply.Envelope.ConteudoCase != Envelope.ConteudoOneofCase.HeartbeatRes)
        {
            LogVerbose($"[SERVIDOR] Resposta inesperada de relógio de {nomeServidor}: {reply.Envelope.ConteudoCase}");
            return null;
        }

        var resposta = reply.Envelope.HeartbeatRes;
        if (resposta.Status != Status.Sucesso)
        {
            LogVerbose($"[SERVIDOR] Servidor {nomeServidor} rejeitou pedido de relógio: {resposta.ErroMsg}");
            return null;
        }

        LogVerbose($"[SERVIDOR] Recebido heartbeat_res de {nomeServidor} {Mensageria.CabecalhoTexto(reply.Envelope.Cabecalho)}");
        return reply;
    }

    private bool ConsultarEleicaoServidor(string nomeServidor, bool registrarLogs)
    {
        var cab = Mensageria.NovoCabecalho(_origem, _relogio);
        var req = new RegisterServerRequest
        {
            Cabecalho = cab.Clone(),
            NomeServidor = _serverName
        };
        var env = new Envelope
        {
            Cabecalho = cab,
            RegisterServerReq = req
        };

        if (registrarLogs)
        {
            LogEleicaoVerbose($"req destino={nomeServidor}");
        }
        var reply = EnviarParaServidor(nomeServidor, ElectionPort, env, RequestTimeoutMs);
        if (reply is null)
        {
            if (registrarLogs)
            {
                LogEleicaoVerbose($"sem_resposta destino={nomeServidor}");
            }
            return false;
        }

        if (reply.Envelope.ConteudoCase != Envelope.ConteudoOneofCase.RegisterServerRes)
        {
            if (registrarLogs)
            {
                LogEleicaoVerbose($"resposta_invalida destino={nomeServidor}");
            }
            return false;
        }

        var resposta = reply.Envelope.RegisterServerRes;
        if (resposta.Status != Status.Sucesso)
        {
            if (registrarLogs)
            {
                LogEleicaoVerbose($"rejeitada destino={nomeServidor} motivo={resposta.ErroMsg}");
            }
            return false;
        }

        if (registrarLogs)
        {
            LogEleicaoVerbose($"ok origem={nomeServidor} rank={resposta.Rank}");
        }
        return true;
    }

    private bool SincronizarComoSeguidor(string coordenador, int timeoutMs)
    {
        if (string.IsNullOrWhiteSpace(coordenador))
        {
            return false;
        }

        var reply = ConsultarRelogioServidor(coordenador, timeoutMs);
        if (reply is null)
        {
            return false;
        }

        var resposta = reply.Envelope.HeartbeatRes;
        if (resposta.Status != Status.Sucesso)
        {
            return false;
        }

        _relogio.AtualizarOffset(reply.Envelope.Cabecalho.TimestampEnvio, reply.EnvioNs, reply.RecebimentoNs);
        LogVerbose($"[SERVIDOR] Offset físico atualizado para {_relogio.OffsetMillis()} ms usando coordenador {coordenador}");
        return true;
    }

    private void SincronizarComoCoordenador()
    {
        var servidores = ServidoresAtivosSnapshot();
        var amostras = new List<long>();
        var participantes = new List<string>();
        var falhas = new List<string>();

        amostras.Add(TimestampToNs(_relogio.NowCorrigidoTimestamp()));
        participantes.Add(_serverName);

        foreach (var servidor in servidores)
        {
            if (servidor.Nome == _serverName)
            {
                continue;
            }

            var reply = ConsultarRelogioServidor(servidor.Nome, RequestTimeoutMs);
            if (reply is null)
            {
                falhas.Add(servidor.Nome);
                continue;
            }

            var resposta = reply.Envelope.HeartbeatRes;
            if (resposta.Status != Status.Sucesso)
            {
                falhas.Add(servidor.Nome);
                continue;
            }

            amostras.Add(TimestampToNs(reply.Envelope.Cabecalho.TimestampEnvio));
            participantes.Add(servidor.Nome);
        }

        var mediaNs = amostras.Sum() / amostras.Count;
        var agoraNs = Mensageria.AgoraNs();
        _relogio.AtualizarOffset(Mensageria.TimestampFromNs(mediaNs), agoraNs, agoraNs);
        LogVerbose(
            $"[SERVIDOR] Berkeley aplicado pelo coordenador {_serverName} com participantes={string.Join(", ", participantes)} falhas={string.Join(", ", falhas)} media={Mensageria.TimestampTexto(Mensageria.TimestampFromNs(mediaNs))} offset={_relogio.OffsetMillis()} ms"
        );
    }

    private void SincronizarRelogioFisico()
    {
        var coordenador = CoordenadorAtual();
        if (string.IsNullOrWhiteSpace(coordenador))
        {
            IniciarEleicaoAsync("coordenador_desconhecido");
            return;
        }

        if (IsCoordenador())
        {
            SincronizarComoCoordenador();
            return;
        }

        var coordenadorAtivo = ServidoresAtivosSnapshot().Any(servidor => servidor.Nome == coordenador);
        if (!string.IsNullOrWhiteSpace(coordenador) && !coordenadorAtivo)
        {
            LogVerbose($"[SERVIDOR] Coordenador {coordenador} ausente da lista da referencia; testando diretamente com timeout={TimeoutTesteCoordenador}ms");
            if (SincronizarComoSeguidor(coordenador, TimeoutTesteCoordenador))
            {
                RestaurarServidorAtivoLocal(coordenador);
                return;
            }

            IniciarEleicaoAsync("coordenador_indisponivel_apos_teste");
            return;
        }

        if (!string.IsNullOrWhiteSpace(coordenador) && SincronizarComoSeguidor(coordenador, RequestTimeoutMs))
        {
            return;
        }

        IniciarEleicaoAsync($"falha_sincronizar_com_{coordenador}");
    }

    private HeartbeatResponse ProcessarPedidoRelogio(HeartbeatRequest req)
    {
        var resposta = new HeartbeatResponse();
        var solicitante = req.NomeServidor?.Trim();
        if (string.IsNullOrWhiteSpace(solicitante))
        {
            solicitante = "desconhecido";
        }

        var coordenador = CoordenadorAtual();
        if (IsCoordenador())
        {
            resposta.Status = Status.Sucesso;
            LogVerbose($"[SERVIDOR] Respondendo relógio ao servidor {solicitante} como coordenador");
            return resposta;
        }

        if (!string.IsNullOrWhiteSpace(coordenador) && solicitante == coordenador)
        {
            resposta.Status = Status.Sucesso;
            LogVerbose($"[SERVIDOR] Respondendo relógio ao coordenador {solicitante}");
            return resposta;
        }

        resposta.Status = Status.Erro;
        resposta.ErroMsg = "nao sou coordenador";
        LogVerbose($"[SERVIDOR] Pedido de relógio de {solicitante} rejeitado; coordenador atual={coordenador}");
        return resposta;
    }

    private RegisterServerResponse ProcessarPedidoEleicao(RegisterServerRequest req)
    {
        var resposta = new RegisterServerResponse();
        var solicitante = req.NomeServidor?.Trim();
        if (string.IsNullOrWhiteSpace(solicitante))
        {
            solicitante = "desconhecido";
        }

        var rankSolicitante = RankServidor(solicitante);
        if (_meuRank > rankSolicitante)
        {
            resposta.Status = Status.Sucesso;
            resposta.Rank = _meuRank;
            LogEleicaoVerbose($"ok enviado destino={solicitante} rank={_meuRank} solicitante_rank={rankSolicitante}");
            if (IsCoordenador())
            {
                AnunciarCoordenador();
            }
            else
            {
                IniciarEleicaoAsync($"pedido de eleição recebido de {solicitante}");
            }
            return resposta;
        }

        resposta.Status = Status.Erro;
        resposta.ErroMsg = "rank inferior";
        resposta.Rank = _meuRank;
        LogEleicaoVerbose($"pedido ignorado origem={solicitante} motivo=rank_inferior solicitante_rank={rankSolicitante} meu_rank={_meuRank}");
        return resposta;
    }

    private int RankServidor(string nome)
    {
        lock (_stateLock)
        {
            if (_servidoresAtivos.TryGetValue(nome, out var rank))
            {
                return rank;
            }
            return _ranksConhecidos.TryGetValue(nome, out rank) ? rank : -1;
        }
    }

    private sealed record PeerReply(Envelope Envelope, long EnvioNs, long RecebimentoNs);

    public void Loop()
    {
        LogVerbose("[SERVIDOR] Aguardando mensagens...");
        while (true)
        {
            var data = _socket.ReceiveFrameBytes();
            var env = Mensageria.EnvelopeFromBytes(data);
            _relogio.OnReceive(env.Cabecalho.RelogioLogico);
            
            LogVerbose($"[SERVIDOR] Recebido {env.ConteudoCase} {Mensageria.CabecalhoTexto(env.Cabecalho)}");

            object? resposta = env.ConteudoCase switch
            {
                Envelope.ConteudoOneofCase.LoginReq => ProcessarLogin(env.LoginReq),
                Envelope.ConteudoOneofCase.CreateChannelReq => ProcessarCreateChannel(env.CreateChannelReq),
                Envelope.ConteudoOneofCase.ListChannelsReq => ProcessarListChannels(),
                Envelope.ConteudoOneofCase.PublishReq => ProcessarPublish(env.PublishReq, env.Cabecalho),
                _ => null
            };

            if (resposta == null) continue;

            var cab = Mensageria.NovoCabecalho(_origem, _relogio);
            var respEnv = new Envelope { Cabecalho = cab };
            
            if (resposta is LoginResponse lr)
            {
                lr.Cabecalho = cab.Clone();
                respEnv.LoginRes = lr;
            }
            else if (resposta is CreateChannelResponse cr)
            {
                cr.Cabecalho = cab.Clone();
                respEnv.CreateChannelRes = cr;
            }
            else if (resposta is ListChannelsResponse lcr)
            {
                lcr.Cabecalho = cab.Clone();
                respEnv.ListChannelsRes = lcr;
            }
            else if (resposta is PublishResponse pr)
            {
                pr.Cabecalho = cab.Clone();
                respEnv.PublishRes = pr;
            }

            LogVerbose($"[SERVIDOR] Enviando {respEnv.ConteudoCase} {Mensageria.CabecalhoTexto(respEnv.Cabecalho)}");
            _socket.SendFrame(respEnv.ToByteArray());

            _requestsProcessadas += 1;
            if (_requestsProcessadas % 10 == 0)
            {
                try
                {
                    HeartbeatESincronizar();
                }
                catch (Exception ex)
                {
                    LogVerbose($"[SERVIDOR] Erro ao executar heartbeat/snapshot: {ex.Message}");
                }
            }
            if (_requestsProcessadas % 15 == 0)
            {
                try
                {
                    SincronizarRelogioFisico();
                }
                catch (Exception ex)
                {
                    LogVerbose($"[SERVIDOR] Erro ao sincronizar relógio físico: {ex.Message}");
                }
            }
        }
    }

    private LoginResponse ProcessarLogin(LoginRequest req)
    {
        var res = new LoginResponse();
        var nome = req.NomeUsuario?.Trim();

        if (string.IsNullOrEmpty(nome))
        {
            res.Status = Status.Erro;
            res.ErroMsg = "nome de usuário vazio";
            return res;
        }

        RegistrarLogin(nome, Mensageria.TimestampTexto(req.Cabecalho.TimestampEnvio));
        ReplicarLogin(req);
        
        res.Status = Status.Sucesso;
        return res;
    }

    private CreateChannelResponse ProcessarCreateChannel(CreateChannelRequest req)
    {
        var res = new CreateChannelResponse();
        var nome = req.NomeCanal?.Trim();

        if (string.IsNullOrEmpty(nome))
        {
            res.Status = Status.Erro;
            res.ErroMsg = "nome de canal vazio";
            return res;
        }

        if (!AdicionarCanalIdempotente(nome))
        {
            res.Status = Status.Erro;
            res.ErroMsg = "canal já existe";
            return res;
        }

        ReplicarCreateChannel(req);
        res.Status = Status.Sucesso;
        return res;
    }

    private ListChannelsResponse ProcessarListChannels()
    {
        var res = new ListChannelsResponse();
        res.Canais.AddRange(LerCanais());
        return res;
    }

    private PublishResponse ProcessarPublish(PublishRequest req, Cabecalho envCabecalho)
    {
        var res = new PublishResponse();
        var canal = req.Canal?.Trim() ?? "";
        var mensagem = req.Mensagem?.Trim() ?? "";

        if (string.IsNullOrEmpty(canal))
        {
            res.Status = Status.Erro;
            res.ErroMsg = "canal vazio";
            return res;
        }
        if (string.IsNullOrEmpty(mensagem))
        {
            res.Status = Status.Erro;
            res.ErroMsg = "mensagem vazia";
            return res;
        }

        var canais = LerCanais();
        if (!canais.Contains(canal))
        {
            res.Status = Status.Erro;
            res.ErroMsg = "canal inexistente";
            return res;
        }

        var remetente = string.IsNullOrWhiteSpace(envCabecalho.LinguagemOrigem) ? "desconhecido" : envCabecalho.LinguagemOrigem;
        var cabPub = Mensageria.NovoCabecalho(_origem, _relogio);
        var channelMsg = new ChannelMessage
        {
            Canal = canal,
            Mensagem = mensagem,
            Remetente = remetente,
            TimestampEnvio = cabPub.TimestampEnvio,
            RelogioLogico = cabPub.RelogioLogico
        };

        LogVerbose($"[SERVIDOR] Publicando em {canal} ts={Mensageria.TimestampTexto(channelMsg.TimestampEnvio)} relogio={channelMsg.RelogioLogico}");
        _pubSocket.SendMoreFrame(canal).SendFrame(channelMsg.ToByteArray());

        RegistrarPublicacao(canal, mensagem, remetente, Mensageria.TimestampTexto(channelMsg.TimestampEnvio));
        ReplicarPublicacao(canal, mensagem, remetente, cabPub);

        res.Status = Status.Sucesso;
        return res;
    }
}
