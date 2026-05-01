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
    private const int ClockPort = 5560;
    private const int ElectionPort = 5561;
    private const int RequestTimeoutMs = 2000;
    private const int AnnouncementTimeoutMs = 3000;
    private const int StartupDelayMs = 200;

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
    private readonly RelogioProcesso _relogio = new();
    private readonly Dictionary<string, int> _servidoresAtivos = new();
    private string _coordenadorAtual = "";
    private long _coordenadorVersao = 0;
    private int _meuRank = 0;
    private bool _electionRunning = false;
    private int _requestsProcessadas = 0;

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
        Console.WriteLine($"[SERVIDOR] Conectado ao orquestrador em {_orqEndpoint}");
        _pubSocket = new PublisherSocket();
        _pubSocket.Connect(_proxyPubEndpoint);
        Console.WriteLine($"[SERVIDOR] Conectado ao proxy pub em {_proxyPubEndpoint}");
        _announceSocket = new PublisherSocket();
        _announceSocket.Connect(_proxyPubEndpoint);
        _referenceSocket = new RequestSocket();
        _referenceSocket.Connect(_referenceEndpoint);
        Console.WriteLine($"[SERVIDOR] Conectado à referência em {_referenceEndpoint}");
        _subSocket = new SubscriberSocket();
        _subSocket.Connect(_proxySubEndpoint);
        _subSocket.Subscribe(TopicoCoordenador);
        Console.WriteLine($"[SERVIDOR] Conectado ao proxy sub em {_proxySubEndpoint}");

        RegistrarNaReferencia();
        var servidores = ListarServidoresReferencia();
        DefinirCoordenadorTentativo(servidores);
        IniciarServicosInternos();
        Thread.Sleep(StartupDelayMs);
        ExecutarEleicao("startup");
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
        Console.WriteLine($"[SERVIDOR] Enviando {env.ConteudoCase} para referência {Mensageria.CabecalhoTexto(env.Cabecalho)}");
        _referenceSocket.SendFrame(env.ToByteArray());
        var data = _referenceSocket.ReceiveFrameBytes();
        var resp = Mensageria.EnvelopeFromBytes(data);
        _relogio.OnReceive(resp.Cabecalho.RelogioLogico);
        Console.WriteLine($"[SERVIDOR] Recebido {resp.ConteudoCase} da referência {Mensageria.CabecalhoTexto(resp.Cabecalho)}");
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

        _meuRank = resp.RegisterServerRes.Rank;
        AtualizarServidoresAtivos([new ServerInfo { Nome = _serverName, Rank = _meuRank }]);
        Console.WriteLine($"[SERVIDOR] Servidor {_serverName} registrado na referência com rank={resp.RegisterServerRes.Rank}");
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
        Console.WriteLine($"[SERVIDOR] Servidores disponíveis na referência: {resumo}");
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
            Console.WriteLine($"[SERVIDOR] Resposta inesperada ao heartbeat: {resp.ConteudoCase}");
            return;
        }

        if (resp.HeartbeatRes.Status != Status.Sucesso)
        {
            Console.WriteLine($"[SERVIDOR] Heartbeat rejeitado: {resp.HeartbeatRes.ErroMsg}");
            return;
        }

        Console.WriteLine($"[SERVIDOR] Heartbeat enviado com sucesso para {_serverName}");
        var servidores = ListarServidoresReferencia();
        var coordenador = CoordenadorAtual();
        var encontrado = servidores.Any(servidor => servidor.Nome == coordenador);
        if (!string.IsNullOrWhiteSpace(coordenador) && !encontrado)
        {
            Console.WriteLine($"[SERVIDOR] Coordenador {coordenador} ausente da lista ativa; iniciando eleição");
            IniciarEleicaoAsync("coordenador ausente da lista ativa");
        }
    }

    private List<string> LerCanais()
    {
        try { return JsonSerializer.Deserialize<List<string>>(File.ReadAllText(_canaisPath)) ?? []; }
        catch { return []; }
    }

    private void SalvarCanais(List<string> canais) => File.WriteAllText(_canaisPath, JsonSerializer.Serialize(canais));

    private void RegistrarLogin(string usuario, string ts) 
    {
        var entry = JsonSerializer.Serialize(new { usuario, timestamp = ts });
        File.AppendAllText(_loginsPath, entry + Environment.NewLine);
    }

    private void RegistrarPublicacao(string canal, string mensagem, string remetente, string ts)
    {
        var entry = JsonSerializer.Serialize(new { canal, mensagem, remetente, timestamp = ts });
        File.AppendAllText(_publicacoesPath, entry + Environment.NewLine);
    }

    private void IniciarServicosInternos()
    {
        _ = Task.Run(LoopRelogio);
        _ = Task.Run(LoopEleicao);
        _ = Task.Run(LoopAnunciosCoordenador);
    }

    private void LoopRelogio()
    {
        using var sock = new ResponseSocket();
        sock.Bind($"tcp://*:{ClockPort}");
        Console.WriteLine($"[SERVIDOR] Serviço de relógio interno ouvindo em tcp://*:{ClockPort}");

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
        Console.WriteLine($"[SERVIDOR] Serviço de eleição interno ouvindo em tcp://*:{ElectionPort}");

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
        Console.WriteLine($"[SERVIDOR] Escutando anúncios de coordenador no tópico {TopicoCoordenador}");

        while (true)
        {
            try
            {
                var topic = _subSocket.ReceiveFrameString();
                var payload = _subSocket.ReceiveFrameBytes();
                var channelMsg = ChannelMessage.Parser.ParseFrom(payload);
                _relogio.OnReceive(channelMsg.RelogioLogico);

                var coordenador = channelMsg.Mensagem.Trim();
                if (string.IsNullOrWhiteSpace(coordenador))
                {
                    continue;
                }

                SetCoordenador(coordenador, $"anúncio publicado por {channelMsg.Remetente}", true);
                Console.WriteLine($"[SERVIDOR] Anúncio de coordenador recebido em {topic}: {coordenador}");
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
            }
        }

        Console.WriteLine($"[SERVIDOR] Lista ativa local atualizada: {ResumoServidores(ServidoresAtivosSnapshot())}");
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

    private long VersaoCoordenador()
    {
        lock (_stateLock)
        {
            return _coordenadorVersao;
        }
    }

    private void SetCoordenador(string nome, string motivo, bool incrementarVersao)
    {
        string anterior;
        long versao;
        lock (_stateLock)
        {
            anterior = _coordenadorAtual;
            _coordenadorAtual = nome;
            if (incrementarVersao)
            {
                _coordenadorVersao += 1;
            }

            versao = _coordenadorVersao;
        }

        if (anterior == nome)
        {
            Console.WriteLine($"[SERVIDOR] Coordenador mantido em {nome} ({motivo}, versao={versao})");
        }
        else
        {
            Console.WriteLine($"[SERVIDOR] Coordenador atualizado de {anterior} para {nome} ({motivo}, versao={versao})");
        }
    }

    private void DefinirCoordenadorTentativo(List<ServerInfo> servidores)
    {
        if (!string.IsNullOrWhiteSpace(CoordenadorAtual()))
        {
            return;
        }

        var candidato = MaiorServidorAtivo();
        SetCoordenador(candidato, "coordenador tentativo inicial", false);
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
                Console.WriteLine($"[SERVIDOR] Eleição já em andamento; ignorando gatilho ({motivo})");
                return;
            }

            _electionRunning = true;
        }

        try
        {
            Console.WriteLine($"[SERVIDOR] Iniciando eleição ({motivo})");
            var versaoInicial = VersaoCoordenador();
            var servidores = ServidoresAtivosSnapshot();
            var superiores = servidores
                .Where(item => item.Rank > _meuRank && item.Nome != _serverName)
                .OrderByDescending(item => item.Rank)
                .ToList();

            if (superiores.Count == 0)
            {
                if (VersaoCoordenador() > versaoInicial)
                {
                    Console.WriteLine("[SERVIDOR] Eleição concluída durante a coleta de respostas");
                    return;
                }

                TornarCoordenador("nenhum servidor de maior rank respondeu");
                return;
            }

            var respostasOk = new List<ServerInfo>();
            foreach (var item in superiores)
            {
                if (ConsultarEleicaoServidor(item.Nome))
                {
                    respostasOk.Add(item);
                }
            }

            if (VersaoCoordenador() > versaoInicial)
            {
                Console.WriteLine("[SERVIDOR] Eleição concluída por anúncio recebido durante a coleta");
                return;
            }

            if (respostasOk.Count == 0)
            {
                TornarCoordenador("nenhum servidor de maior rank respondeu");
                return;
            }

            var deadline = DateTime.UtcNow.AddMilliseconds(AnnouncementTimeoutMs);
            while (DateTime.UtcNow < deadline)
            {
                if (VersaoCoordenador() > versaoInicial)
                {
                    Console.WriteLine("[SERVIDOR] Eleição concluída por anúncio de coordenador");
                    return;
                }

                Thread.Sleep(100);
            }

            var candidato = respostasOk.OrderByDescending(item => item.Rank).First();
            SetCoordenador(candidato.Nome, $"coordenador inferido após timeout de anúncio ({motivo})", true);
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
        SetCoordenador(_serverName, motivo, true);
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
            Console.WriteLine($"[SERVIDOR] Anunciado coordenador em servers: {_serverName}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SERVIDOR] Falha ao anunciar coordenador: {ex.Message}");
        }
    }

    private static long TimestampToNs(global::Google.Protobuf.WellKnownTypes.Timestamp timestamp)
        => timestamp.Seconds * 1_000_000_000L + timestamp.Nanos;

    private PeerReply? EnviarParaServidor(string nomeServidor, int porta, Envelope env, int timeoutMs)
    {
        using var sock = new RequestSocket();
        sock.Options.Linger = TimeSpan.Zero;
        sock.Connect($"tcp://{nomeServidor}:{porta}");

        try
        {
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
            Console.WriteLine($"[SERVIDOR] Falha ao comunicar com {nomeServidor}:{porta} - {ex.Message}");
            return null;
        }
    }

    private PeerReply? ConsultarRelogioServidor(string nomeServidor)
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

        Console.WriteLine($"[SERVIDOR] Enviando heartbeat_req para {nomeServidor} {Mensageria.CabecalhoTexto(cab)}");
        var reply = EnviarParaServidor(nomeServidor, ClockPort, env, RequestTimeoutMs);
        if (reply is null)
        {
            Console.WriteLine($"[SERVIDOR] Sem resposta de relógio de {nomeServidor}");
            return null;
        }

        if (reply.Envelope.ConteudoCase != Envelope.ConteudoOneofCase.HeartbeatRes)
        {
            Console.WriteLine($"[SERVIDOR] Resposta inesperada de relógio de {nomeServidor}: {reply.Envelope.ConteudoCase}");
            return null;
        }

        var resposta = reply.Envelope.HeartbeatRes;
        if (resposta.Status != Status.Sucesso)
        {
            Console.WriteLine($"[SERVIDOR] Servidor {nomeServidor} rejeitou pedido de relógio: {resposta.ErroMsg}");
            return null;
        }

        Console.WriteLine($"[SERVIDOR] Recebido heartbeat_res de {nomeServidor} {Mensageria.CabecalhoTexto(reply.Envelope.Cabecalho)}");
        return reply;
    }

    private bool ConsultarEleicaoServidor(string nomeServidor)
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

        Console.WriteLine($"[SERVIDOR] Enviando pedido de eleição para {nomeServidor} {Mensageria.CabecalhoTexto(cab)}");
        var reply = EnviarParaServidor(nomeServidor, ElectionPort, env, RequestTimeoutMs);
        if (reply is null)
        {
            Console.WriteLine($"[SERVIDOR] Sem resposta de eleição de {nomeServidor}");
            return false;
        }

        if (reply.Envelope.ConteudoCase != Envelope.ConteudoOneofCase.RegisterServerRes)
        {
            Console.WriteLine($"[SERVIDOR] Resposta inesperada de eleição de {nomeServidor}: {reply.Envelope.ConteudoCase}");
            return false;
        }

        var resposta = reply.Envelope.RegisterServerRes;
        if (resposta.Status != Status.Sucesso)
        {
            Console.WriteLine($"[SERVIDOR] Servidor {nomeServidor} rejeitou eleição: {resposta.ErroMsg}");
            return false;
        }

        Console.WriteLine($"[SERVIDOR] Servidor {nomeServidor} respondeu OK à eleição (rank={resposta.Rank})");
        return true;
    }

    private bool SincronizarComoSeguidor(string coordenador)
    {
        if (string.IsNullOrWhiteSpace(coordenador))
        {
            return false;
        }

        var reply = ConsultarRelogioServidor(coordenador);
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
        Console.WriteLine($"[SERVIDOR] Offset físico atualizado para {_relogio.OffsetMillis()} ms usando coordenador {coordenador}");
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

            var reply = ConsultarRelogioServidor(servidor.Nome);
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
        Console.WriteLine(
            $"[SERVIDOR] Berkeley aplicado pelo coordenador {_serverName} com participantes={string.Join(", ", participantes)} falhas={string.Join(", ", falhas)} media={Mensageria.TimestampTexto(Mensageria.TimestampFromNs(mediaNs))} offset={_relogio.OffsetMillis()} ms"
        );
    }

    private void SincronizarRelogioFisico()
    {
        var coordenador = CoordenadorAtual();
        if (string.IsNullOrWhiteSpace(coordenador))
        {
            Console.WriteLine("[SERVIDOR] Coordenador desconhecido; iniciando eleição");
            ExecutarEleicao("coordenador desconhecido");
            coordenador = CoordenadorAtual();
        }

        if (IsCoordenador())
        {
            SincronizarComoCoordenador();
            return;
        }

        var coordenadorAtivo = ServidoresAtivosSnapshot().Any(servidor => servidor.Nome == coordenador);
        if (!string.IsNullOrWhiteSpace(coordenador) && !coordenadorAtivo)
        {
            Console.WriteLine($"[SERVIDOR] Coordenador {coordenador} ausente da lista ativa; iniciando eleição");
            ExecutarEleicao("coordenador ausente da lista ativa");
            coordenador = CoordenadorAtual();
            if (IsCoordenador())
            {
                SincronizarComoCoordenador();
                return;
            }
        }

        if (!string.IsNullOrWhiteSpace(coordenador) && SincronizarComoSeguidor(coordenador))
        {
            return;
        }

        Console.WriteLine($"[SERVIDOR] Falha ao sincronizar com coordenador {coordenador}; iniciando eleição");
        ExecutarEleicao("falha ao sincronizar com coordenador");
        coordenador = CoordenadorAtual();
        if (IsCoordenador())
        {
            SincronizarComoCoordenador();
            return;
        }

        if (!string.IsNullOrWhiteSpace(coordenador) && !SincronizarComoSeguidor(coordenador))
        {
            Console.WriteLine($"[SERVIDOR] Ainda não foi possível sincronizar com {coordenador} após a eleição");
        }
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
            Console.WriteLine($"[SERVIDOR] Respondendo relógio ao servidor {solicitante} como coordenador");
            return resposta;
        }

        if (!string.IsNullOrWhiteSpace(coordenador) && solicitante == coordenador)
        {
            resposta.Status = Status.Sucesso;
            Console.WriteLine($"[SERVIDOR] Respondendo relógio ao coordenador {solicitante}");
            return resposta;
        }

        resposta.Status = Status.Erro;
        resposta.ErroMsg = "nao sou coordenador";
        Console.WriteLine($"[SERVIDOR] Pedido de relógio de {solicitante} rejeitado; coordenador atual={coordenador}");
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
            Console.WriteLine($"[SERVIDOR] Respondendo eleição OK para {solicitante} (solicitante rank={rankSolicitante}, meu rank={_meuRank})");
            if (!IsCoordenador())
            {
                IniciarEleicaoAsync($"pedido de eleição recebido de {solicitante}");
            }
            return resposta;
        }

        resposta.Status = Status.Erro;
        resposta.ErroMsg = "rank inferior";
        resposta.Rank = _meuRank;
        Console.WriteLine($"[SERVIDOR] Pedido de eleição de {solicitante} rejeitado (solicitante rank={rankSolicitante}, meu rank={_meuRank})");
        return resposta;
    }

    private int RankServidor(string nome)
    {
        lock (_stateLock)
        {
            return _servidoresAtivos.TryGetValue(nome, out var rank) ? rank : -1;
        }
    }

    private sealed record PeerReply(Envelope Envelope, long EnvioNs, long RecebimentoNs);

    public void Loop()
    {
        Console.WriteLine("[SERVIDOR] Aguardando mensagens...");
        while (true)
        {
            var data = _socket.ReceiveFrameBytes();
            var env = Mensageria.EnvelopeFromBytes(data);
            _relogio.OnReceive(env.Cabecalho.RelogioLogico);
            
            Console.WriteLine($"[SERVIDOR] Recebido {env.ConteudoCase} {Mensageria.CabecalhoTexto(env.Cabecalho)}");

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

            Console.WriteLine($"[SERVIDOR] Enviando {respEnv.ConteudoCase} {Mensageria.CabecalhoTexto(respEnv.Cabecalho)}");
            _socket.SendFrame(respEnv.ToByteArray());

            _requestsProcessadas += 1;
            if (_requestsProcessadas % 10 == 0)
            {
                HeartbeatESincronizar();
            }
            if (_requestsProcessadas % 15 == 0)
            {
                SincronizarRelogioFisico();
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

        var canais = LerCanais();
        if (canais.Contains(nome))
        {
            res.Status = Status.Erro;
            res.ErroMsg = "canal já existe";
            return res;
        }

        canais.Add(nome);
        SalvarCanais(canais);
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

        Console.WriteLine($"[SERVIDOR] Publicando em {canal} ts={Mensageria.TimestampTexto(channelMsg.TimestampEnvio)} relogio={channelMsg.RelogioLogico}");
        _pubSocket.SendMoreFrame(canal).SendFrame(channelMsg.ToByteArray());

        RegistrarPublicacao(canal, mensagem, remetente, Mensageria.TimestampTexto(channelMsg.TimestampEnvio));

        res.Status = Status.Sucesso;
        return res;
    }
}
