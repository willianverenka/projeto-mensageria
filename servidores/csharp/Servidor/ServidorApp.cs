using System.Text.Json;
using Chat;
using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;
using Shared;

namespace Servidor;

public class ServidorApp
{
    private readonly string _orqEndpoint;
    private readonly string _proxyPubEndpoint;
    private readonly string _referenceEndpoint;
    private readonly string _serverName;
    private readonly string _origem;
    private readonly string _dataDir;
    private readonly string _loginsPath;
    private readonly string _canaisPath;
    private readonly string _publicacoesPath;
    private readonly DealerSocket _socket;
    private readonly PublisherSocket _pubSocket;
    private readonly RequestSocket _referenceSocket;
    private readonly RelogioProcesso _relogio = new();
    private int _requestsProcessadas = 0;

    public ServidorApp()
    {
        _orqEndpoint = Environment.GetEnvironmentVariable("ORQ_ENDPOINT_SERVIDOR") ?? "tcp://localhost:5556";
        _proxyPubEndpoint = Environment.GetEnvironmentVariable("PROXY_PUB_ENDPOINT") ?? "tcp://proxy:5557";
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
        _referenceSocket = new RequestSocket();
        _referenceSocket.Connect(_referenceEndpoint);
        Console.WriteLine($"[SERVIDOR] Conectado à referência em {_referenceEndpoint}");

        RegistrarNaReferencia();
        ListarServidoresReferencia();
    }

    private void InitStorage()
    {
        if (!File.Exists(_loginsPath)) File.WriteAllText(_loginsPath, "");
        if (!File.Exists(_canaisPath)) File.WriteAllText(_canaisPath, "[]");
        if (!File.Exists(_publicacoesPath)) File.WriteAllText(_publicacoesPath, "");
    }

    private Envelope EnviarParaReferencia(Envelope env, bool atualizarOffset = true)
    {
        Console.WriteLine($"[SERVIDOR] Enviando {env.ConteudoCase} para referência {Mensageria.CabecalhoTexto(env.Cabecalho)}");
        var envioNs = Mensageria.AgoraNs();
        _referenceSocket.SendFrame(env.ToByteArray());
        var data = _referenceSocket.ReceiveFrameBytes();
        var recebimentoNs = Mensageria.AgoraNs();
        var resp = Mensageria.EnvelopeFromBytes(data);
        _relogio.OnReceive(resp.Cabecalho.RelogioLogico);
        if (atualizarOffset)
        {
            _relogio.AtualizarOffset(resp.Cabecalho.TimestampEnvio, envioNs, recebimentoNs);
            Console.WriteLine($"[SERVIDOR] Offset físico atualizado para {_relogio.OffsetMillis()} ms");
        }
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

        Console.WriteLine($"[SERVIDOR] Servidor {_serverName} registrado na referência com rank={resp.RegisterServerRes.Rank}");
    }

    private void ListarServidoresReferencia()
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
            return;
        }

        var servidores = resp.ListServersRes.Servidores;
        var resumo = servidores.Count == 0
            ? "(nenhum)"
            : string.Join(", ", servidores.Select(item => $"{item.Nome}(rank={item.Rank})"));
        Console.WriteLine($"[SERVIDOR] Servidores disponíveis na referência: {resumo}");
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
        ListarServidoresReferencia();
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
