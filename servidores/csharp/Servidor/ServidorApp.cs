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
    private readonly string _dataDir;
    private readonly string _loginsPath;
    private readonly string _canaisPath;
    private readonly string _publicacoesPath;
    private readonly DealerSocket _socket;
    private readonly PublisherSocket _pubSocket;

    public ServidorApp()
    {
        _orqEndpoint = Environment.GetEnvironmentVariable("ORQ_ENDPOINT_SERVIDOR") ?? "tcp://localhost:5556";
        _proxyPubEndpoint = Environment.GetEnvironmentVariable("PROXY_PUB_ENDPOINT") ?? "tcp://proxy:5557";
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

        //RegistrarNoOrquestrador();
    }

    private void InitStorage()
    {
        if (!File.Exists(_loginsPath)) File.WriteAllText(_loginsPath, "");
        if (!File.Exists(_canaisPath)) File.WriteAllText(_canaisPath, "[]");
        if (!File.Exists(_publicacoesPath)) File.WriteAllText(_publicacoesPath, "");
    }

    private void RegistrarNoOrquestrador()
    {
        var env = new Envelope { Cabecalho = Mensageria.NovoCabecalho(Mensageria.OrigemLabel("servidor")) };
        var req = new ListChannelsRequest { Cabecalho = env.Cabecalho.Clone() };
        env.ListChannelsReq = req;

        try
        {
            // Envia sem bloquear (equivalente ao NOBLOCK do zmq)
            _socket.TrySendFrame(TimeSpan.Zero, env.ToByteArray());
        }
        catch
        {
            Console.WriteLine("[AVISO] Não foi possível enviar registro inicial.");
        }
    }

    private List<string> LerCanais()
    {
        try { return JsonSerializer.Deserialize<List<string>>(File.ReadAllText(_canaisPath)) ?? new(); }
        catch { return new(); }
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
            
            Console.WriteLine($"[SERVIDOR] Servidor csharp processando: {env.ConteudoCase}");

            object? resposta = env.ConteudoCase switch
            {
                Envelope.ConteudoOneofCase.LoginReq => ProcessarLogin(env.LoginReq),
                Envelope.ConteudoOneofCase.CreateChannelReq => ProcessarCreateChannel(env.CreateChannelReq),
                Envelope.ConteudoOneofCase.ListChannelsReq => ProcessarListChannels(env.ListChannelsReq),
                Envelope.ConteudoOneofCase.PublishReq => ProcessarPublish(env.PublishReq, env.Cabecalho),
                _ => null
            };

            if (resposta == null) continue;

            var respEnv = new Envelope { Cabecalho = Mensageria.NovoCabecalho(Mensageria.OrigemLabel("servidor")) };
            
            if (resposta is LoginResponse lr) respEnv.LoginRes = lr;
            else if (resposta is CreateChannelResponse cr) respEnv.CreateChannelRes = cr;
            else if (resposta is ListChannelsResponse lcr) respEnv.ListChannelsRes = lcr;
            else if (resposta is PublishResponse pr) respEnv.PublishRes = pr;

            _socket.SendFrame(respEnv.ToByteArray());
        }
    }

    private LoginResponse ProcessarLogin(LoginRequest req)
    {
        var res = new LoginResponse { Cabecalho = req.Cabecalho.Clone() };
        var nome = req.NomeUsuario?.Trim();

        if (string.IsNullOrEmpty(nome))
        {
            res.Status = Status.Erro;
            res.ErroMsg = "nome de usuário vazio";
            return res;
        }

        var ts = req.Cabecalho.TimestampEnvio;
        RegistrarLogin(nome, $"{ts.Seconds}.{ts.Nanos:D9}");
        
        res.Status = Status.Sucesso;
        return res;
    }

    private CreateChannelResponse ProcessarCreateChannel(CreateChannelRequest req)
    {
        var res = new CreateChannelResponse { Cabecalho = req.Cabecalho.Clone() };
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

    private ListChannelsResponse ProcessarListChannels(ListChannelsRequest req)
    {
        var res = new ListChannelsResponse { Cabecalho = req.Cabecalho.Clone() };
        res.Canais.AddRange(LerCanais());
        return res;
    }

    private PublishResponse ProcessarPublish(PublishRequest req, Cabecalho envCabecalho)
    {
        var res = new PublishResponse { Cabecalho = req.Cabecalho.Clone() };
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
        var channelMsg = new ChannelMessage
        {
            Canal = canal,
            Mensagem = mensagem,
            Remetente = remetente,
            TimestampEnvio = req.Cabecalho.TimestampEnvio
        };
        _pubSocket.SendMoreFrame(canal).SendFrame(channelMsg.ToByteArray());

        var ts = req.Cabecalho.TimestampEnvio;
        var tsIso = $"{ts.Seconds}.{ts.Nanos:D9}";
        RegistrarPublicacao(canal, mensagem, remetente, tsIso);

        res.Status = Status.Sucesso;
        return res;
    }
}