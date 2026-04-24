using Chat;
using NetMQ;
using NetMQ.Sockets;
using Shared;

namespace Cliente;

public class ClienteApp
{
    private readonly string _orqEndpoint;
    private readonly string _proxySubEndpoint;
    private readonly string _nomeUsuario;
    private readonly string _nomeCanal;
    private readonly string _origem;
    private readonly DealerSocket _socket;
    private readonly SubscriberSocket _subSocket;
    private readonly HashSet<string> _inscricoes = [];
    private readonly Random _random = new();
    private readonly RelogioProcesso _relogio = new();

    public ClienteApp()
    {
        _orqEndpoint = Environment.GetEnvironmentVariable("ORQ_ENDPOINT") ?? "tcp://orquestrador:5555";
        _proxySubEndpoint = Environment.GetEnvironmentVariable("PROXY_SUB_ENDPOINT") ?? "tcp://proxy:5558";
        _nomeUsuario = Environment.GetEnvironmentVariable("CLIENTE_NOME") ?? "cliente_csharp";
        _nomeCanal = Environment.GetEnvironmentVariable("CLIENTE_CANAL") ?? "canal_csharp";
        _origem = Mensageria.OrigemLabel("cliente");

        _socket = new DealerSocket();
        _socket.Connect(_orqEndpoint);
        _subSocket = new SubscriberSocket();
        _subSocket.Connect(_proxySubEndpoint);
        Console.WriteLine($"[CLIENTE] Conectado ao orquestrador em {_orqEndpoint}");
        Console.WriteLine($"[CLIENTE] Conectado ao proxy sub em {_proxySubEndpoint}");
    }

    private Envelope EnviarEAguardar(Envelope env)
    {
        Console.WriteLine($"[CLIENTE] Enviando {env.ConteudoCase} {Mensageria.CabecalhoTexto(env.Cabecalho)}");
        _socket.SendFrame(Mensageria.EnvelopeBytes(env));
        var data = _socket.ReceiveFrameBytes();
        var resp = Mensageria.EnvelopeFromBytes(data);
        _relogio.OnReceive(resp.Cabecalho.RelogioLogico);
        Console.WriteLine($"[CLIENTE] Recebido {resp.ConteudoCase} {Mensageria.CabecalhoTexto(resp.Cabecalho)}");
        return resp;
    }

    public bool FazerLogin(string nomeUsuario)
    {
        var cab = Mensageria.NovoCabecalho(_origem, _relogio);
        var env = new Envelope
        {
            Cabecalho = cab,
            LoginReq = new LoginRequest
            {
                Cabecalho = cab.Clone(),
                NomeUsuario = nomeUsuario
            }
        };

        var resp = EnviarEAguardar(env);

        if (resp.ConteudoCase != Envelope.ConteudoOneofCase.LoginRes)
        {
            Console.WriteLine($"[CLIENTE] Resposta inesperada ao login: {resp.ConteudoCase}");
            return false;
        }

        if (resp.LoginRes.Status == Status.Sucesso)
        {
            Console.WriteLine($"[CLIENTE] Login bem-sucedido para '{nomeUsuario}'");
            return true;
        }

        Console.WriteLine($"[CLIENTE] Falha no login: {resp.LoginRes.ErroMsg}");
        return false;
    }

    public void CriarCanal(string nomeCanal)
    {
        var cab = Mensageria.NovoCabecalho(_origem, _relogio);
        var env = new Envelope
        {
            Cabecalho = cab,
            CreateChannelReq = new CreateChannelRequest
            {
                Cabecalho = cab.Clone(),
                NomeCanal = nomeCanal
            }
        };

        var resp = EnviarEAguardar(env);

        if (resp.ConteudoCase != Envelope.ConteudoOneofCase.CreateChannelRes)
        {
            Console.WriteLine($"[CLIENTE] Resposta inesperada a criação de canal: {resp.ConteudoCase}");
            return;
        }

        if (resp.CreateChannelRes.Status == Status.Sucesso)
            Console.WriteLine($"[CLIENTE] Canal '{nomeCanal}' criado com sucesso");
        else
            Console.WriteLine($"[CLIENTE] Falha ao criar canal '{nomeCanal}': {resp.CreateChannelRes.ErroMsg}");
    }

    public List<string> ListarCanais()
    {
        var cab = Mensageria.NovoCabecalho(_origem, _relogio);
        var env = new Envelope
        {
            Cabecalho = cab,
            ListChannelsReq = new ListChannelsRequest
            {
                Cabecalho = cab.Clone()
            }
        };

        var resp = EnviarEAguardar(env);

        if (resp.ConteudoCase != Envelope.ConteudoOneofCase.ListChannelsRes)
        {
            Console.WriteLine($"[CLIENTE] Resposta inesperada a listagem de canais: {resp.ConteudoCase}");
            return [];
        }

        var canais = resp.ListChannelsRes.Canais.ToList();
        var lista = canais.Any() ? string.Join(", ", canais) : "(nenhum)";
        Console.WriteLine($"[CLIENTE] Canais existentes: {lista}");
        return canais;
    }

    public bool Publicar(string canal, string mensagem)
    {
        var cab = Mensageria.NovoCabecalho(_origem, _relogio);
        var env = new Envelope
        {
            Cabecalho = cab,
            PublishReq = new PublishRequest
            {
                Cabecalho = cab.Clone(),
                Canal = canal,
                Mensagem = mensagem
            }
        };
        var resp = EnviarEAguardar(env);
        if (resp.ConteudoCase != Envelope.ConteudoOneofCase.PublishRes)
            return false;
        return resp.PublishRes.Status == Status.Sucesso;
    }

    public void Inscrever(string canal)
    {
        if (_inscricoes.Contains(canal))
            return;
        _subSocket.Subscribe(canal);
        _inscricoes.Add(canal);
        Console.WriteLine($"[CLIENTE] Inscrito no canal '{canal}'");
    }

    public void IniciarReceptor()
    {
        _ = Task.Run(() =>
        {
            while (true)
            {
                try
                {
                    var topic = _subSocket.ReceiveFrameString();
                    var payload = _subSocket.ReceiveFrameBytes();
                    var msg = ChannelMessage.Parser.ParseFrom(payload);
                    _relogio.OnReceive(msg.RelogioLogico);
                    Console.WriteLine(
                        $"[CLIENTE] [CANAL={topic}] msg='{msg.Mensagem}' remetente={msg.Remetente} envio={Mensageria.TimestampTexto(msg.TimestampEnvio)} recebimento={Mensageria.TimestampTexto(Mensageria.TimestampFromNs(Mensageria.AgoraNs()))} relogio={msg.RelogioLogico}"
                    );
                }
                catch
                {
                    Thread.Sleep(200);
                }
            }
        });
    }

    public void Run()
    {
        for (int i = 0; i < 3; i++)
        {
            if (FazerLogin(_nomeUsuario)) break;
            Thread.Sleep(1000);
        }

        IniciarReceptor();
        var canais = ListarCanais();
        if (canais.Count < 5)
        {
            CriarCanal($"{_nomeCanal}_{_random.Next(10_000)}");
            canais = ListarCanais();
        }
        while (_inscricoes.Count < 3 && _inscricoes.Count < canais.Count)
        {
            var canal = canais[_random.Next(canais.Count)];
            Inscrever(canal);
        }

        while (true)
        {
            canais = ListarCanais();
            if (canais.Count == 0)
            {
                Thread.Sleep(1000);
                continue;
            }
            var canal = canais[_random.Next(canais.Count)];
            for (int i = 0; i < 10; i++)
            {
                Publicar(canal, $"msg_{_random.Next(1_000_000)}");
                Thread.Sleep(1000);
            }
        }
    }
}
