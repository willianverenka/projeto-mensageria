// See https://aka.ms/new-console-template for more information

using Chat;
using NetMQ;
using NetMQ.Sockets;
using Shared;

namespace Cliente;

public class ClienteApp
{
    private readonly string _orqEndpoint;
    private readonly string _nomeUsuario;
    private readonly string _nomeCanal;
    private readonly DealerSocket _socket;

    public ClienteApp()
    {
        _orqEndpoint = Environment.GetEnvironmentVariable("ORQ_ENDPOINT") ?? "tcp://orquestrador:5555";
        _nomeUsuario = Environment.GetEnvironmentVariable("CLIENTE_NOME") ?? "cliente_csharp";
        _nomeCanal = Environment.GetEnvironmentVariable("CLIENTE_CANAL") ?? "canal_csharp";

        _socket = new DealerSocket();
        _socket.Connect(_orqEndpoint);
        Console.WriteLine($"[CLIENTE] Conectado ao orquestrador em {_orqEndpoint}");
    }

    private Envelope EnviarEAguardar(Envelope env)
    {
        _socket.SendFrame(Mensageria.EnvelopeBytes(env));
        var data = _socket.ReceiveFrameBytes();
        return Mensageria.EnvelopeFromBytes(data);
    }

    public bool FazerLogin(string nomeUsuario)
    {
        var cab = Mensageria.NovoCabecalho(Mensageria.OrigemLabel("cliente"));
        var env = new Envelope
        {
            Cabecalho = cab,
            LoginReq = new LoginRequest
            {
                Cabecalho = cab.Clone(),
                NomeUsuario = nomeUsuario
            }
        };

        Console.WriteLine($"[CLIENTE] Enviando LoginRequest para usuário '{nomeUsuario}'");
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
        var cab = Mensageria.NovoCabecalho(Mensageria.OrigemLabel("cliente"));
        var env = new Envelope
        {
            Cabecalho = cab,
            CreateChannelReq = new CreateChannelRequest
            {
                Cabecalho = cab.Clone(),
                NomeCanal = nomeCanal
            }
        };

        Console.WriteLine($"[CLIENTE] Solicitando criação do canal '{nomeCanal}'");
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
        var cab = Mensageria.NovoCabecalho(Mensageria.OrigemLabel("cliente"));
        var env = new Envelope
        {
            Cabecalho = cab,
            ListChannelsReq = new ListChannelsRequest
            {
                Cabecalho = cab.Clone()
            }
        };

        Console.WriteLine("[CLIENTE] Solicitando listagem de canais");
        var resp = EnviarEAguardar(env);

        if (resp.ConteudoCase != Envelope.ConteudoOneofCase.ListChannelsRes)
        {
            Console.WriteLine($"[CLIENTE] Resposta inesperada a listagem de canais: {resp.ConteudoCase}");
            return new List<string>();
        }

        var canais = resp.ListChannelsRes.Canais.ToList();
        var lista = canais.Any() ? string.Join(", ", canais) : "(nenhum)";
        Console.WriteLine($"[CLIENTE] Canais existentes: {lista}");
        return canais;
    }

    public void Run()
    {
        for (int i = 0; i < 3; i++)
        {
            if (FazerLogin(_nomeUsuario)) break;
            Thread.Sleep(1000);
        }

        CriarCanal(_nomeCanal);

        for (int i = 0; i < 100; i++)
        {
            ListarCanais();
            Thread.Sleep(500);
        }
    }
}