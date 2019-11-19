import java.io.IOException;
import java.net.*;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import static java.lang.Thread.sleep;

public class PlacesManager extends UnicastRemoteObject implements PlacesListInterface,MonitoringInterface {

    //Lista de inteiros que armazena os portos dos servidores , vao ser armazenados os valores da lista ListManagertemp caso que ambas as listas sejam
    //diferentes uma da outra
    ArrayList <Integer> ListaManager = new ArrayList<Integer>();

    //Lista de inteiros que armazena os portos dos servidores , vao ser armazenados os valores obtidos pela função ReceiveUDPMessage
    ArrayList <Integer> ListaManagertemp = new ArrayList<Integer>();

    //Lista de armazenamento de objectos Place
    ArrayList <Place> places;

    static Thread t;
    static Thread p;

    //IP e Porto comum para a partilha de mensagens multicasting
    final static String INET_ADDR = "224.0.0.3";
    final static int PORT = 7555;

    //Armazena o porto do servidor
    int myport;

    //Armazena o porto do Leader
    int leader;

    //Construtor do servidor
    public PlacesManager() throws RemoteException {
        super();
        this.places=new ArrayList <Place>();
    }

    //Construtor do servidor dado um porto
    public PlacesManager(int port) throws IOException, InterruptedException {
        super();
        myport=port;

        //Inicialização da lista de Place
        this.places=new ArrayList <Place>();

        String addr=null;

        //Nova Thread com objectivo de receber as mensagens de multicast
        t= (new Thread(){
            public void run() {
                try {
                    receiveUDPMessage(INET_ADDR, PORT);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        });
        t.start();
        sleep(1000);

        //Nova Thread com objectivo de atualizar as listas de portos e caso seja necessario , eleição de um novo leader
        p= (new Thread(){
            public void run() {
                try {
                    keepalive();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        p.start();
    }


    /*
        Multicast é baseado em grupos com membros , onde cada endereço multicast representa um grupo
        Em IPv4 , qualquer endereço entre 224.0.0.0 to 239.255.255.255 pode ser usado para o serviço de multicast
        Apenas os nodos que se registam como membros do grupo recevem os pacotes comunicados no grupo.
    */
    private void receiveUDPMessage(String ip, int port) throws IOException {

        byte[] buffer = new byte[1024];

        //Criação do grupo multicasting e a junção do servidor ao grupo
        MulticastSocket socket = new MulticastSocket(port);
        InetAddress group = InetAddress.getByName(ip);
        socket.joinGroup(group);

        //Loop de modo a proceder com a leitura da mensagem multicast
        while(true){
            //System.out.println("Waiting for multicast message...");

            //Receção do pacote DatagramPacket e a sua conversão para String
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet);
            String msg = new String(packet.getData(), packet.getOffset(), packet.getLength());
            //System.out.println("[Multicast UDP message received]>> "+String.valueOf(myport)+" "+msg);

            /*
            * Mensagem é colocada num String Array onde é dividia por espaço
            * Mensagem é composta por 2 partes
            * 1º Contem o tipo de mensagem (Info ou Reply)
            * 2º Contem o porto de origem da mensagem
            */
            String[] parts=msg.split(" ");
            int porta=Integer.valueOf(parts[1]);


            /*System.out.println("list manager rece antes");
            for(int i = 0; i < ListaManager.size(); i++){
                System.out.println(ListaManager.get(i)+" "+myport);
            }
            System.out.println("lista temp rec antes");
            for(int i = 0; i < ListaManagertemp.size(); i++){
                System.out.println(ListaManagertemp.get(i)+" "+myport);
            }*/

            //Insere os portos na lista ListaManagertemp caso nao existam
            if(!ListaManagertemp.contains(porta)) {
                ListaManagertemp.add(porta);
            }


            /*System.out.println("list manager rece");
            for(int i = 0; i < ListaManager.size(); i++){
                System.out.println(ListaManager.get(i)+" "+myport);
            }
            System.out.println("lista temp rec");
            for(int i = 0; i < ListaManagertemp.size(); i++){
                System.out.println(ListaManagertemp.get(i)+" "+myport);
            }*/

            /*
            Verifica se a mensagem é do tipo Info ou tipo Reply , se for do tipo Info vai entao enviar um Reply
            para o grupo de modo a que estes adicionem assim o porto deste servidor na sua lista
             */
            if(parts[0].equals("Info"))
                replyServers(myport);

            /*
            Caso seja recevida a mensagem "Ok" os servidores terminam a sua execução
            Nao tem aplicação pratica de momento mas pode ser usado como um shutdown de emergencia
             */
            if("OK".equals(msg)) {
                System.out.println("No more message. Exiting : "+msg);
                break;
            }
        }

        //Saida do grupo
        socket.leaveGroup(group);
        socket.close();
    }

    /*
    * Função com o objectivo de atualização das listas de portos de cada servidor
    * Vao ser organizadas as listas de modo crescente
    * A lista ListaManager vai armazenar uma iteração anterior da lista ListaManagertemp
    * Caso existam diferenças nestas , a lista ListaManager vai ser igual à ListaManagertemp e vai ser eleito um novo leader
    * Caso sejam iguais , significa que nao foi criado ou apagado um novo servidor
    */
    public void keepalive() throws InterruptedException {

        while(true){

            //Ordenação das listas
            Collections.sort(ListaManager);
            Collections.sort(ListaManagertemp);

            System.out.println("list manager");
            for(int i = 0; i < ListaManager.size(); i++){
                System.out.println(ListaManager.get(i)+" "+myport);
            }

            System.out.println("lista temp");
            for(int i = 0; i < ListaManagertemp.size(); i++){
                System.out.println(ListaManagertemp.get(i)+" "+myport);
            }

            //Se as listas forem diferentes , executada uma eleiçao de um novo leader , ListaManager é limpa e é copiada para la a ListaManagertemp
            if(!ListaManager.equals(ListaManagertemp)) {
                electleader();
                ListaManager.clear();
                for(int i=0;i<ListaManagertemp.size();i++){
                    ListaManager.add(ListaManagertemp.get(i));
                }
                //System.arraycopy(ListaManagertemp,0,ListaManager,0,ListaManagertemp.size());
                //ListaManager = ListaManagertemp;
            }
            else {
                System.out.println("Current Leader = "+myport+" " + leader);
            }
            ListaManagertemp.clear();

            try {
                findServers(myport);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            sleep(20000);
        }
    }


    public void findServers(int port) throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "Info ".concat(String.valueOf(port));
            DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
            serverSocket.send(msgPacket);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void replyServers(int port) throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "Reply ".concat(String.valueOf(port));
            DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
            serverSocket.send(msgPacket);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void electleader() {
        leader=ListaManagertemp.get(0);
        for(int i=0;i<ListaManagertemp.size();i++){
            if(leader>=ListaManagertemp.get(i))
                leader=ListaManagertemp.get(i);
        }
        System.out.println("New Leader = "+myport+" "+leader);
    }

    public PlacesManager(ArrayList<Place> places) throws RemoteException {
        super();
        this.places = places;
    }

    @Override
    public void addPlace(Place p) throws RemoteException {
        places.add(p);
    }

    @Override
    public ArrayList allPlaces() throws RemoteException {
        return places;
    }

    @Override
    public Place getPlace(String id) throws RemoteException {
        int i=0;
        while(i<places.size()){
            if(places.get(i).getPostalCode().equals(id))
                return places.get(i);
            i++;
        }
        return null;
    }

    @Override
    public void ping() throws RemoteException {

    }
}
