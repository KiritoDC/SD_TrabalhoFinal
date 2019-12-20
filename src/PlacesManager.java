import java.io.IOException;
import java.net.*;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLOutput;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.Thread.sleep;

public class PlacesManager extends UnicastRemoteObject implements PlacesListInterface,MonitoringInterface {

    private static final long serialVersionUID = 1L;
    HashMap <Integer, Timestamp> lista =new HashMap<Integer, Timestamp>(); //estrutura que guarda os portos ativos e o seu timestamp do último heartbeat
    ArrayList <Place> places; //Array que guarda os places adicionados
    HashMap <Integer,String> appendlog = new HashMap<Integer, String>(); //Estrutura que guarda os logs dos addPlaces
    static Thread t;
    static Thread p;
    static Thread y;
    final static String INET_ADDR = "224.0.0.3"; //endereço multicast
    final static int PORT = 7555;
    int myport; //porto do servidor
    int leader=-1; //porto do lider
    int leadertemp; //porto candidato
    int neleicao=-1; //numero da eleiçao
    int cidplace=0; //contador do numero de places dos logs
    boolean novo=true;
    boolean estado=true;

    public PlacesManager() throws RemoteException {
        super();
        this.places=new ArrayList <Place>();
    }




    public PlacesManager(int port) throws IOException, InterruptedException {
        super();
        myport=port;
        this.places=new ArrayList <Place>();
        lista.put(myport, new Timestamp(System.currentTimeMillis()));
        //criação da thread para receber e processar as mensagens multicast
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
        sleep(5000);
        //criação da thread para o envio de heartbeats e verificação de processos inativos
        p= (new Thread(){
            public void run() {
                try {
                    keepalive();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        });
        p.start();
        //criação da thread que simula a morte do porto lider 2028
        y= (new Thread(){
            public void run() {
                if(myport==2028) {
                    try {
                        Thread.sleep(20000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    estado=false;
                }
            }
        });
        y.start();
    }

    public void receiveUDPMessage(String ip, int port) throws
            IOException {
        //variaveis usadas para receber e processar as mensagens multicast
        byte[] buffer = new byte[1024];
        MulticastSocket socket = new MulticastSocket(port);
        InetAddress group = InetAddress.getByName(ip);
        socket.joinGroup(group);
        PlacesListInterface plm;
        HashMap <Integer,Integer> mapleaders=new HashMap<Integer, Integer>();
        while(estado){
            //receçao das mensagens multicast
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet);
            String msg = new String(packet.getData(), packet.getOffset(), packet.getLength());
            String[] parts=msg.split(" "); //divisao da mensagem por espaços
            switch (parts[0]) { //verificaçao do tipo de mensagem
                case "Info": //heartbeat
                    int porta = Integer.valueOf(parts[1]);
                    //adiçao do porto aos portos conhecidos caso não o conheça
                    if (lista.containsKey(porta)) {
                        lista.put(porta, new Timestamp(System.currentTimeMillis()));
                    } else {
                        lista.put(porta, new Timestamp(System.currentTimeMillis()));
                    }
                    if (novo) { //se for a primeira vez a processar um heartbeat
                        novo = false;
                        //processar as informaçoes do lider e da eleiçao
                        leader = Integer.parseInt(parts[2]);
                        neleicao = Integer.parseInt(parts[3]);
                        if(leader!=-1) { //se existir um lider na rede pede lhe a lista de places por rmi
                            String endereco = "rmi://localhost:" + leader + "/placelist";
                            try {
                                plm = (PlacesListInterface) Naming.lookup(endereco);
                                places = plm.allPlaces(); //adiçao aos logs
                                appendlog.put(places.size(),"allPlaces()");
                            } catch (NotBoundException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    break;
                case "leader": //mensagem de eleiçao
                    if (Integer.parseInt(parts[3]) == neleicao) { //se o numero de eleiçao da mensagem for correto processa a mesma; caso contrario ignora
                        if (mapleaders.containsKey(Integer.parseInt(parts[1]))) { //processamento dos votos
                            int c = mapleaders.get(Integer.parseInt(parts[1]));
                            c++;
                            mapleaders.put(Integer.parseInt(parts[1]), c);
                        } else {
                            mapleaders.put(Integer.parseInt(parts[1]), 1);
                        }
                        int tamanho = 0; //numero de votos total
                        Iterator it = mapleaders.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry pair = (Map.Entry) it.next();
                            tamanho += Integer.valueOf(pair.getValue().toString());
                        }
                        boolean lidereleito = false;
                        Iterator iterator = mapleaders.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry pair = (Map.Entry) iterator.next();
                            //verificaçao se existe algum porto eleito por maioria
                            if ((float) Integer.parseInt(pair.getValue().toString()) / lista.size() > 0.5) { //eleito por maioria
                                lidereleito = true;
                                leader = Integer.parseInt(pair.getKey().toString());
                                neleicao++;
                            }
                        }
                        if (lidereleito) {
                            mapleaders.clear();
                        } else if (tamanho >= lista.size()) { //caso não tenha havido um lider eleito por maioria é reiniciado o processo
                            electleader();
                            mapleaders.clear();
                        }
                    } else if (Integer.parseInt(parts[3]) > neleicao) { //caso haja uma nova eleiçao que este servidor nao conhecesse executa o processo de eleiçao
                        neleicao = Integer.parseInt(parts[3]);
                        electleader();
                        mapleaders.clear();
                    }
                    break;
                case "add": //mensagem para adicionar os places
                    if(myport!=leader) {
                        if (appendlog.containsKey(Integer.parseInt(parts[1]) - 1)) { //se houver places que não recebeu e que estão em falta pede-os primeiro e depois é que processa o que recebeu
                            Place place = new Place(parts[3], parts[2]);
                            if (!places.contains(place)) { //adiciona aos logs e à lista de places
                                addPlace(place);
                                cidplace=Integer.parseInt(parts[1]);
                                appendlog.put(Integer.parseInt(parts[1]),"addPlace( "+parts[3]+" , "+parts[2]+" )");
                            }
                        } else {
                            int i=1;
                        while(appendlog.containsKey(Integer.parseInt(parts[1])-i)&& i<=Integer.parseInt(parts[1])){ //pede os places em falta
                            //pede tds k faltam
                            sendgetplacemessage(Integer.parseInt(parts[1])-i);
                            i++;
                        } //por fim processa o place recebido
                            Place place = new Place(parts[3], parts[2]);
                            if (!places.contains(place)) {
                                addPlace(place);
                                cidplace=Integer.parseInt(parts[1]);
                                appendlog.put(Integer.parseInt(parts[1]),"addPlace("+parts[3]+","+parts[2]+")");
                            }
                        }
                    }
                    break;
                case "getplace": //o lider envia os places em falta pedidos por um follower
                    if(myport==leader){
                        String[] parts1=appendlog.get(parts[1]).split(" ");
                        sendplacemessage(parts1[1],parts1[3],parts[2]);
                    }
                    break;
                case "place": //o porto que pediu o place processa a mensagem com o place e adiciona-o à lista de places
                    if(Integer.parseInt(parts[3])==myport){
                        Place place = new Place(parts[1], parts[2]);
                        if (!places.contains(place)) {
                            addPlace(place);
                            appendlog.put(Integer.parseInt(parts[1]),"addPlace("+parts[1]+","+parts[2]+")");
                        }
                    }
                    break;
            }
        }
        socket.leaveGroup(group);
        socket.close();
    }

    public void keepalive() throws InterruptedException, ParseException {
        while(estado){ //envio de heartbeats e avaliação dos processos ativos de 5 em 5 segundos
            System.out.println("lista");
            boolean eliminado=false;
            Iterator it = lista.entrySet().iterator();
            while (it.hasNext()) { //verificação se algum porto esta inativo, se não enviou nenhum heartbeat para este servidor durante 2 iterações
                Map.Entry pair = (Map.Entry) it.next();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                if(new Timestamp(System.currentTimeMillis()).getTime()-dateFormat.parse(pair.getValue().toString()).getTime()>10000){
                    if(Integer.parseInt(pair.getKey().toString())==leader)
                        eliminado=true;
                    it.remove();
                }
                System.out.println(pair.getKey()+" "+myport);
            }

            if(eliminado || !lista.containsKey(leader)) { //se o lider foi o porto que ficou inativo é feito o processo de eleição
                System.out.println(myport+" "+leader+" "+eliminado);
                electleader();
                eliminado=false;
            }
            else{ //apresentação de algumas informações como o lider e a lista de places
                System.out.println("Current Leader = "+myport+" " + leader);
                for(int h=0;h<places.size();h++)
                    System.out.println("place "+myport+" "+places.get(h).getPostalCode()+" "+places.get(h).getLocality());
            }

            Iterator ab=appendlog.entrySet().iterator();
            System.out.println("logs "+myport); //apresentaçao dos logs
            while(ab.hasNext()){
                Map.Entry pair =(Map.Entry) ab.next();
                System.out.println(myport+" "+pair.getKey()+" "+pair.getValue());
            }
            try {
                findServers(myport); //envio do heartbeat
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            sleep(5000);
        }
    }

    public void findServers(int port) throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos (heartbeat)
        //esta mensagem tem na sua composiçao o seu porto, o lider e o numero da eleiçao
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "Info "+port+" "+leader+" "+neleicao;
            DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
            serverSocket.send(msgPacket);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void electleader() { //funçao de eleição do candidato
        if(lista.size()<1) { //auto-eleiçao caso não exista mais ninguem presente na rede
            leadertemp = myport;
            neleicao=1;
        }
        else
            leadertemp=lista.entrySet().iterator().next().getKey();
        Iterator it = lista.entrySet().iterator();
        while (it.hasNext()) { //eleiçao do candidato selecionando o porto com menor valor
            Map.Entry pair = (Map.Entry) it.next();
            if(leadertemp>=Integer.parseInt(pair.getKey().toString())){
                leadertemp=Integer.parseInt(pair.getKey().toString());
            }
        }
        try {
            sendleadermessage(); //envio da mensagem de eleiçao com o candidato escolhido
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        System.out.println("New Leader = "+myport+" "+leadertemp+" "+leader+ " "+neleicao);
    }

    public void sendleadermessage() throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a informar do seu voto para a eleiçao
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "leader ".concat(String.valueOf(leadertemp)).concat(" ").concat(String.valueOf(myport))+" "+neleicao;
            DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
            serverSocket.send(msgPacket);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendaddmessage(Place p) throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de a adicionarem o novo place criado
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "add "+appendlog.size()+" "+p.getLocality()+" "+p.getPostalCode();
            DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
            serverSocket.send(msgPacket);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendgetplacemessage(int id) throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a pedir ao lider que envie o place desejado
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "getplace "+id+" "+myport;
            DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
            serverSocket.send(msgPacket);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendplacemessage(String cd,String loc,String port) throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a enviar o place pedido pelo servidor
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "place "+cd+" "+loc+" "+port;
            DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
            serverSocket.send(msgPacket);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public PlacesManager(ArrayList<Place> places) throws RemoteException {
        super();
        this.places = places;
    }

    @Override
    public void addPlace(Place p) throws RemoteException { //função usada para adicionar places
        places.add(p); //adiçao do place à lista de places
        System.out.println("Place adicionado: "+p.getPostalCode()+" "+p.getLocality());
        if(leader==myport){//se for o lider envia a mensagem para os outros servidores com o place adicionado
            cidplace++;
            appendlog.put(cidplace,"addPlace( "+p.getPostalCode()+" , "+p.getLocality()+" )"); //regista a adiçao do place nos logs
            try {
                sendaddmessage(p); //envio da mensagem
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public ArrayList allPlaces() throws RemoteException { //funçao que devolve a lista de places
        return places;
    }

    @Override
    public Place getPlace(String id) throws RemoteException { //devolve um place pedido segundo o id/codigo postal
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
