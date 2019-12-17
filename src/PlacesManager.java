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
    HashMap <Integer, Timestamp> lista =new HashMap<Integer, Timestamp>();
    ArrayList <Place> places;
    HashMap <Integer,String> appendlog = new HashMap<Integer, String>();
    static Thread t;
    static Thread p;
    static Thread y;
    final static String INET_ADDR = "224.0.0.3";
    final static int PORT = 7555;
    int myport;
    int leader=-1;
    int leadertemp;
    int neleicao=-1;
    int cidplace=0;
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
        /*if(myport==2028)
            places.add(new Place("3545","ugrhgr"));*/
        lista.put(myport, new Timestamp(System.currentTimeMillis()));
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
        //sleep(5000);
        sleep(5000);
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
        y= (new Thread(){
            public void run() {
                if(myport==2028) {
                    try {
                        /*Thread.sleep(20000);
                        if(myport==2028) {
                            try {
                                addPlace(new Place("3545","ugrhgr"));
                            } catch (RemoteException e) {
                                e.printStackTrace();
                            }
                        }*/
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
        /*
        Multicasting is based on a group membership concept, where a multicast address represents each group.
        In IPv4, any address between 224.0.0.0 to 239.255.255.255 can be used as a multicast address. Only those nodes that subscribe to a group receive packets communicated to the group.
        */
        byte[] buffer = new byte[1024];
        MulticastSocket socket = new MulticastSocket(port);
        InetAddress group = InetAddress.getByName(ip);
        socket.joinGroup(group);
        PlacesListInterface plm;
        HashMap <Integer,Integer> mapleaders=new HashMap<Integer, Integer>();
        while(estado){
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet);
            String msg = new String(packet.getData(), packet.getOffset(), packet.getLength());
            //System.out.println("[Multicast UDP message received]>> "+String.valueOf(myport)+" "+msg);
            String[] parts=msg.split(" ");
            switch (parts[0]) {
                case "Info":
                    int porta = Integer.valueOf(parts[1]);
                    if (lista.containsKey(porta)) {
                        lista.put(porta, new Timestamp(System.currentTimeMillis()));
                    } else {
                        lista.put(porta, new Timestamp(System.currentTimeMillis()));
                    }
                    if (novo) {
                        novo = false;
                        //processar mensagem do leader
                        leader = Integer.parseInt(parts[2]);
                        neleicao = Integer.parseInt(parts[3]);
                        if(leader!=-1) {
                            String endereco = "rmi://localhost:" + leader + "/placelist";
                            try {
                                plm = (PlacesListInterface) Naming.lookup(endereco);
                                places = plm.allPlaces();
                                appendlog.put(places.size(),"allPlaces()");
                            } catch (NotBoundException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    break;
                case "leader":
                    if (Integer.parseInt(parts[3]) == neleicao) {
                        int a = 0;
                        if (mapleaders.containsKey(Integer.parseInt(parts[1]))) {
                            int c = mapleaders.get(Integer.parseInt(parts[1]));
                            c++;
                            mapleaders.put(Integer.parseInt(parts[1]), c);
                        } else {
                            mapleaders.put(Integer.parseInt(parts[1]), 1);
                        }
                        int tamanho = 0;
                        Iterator it = mapleaders.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry pair = (Map.Entry) it.next();
                            tamanho += Integer.valueOf(pair.getValue().toString());
                        }
                        boolean lidereleito = false;
                        Iterator iterator = mapleaders.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry pair = (Map.Entry) iterator.next();
                            if ((float) Integer.parseInt(pair.getValue().toString()) / lista.size() > 0.5) {
                                lidereleito = true;
                                leader = Integer.parseInt(pair.getKey().toString());
                                neleicao++;
                            }
                        }
                        if (lidereleito) {
                            mapleaders.clear();
                        } else if (tamanho >= lista.size()) {
                            //leader = -1;
                            electleader();
                            mapleaders.clear();
                        }
                    } else if (Integer.parseInt(parts[3]) > neleicao) {
                        neleicao = Integer.parseInt(parts[3]);
                        electleader();
                        mapleaders.clear();
                    }
                    break;
                case "add":
                    if(myport!=leader) {
                        if (appendlog.containsKey(Integer.parseInt(parts[1]) - 1)) {
                            Place place = new Place(parts[3], parts[2]);
                            if (!places.contains(place)) {
                                addPlace(place);
                                appendlog.put(Integer.parseInt(parts[1]),"addPlace("+parts[3]+","+parts[2]+")");
                            }
                        } else {
                            int i=1;
                        while(appendlog.containsKey(Integer.parseInt(parts[1])-i)&& i<=Integer.parseInt(parts[1])){
                            //pede tds k faltam
                            sendgetplacemessage(Integer.parseInt(parts[1])-i);
                            i++;
                        }
                            Place place = new Place(parts[3], parts[2]);
                            if (!places.contains(place)) {
                                addPlace(place);
                                appendlog.put(Integer.parseInt(parts[1]),"addPlace("+parts[3]+","+parts[2]+")");
                            }
                            //optamos por pedir tds por rmi
                            //???????????????????????????
                            /*String endereco = "rmi://localhost:" + leader + "/placelist";
                            try {
                                plm = (PlacesListInterface) Naming.lookup(endereco);
                                places = plm.allPlaces();
                            } catch (NotBoundException e) {
                                e.printStackTrace();
                            }
                            appendlog.put(places.size(), "allPlaces()");*/
                        }
                    }
                    break;
                case "getplace":
                    if(myport==leader){
                        String[] parts1=appendlog.get(parts[1]).split(" ");
                        sendplacemessage(parts1[1],parts1[3],parts[2]);
                    }
                    break;
                case "place":
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
        while(estado){
            System.out.println("lista");
            boolean eliminado=false;
            Iterator it = lista.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                if(new Timestamp(System.currentTimeMillis()).getTime()-dateFormat.parse(pair.getValue().toString()).getTime()>10000){
                    if(Integer.parseInt(pair.getKey().toString())==leader)
                        eliminado=true;
                    it.remove();
                }
                System.out.println(pair.getKey()+" "+myport);
            }

            if(eliminado || !lista.containsKey(leader)) {
                System.out.println(myport+" "+leader+" "+eliminado);
                electleader();
                eliminado=false;
            }
            else{
                System.out.println("Current Leader = "+myport+" " + leader);
                for(int h=0;h<places.size();h++)
                    System.out.println("place "+myport+" "+places.get(h).getPostalCode()+" "+places.get(h).getLocality());
            }

            Iterator ab=appendlog.entrySet().iterator();
            System.out.println("logs "+myport);
            while(ab.hasNext()){
                Map.Entry pair =(Map.Entry) ab.next();
                System.out.println(myport+" "+pair.getKey()+" "+pair.getValue());
            }
            try {
                findServers(myport);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            sleep(5000);
        }
    }

    public void findServers(int port) throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
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

    public void electleader() {
        if(lista.size()<1) {
            leadertemp = myport;
            neleicao=1;
        }
        else
            leadertemp=lista.entrySet().iterator().next().getKey();
        //neleicao++;
        Iterator it = lista.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            if(leadertemp>=Integer.parseInt(pair.getKey().toString())){
                leadertemp=Integer.parseInt(pair.getKey().toString());
            }
        }
        try {
            sendleadermessage();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        System.out.println("New Leader = "+myport+" "+leadertemp+" "+leader+ " "+neleicao);
    }

    public void sendleadermessage() throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
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
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
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
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
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
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
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
    public void addPlace(Place p) throws RemoteException {
        places.add(p);
        System.out.println("Place adicionado: "+p.getPostalCode()+" "+p.getLocality());
        if(leader==myport){
            cidplace++;
            appendlog.put(cidplace,"addPlace( "+p.getPostalCode()+" , "+p.getLocality()+" )");
            try {
                sendaddmessage(p);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
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
