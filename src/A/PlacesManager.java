package A;
import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.*;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

import static java.lang.Thread.sleep;

public class PlacesManager extends UnicastRemoteObject implements PlacesListInterface,MonitoringInterface {

    private static final long serialVersionUID = 1L;
    ArrayList <Integer> ListaManager = new ArrayList<Integer>();
    ArrayList <Integer> ListaManagertemp = new ArrayList<Integer>();
    ArrayList <Place> places;
    static Thread t;
    static Thread p;
    static Thread y;
    final static String INET_ADDR = "224.0.0.3";
    final static int PORT = 7555;
    int myport;
    int leader;
    int leadertemp;
    static int portoServerLider;
    boolean estado=true;

    public PlacesManager() throws RemoteException {
        super();
        //this.places=null;
        this.places=new ArrayList <Place>();
    }




    public PlacesManager(int port) throws IOException, InterruptedException {
        super();
        myport=port;
        this.places=new ArrayList <Place>();
        PlacesListInterface placesListInterface=null;
        String addr=null;
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
        if(myport==2026) {
            sleep(10000);
            estado=false;
        }
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
        HashMap <Integer,Integer> mapleaders=new HashMap<Integer, Integer>();
        while(estado){
            //System.out.println("Waiting for multicast message...");
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet);
            String msg = new String(packet.getData(), packet.getOffset(), packet.getLength());
            //System.out.println("[Multicast UDP message received]>> "+String.valueOf(myport)+" "+msg);
            String[] parts=msg.split(" ");
            if(parts[0].equals("Info")) {
                int porta = Integer.valueOf(parts[1]);
            /*System.out.println("list manager rece antes");
            for(int i = 0; i < ListaManager.size(); i++){
                System.out.println(ListaManager.get(i)+" "+myport);
            }
            System.out.println("lista temp rec antes");
            for(int i = 0; i < ListaManagertemp.size(); i++){
                System.out.println(ListaManagertemp.get(i)+" "+myport);
            }*/
                if (!ListaManagertemp.contains(porta)) {
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
            }
            else
                if(parts[0].equals("leader")){
                    int a=0;
                    if(mapleaders.containsKey(Integer.parseInt(parts[1]))){
                        int c=mapleaders.get(Integer.parseInt(parts[1]));
                        c++;
                        mapleaders.put(Integer.parseInt(parts[1]),c);
                    }
                    else{
                        mapleaders.put(Integer.parseInt(parts[1]),1);
                    }
                    int tamanho=0;
                    Iterator it=mapleaders.entrySet().iterator();
                    while(it.hasNext()){
                        Map.Entry pair=(Map.Entry)it.next();
                        tamanho+=Integer.valueOf(pair.getValue().toString());
                    }
                    if(tamanho==ListaManagertemp.size()){
                        Iterator ite=mapleaders.entrySet().iterator();
                        while(ite.hasNext()){
                            Map.Entry pair=(Map.Entry)ite.next();
                            if((float)Integer.valueOf(pair.getValue().toString())/tamanho * 100 >= 50 ){
                                leader=Integer.parseInt(pair.getKey().toString());
                                leaderdefinitivo(leader);
                                a=1;
                                break;
                            }
                        }
                        if(a==0){
                            leader=-1;
                        }
                        a=0;
                        mapleaders.clear();
                    }
                }
                else if(parts[0].equals("defleader")) {
                    leader=Integer.parseInt(parts[1]);
                    System.out.println(myport+"Lider definitivo " + leader);
                }
            if("OK".equals(msg)) {
                System.out.println("No more message. Exiting : "+msg);
                break;
            }
        }
        socket.leaveGroup(group);
        socket.close();
    }

    public void keepalive() throws InterruptedException {
        while(estado){
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
            if(!ListaManager.equals(ListaManagertemp) || leader==-1) {
                electleader();
                ListaManager.clear();
                for(int i=0;i<ListaManagertemp.size();i++){
                    ListaManager.add(ListaManagertemp.get(i));
                }
            }
            else {
                System.out.println("Current Leader = "+myport+" " + leader);
            }
            ListaManagertemp.clear();
            ListaManagertemp.add(myport);

            try {
                findServers(myport);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            sleep(5000);
        }
    }

    public void leaderdefinitivo(int port) throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "defleader ".concat(String.valueOf(port));
            DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
            serverSocket.send(msgPacket);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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

    public void electleader() {
        leadertemp=ListaManagertemp.get(0);
        for(int i=0;i<ListaManagertemp.size();i++){
            if(leadertemp>=ListaManagertemp.get(i))
                leadertemp=ListaManagertemp.get(i);
        }
        try {
            sendleadermessage();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        System.out.println("New Leader = "+myport+" "+leadertemp);
    }

    public void sendleadermessage() throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "leader ".concat(String.valueOf(leadertemp)).concat(" ").concat(String.valueOf(myport));
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
