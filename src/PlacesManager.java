import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.*;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.Thread.sleep;

public class PlacesManager extends UnicastRemoteObject implements PlacesListInterface,MonitoringInterface {

    private static final long serialVersionUID = 1L;
    ArrayList <Integer> ListaManager = new ArrayList<Integer>();
    ArrayList <Integer> ListaManagertemp = new ArrayList<Integer>();
    ArrayList <Place> places;
    static Thread t;
    static Thread p;
    final static String INET_ADDR = "224.0.0.3";
    final static int PORT = 7555;
    int myport;
    int leader;
    static int portoServerLider;

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
        while(true){
            //System.out.println("Waiting for multicast message...");
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet);
            String msg = new String(packet.getData(), packet.getOffset(), packet.getLength());
            //System.out.println("[Multicast UDP message received]>> "+String.valueOf(myport)+" "+msg);
            String[] parts=msg.split(" ");
            int porta=Integer.valueOf(parts[1]);
            if(!ListaManagertemp.contains(porta)) {
                ListaManagertemp.add(porta);
            }
            //System.out.println("Portos conhecidos:"+String.valueOf(myport) );
            /*for(int i = 0; i < ListaManager.size(); i++){
                System.out.println(ListaManager.get(i));
            }*/
            if(parts[0].equals("Info"))
                replyServers(myport);

            if("OK".equals(msg)) {
                System.out.println("No more message. Exiting : "+msg);
                break;
            }
        }
        socket.leaveGroup(group);
        socket.close();
    }

    public void keepalive() throws InterruptedException {
        while(true){
            Collections.sort(ListaManager);
            Collections.sort(ListaManagertemp);
            System.out.println("list manager");
            for(int i = 0; i < ListaManager.size(); i++){
                System.out.println(ListaManager.get(i));
            }
            System.out.println("lista temp");
            for(int i = 0; i < ListaManagertemp.size(); i++){
                System.out.println(ListaManagertemp.get(i));
            }
            if(!ListaManager.equals(ListaManagertemp)) {
                electleader();
                ListaManager = ListaManagertemp;
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
            sleep(30000);
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
