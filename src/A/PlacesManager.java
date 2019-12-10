package A;

import java.io.IOException;
import java.net.*;
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
    static Thread t;
    static Thread p;
    static Thread y;
    final static String INET_ADDR = "224.0.0.3";
    final static int PORT = 7555;
    int myport;
    int leader=-1;
    int leadertemp;
    int neleicao=-1;
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
        if(myport==2028)
            places.add(new Place("3545","ugrhgr"));
        PlacesListInterface placesListInterface=null;
        lista.put(myport, new Timestamp(System.currentTimeMillis()));
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
        //sleep(5000);
        sendnewmessage();
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
        HashMap <Integer,Integer> mapleaders=new HashMap<Integer, Integer>();
        while(estado){
            //System.out.println("Waiting for multicast message...");
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
                        //rejeitar em caso de ser diferente-> manda mensagem com o seu lider-> ao receber tenta pingar o lider recebido se conseguir fica com ele como lider senao vai dormir
                        int maioria = Math.round(lista.size() / 2);
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
                    Place place = new Place(parts[2], parts[1]);
                    if (!places.contains(place) && leader != myport) {
                        places.add(place);
                    }
                    break;
                case "new":
                    sendreplyleader();
                    break;
                case "newreply":
                    if (novo) {
                        novo = false;
                        //processar mensagem do leader
                        leader = Integer.parseInt(parts[1]);
                        neleicao = Integer.parseInt(parts[2]);
                        sendgetplacesmessage();
                    }
                    break;
                case "getplace":
                    if (leader == myport) {
                        sendplacesmessage();
                    }
                    break;
                case "place":
                    int c = 0;
                    int i = 1;
                    if (myport != leader) {
                        while (c == 0) {
                            if (parts[i].equals("++")) {
                                c = 1;
                            } else {
                                //System.out.println(myport + " " + parts[i]);
                                String[] parts1 = parts[i].split("/");
                                Place p = new Place(parts1[0], parts1[1]);
                                boolean ab=false;
                                for(int h=0;h<places.size();h++){
                                    if(places.get(h).getPostalCode().equals(parts1[0]) && places.get(h).getLocality().equals(parts1[1]))
                                        ab=true;
                                }
                               // if (!places.contains(p)) {
                                if(!ab){
                                    ab=false;
                                    places.add(p);
                                    //System.out.println("-"+p.getPostalCode() + "- -" + p.getLocality()+"-");
                                }
                            }
                            i++;
                        }
                    /*for(int h=0;h<places.size();h++)
                        System.out.println("place "+myport+" "+places.get(h).getPostalCode()+" "+places.get(h).getLocality());*/
                        break;
                    }
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

    public void sendgetplacesmessage() throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "getplace "+myport;
            DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
            serverSocket.send(msgPacket);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendplacesmessage() throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "place ";
            for(int i=0;i<places.size();i++){
                msg+=places.get(i).getPostalCode()+"/"+places.get(i).getLocality()+" ";
            }
            msg+="++";
            DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
            serverSocket.send(msgPacket);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendnewmessage() throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
        /*if(lista.isEmpty()){
            leader=myport;
        }
        else {*/
            InetAddress addr = InetAddress.getByName(INET_ADDR);
            try (DatagramSocket serverSocket = new DatagramSocket()) {
                String msg = "new ".concat(String.valueOf(myport));
                DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
                serverSocket.send(msgPacket);
            } catch (SocketException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
      //  }
    }

    public void sendaddmessage(Place p) throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "add ".concat(p.getLocality()).concat(" ").concat(p.getPostalCode());
            DatagramPacket msgPacket = new DatagramPacket(msg.getBytes(), msg.getBytes().length, addr, PORT);
            serverSocket.send(msgPacket);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendreplyleader() throws UnknownHostException {
        //Envio de uma mensagem multicast aos outros servidores de modo a verificar quais os portos ainda ativos
        InetAddress addr = InetAddress.getByName(INET_ADDR);
        try (DatagramSocket serverSocket = new DatagramSocket()){
            String msg = "newreply "+leader+" "+neleicao;
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
