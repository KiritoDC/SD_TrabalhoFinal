import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.MulticastSocket;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

public class PlacesManager extends UnicastRemoteObject implements PlacesListInterface,MonitoringInterface {
    private static final long serialVersionUID = 1L;
    ArrayList <Place> places;
    static Thread t;
    final static String INET_ADDR = "224.0.0.3";
    final static int PORT = 7555;

    public PlacesManager() throws RemoteException {
        super();
        //this.places=null;
        this.places=new ArrayList <Place>();
    }

    public void receiveUDPMessage(String ip, int port) throws
            IOException {
        byte[] buffer=new byte[1024];
        MulticastSocket socket=new MulticastSocket(port);
        InetAddress group=InetAddress.getByName(ip);
        socket.joinGroup(group);
        while(true){
            System.out.println("Waiting for multicast message...");
            DatagramPacket packet=new DatagramPacket(buffer,
                    buffer.length);
            socket.receive(packet);
            String msg=new String(packet.getData(),
                    packet.getOffset(),packet.getLength());
            System.out.println("[Multicast UDP message received]>> "+msg);
            if("OK".equals(msg)) {
                System.out.println("No more message. Exiting : "+msg);
                break;
            }
        }
        socket.leaveGroup(group);
        socket.close();
    }


    public PlacesManager(int port) throws IOException {
        super();
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
        /*try {
            replicasManagementInterface=(ReplicasManagementInterface)Naming.lookup("rmi://localhost:2024/replicamanager");
            addr=replicasManagementInterface.addReplica("rmi://localhost:"+port+"/placelist");
        } catch (NotBoundException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        if(addr!=null) {
            try {
                placesListInterface = (PlacesListInterface) Naming.lookup(addr);
            } catch (NotBoundException e) {
                e.printStackTrace();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
            places = placesListInterface.allPlaces();
        }*/
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
