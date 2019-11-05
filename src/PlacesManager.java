import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

public class PlacesManager extends UnicastRemoteObject implements PlacesListInterface,MonitoringInterface {
    private static final long serialVersionUID = 1L;
    ArrayList <Place> places;

    public PlacesManager() throws RemoteException {
        super();
        //this.places=null;
        this.places=new ArrayList <Place>();
    }

    public PlacesManager(int port) throws RemoteException{
        super();
        this.places=new ArrayList <Place>();
        ReplicasManagementInterface replicasManagementInterface=null;
        PlacesListInterface placesListInterface=null;
        String addr=null;
        try {
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
