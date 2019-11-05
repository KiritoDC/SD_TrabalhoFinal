import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

public class PlaceManager extends UnicastRemoteObject implements PlacesListInterface {

    protected PlaceManager() throws  RemoteException{
        super();
    }

    private static ArrayList<Place> ListPlace = new ArrayList<Place>();

    @Override
    public ArrayList allPlaces() throws RemoteException {
        return ListPlace;
    }

    @Override
    public Place getPlace(String id) throws RemoteException {

        int i = 0;
        while(i < ListPlace.size()) {
            if (ListPlace.get(i).getPostalCode().equals(id))
                return ListPlace.get(i);
            i++;
        }
        return null;

    }

    @Override
    public void addPlace(Place p) throws RemoteException {
        ListPlace.add(p);
        ObjectRegistryInterface registry = null;

        try {
            registry = (ObjectRegistryInterface) Naming.lookup("rmi://localhost:2023/registry");
            registry.addObject(p.getPostalCode(),"rmi://localhost:2022/placelist");
        }catch (NotBoundException | MalformedURLException e){
            e.printStackTrace();
        }
    }


}
