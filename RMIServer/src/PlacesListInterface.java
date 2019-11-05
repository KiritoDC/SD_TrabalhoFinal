import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface PlacesListInterface extends Remote {

    void addPlace(Place p) throws RemoteException;
    ArrayList allPlaces() throws RemoteException;
    Place getPlace(String id) throws RemoteException;
}
