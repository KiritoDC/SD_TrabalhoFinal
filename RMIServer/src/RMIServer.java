import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RMIServer {

    public static void main(String[] args) {
        Registry r = null;
        PlaceManager placeList =null;

        try{
            r = LocateRegistry.createRegistry(2022);
        }catch(RemoteException a){
            a.printStackTrace();
        }


        try{
            placeList = new PlaceManager();
            r.rebind("placelist", placeList );
            System.out.println("Place server ready");

        }catch(Exception e) {
            System.out.println("Place server main " + e.getMessage());
        }
    }
}
