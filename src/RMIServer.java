import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;

public class RMIServer {
    public static PlacesManager placeList;
    public static Registry r;
    public static void main (String args[]) {
        try {
            r = LocateRegistry.createRegistry(Integer.parseInt(args[0]));
        } catch (RemoteException e1) {
            e1.printStackTrace();
        }

        try {            //System.out.println(args[0]);
            placeList = new PlacesManager(Integer.parseInt(args[0]));

            try {
                r.rebind("placelist", placeList);
            } catch (RemoteException e1) {
                e1.printStackTrace();
            }

            System.out.println("Place server ready");
        } catch (Exception e) {
            System.out.println("Place server main " + e.getMessage());
        }
    }
}
