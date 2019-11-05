import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MonitoringInterface extends Remote {
    void ping() throws RemoteException;
}
