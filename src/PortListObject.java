import java.net.Socket;
import java.util.Vector;

public class PortListObject {
    //keeps relationships of controller port entry points and dstore reciever ports

    Integer dPort;

    Integer cPort;

    //takes a file and a state of it
    PortListObject(Integer dPort, Integer cPort) {
        this.dPort = dPort;
        this.cPort = cPort;
    }

    public Integer getcPort() {
        return cPort;
    }

    public Integer getdPort() {
        return dPort;
    }



}
