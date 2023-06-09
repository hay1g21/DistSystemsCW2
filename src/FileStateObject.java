import java.io.File;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;

public class FileStateObject {
    String fileName;
    int fileSize;
    String state;

    CountDownLatch countDownLatch;

    CountDownLatch remCountDownLatch;

    Vector<Integer> ports = new Vector<Integer>();

    Vector<Socket> sockets = new Vector<>();

    //takes a file and a state of it
    FileStateObject(String fileName, int fileSize, String state) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.state = state;
    }

    public String getFileName() {
        return fileName;
    }

    public int getFileSize() {
        return fileSize;
    }

    public String getState() {
        return state;
    }

    public void setFilename(String fileName) {
        this.fileName = fileName;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void addPort(int port) {
        //add port to list that stores the file
        ports.add(port);
    }

    public void setCountDownLatch(int R){
        countDownLatch = new CountDownLatch(R);

    }


    public void setRemCountDownLatch(int R){
        remCountDownLatch = new CountDownLatch(R);
    }

    public CountDownLatch getCountDownLatch(){
        return countDownLatch;
    }

    public CountDownLatch getRemCountDownLatch(){
        return remCountDownLatch;
    }

    public Vector<Integer> getPorts() {
        /*
        String line = "";
        for(int port: ports){
            line = " " + port;
        }

         */
        return ports;
    }

    //add socket of datastore that holds the file
    public void addSocket(Socket socket) {

        sockets.add(socket);
    }

    public Vector<Socket> getSockets() {
        /*
        String line = "";
        for(int port: ports){
            line = " " + port;
        }

         */
        return sockets;
    }
}
