import java.io.File;
import java.util.ArrayList;
import java.util.Vector;

public class FileStateObject {
    String fileName;
    int fileSize;
    String state;

    Vector<Integer> ports = new Vector<Integer>();
    //takes a file and a state of it
    FileStateObject(String fileName, int fileSize, String state){
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

    public void addPort(int port){
        //add port to list that stores the file
        ports.add(port);
    }

    public String getPorts(){
        String line = "";
        for(int port: ports){
            line = " " + port;
        }
        return line;
    }
}
