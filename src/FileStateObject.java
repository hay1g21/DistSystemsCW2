import java.io.File;

public class FileStateObject {
    String fileName;
    String state;
    //takes a file and a state of it
    FileStateObject(String fileName, String state){
        this.fileName = fileName;
        this.state = state;
    }

    public String getFileName() {
        return fileName;
    }

    public String getState() {
        return state;
    }

    public void setFilename(String fileName) {
        this.fileName = fileName;
    }

    public void setState(String state) {
        this.state = state;
    }
}
