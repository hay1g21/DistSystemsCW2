import java.io.File;

public class FileStateObject {
    String fileName;
    int fileSize;
    String state;
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
}
