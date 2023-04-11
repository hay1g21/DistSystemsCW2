import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class DataStore {
    //listen on different ports, establish connection as soon as they start

    //A Dstore: java Dstore port cport timeout file_folder

    //R is replication factor
    //we have N datastores

    //For data messages (i.e., file content), processes should send using the write()
    //method of OutputStream class and receive using the readNBytes() method of
    //InputStream class.
    public static void main(String[] args) throws Exception{
        //datastores should use different ports

        int port = Integer.parseInt(args[0]);
        //int cport = Integer.parseInt(args[1]);
        //int timeout = Integer.parseInt(args[2]);
        String file_folder = args[3];
        int cport = 12345;
        int timeout = 1000; //ms
        //String file_folder = "to_store";

        //makes a directory
        File uploadFolder = new File(file_folder);
        if (!uploadFolder.exists())
            if (!uploadFolder.mkdir()) throw new RuntimeException("Cannot create download folder (folder absolute path: " + uploadFolder.getAbsolutePath() + ")");


        Socket controllerSocket = null;
        ServerSocket dSSocket = null;

        try {
            InetAddress address = InetAddress.getLocalHost();
            controllerSocket = new Socket(address, cport); //sends to cport -controller listens at this port
            dSSocket = new ServerSocket(port); //port to listen for messages this way, make it server socket??
            new Thread(new DataStoreReceiver(dSSocket,controllerSocket,uploadFolder)).start();
            //System.out.println("Socket started on port:" + dSSocket.getLocalPort());
            PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);
            //listens to textual messages from the controller
            BufferedReader in = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
            //listens to data messages from controller
            InputStream inData = controllerSocket.getInputStream();
            //filereading test

            File testFile = new File(uploadFolder+"/file2.txt");
            System.out.println(testFile.getAbsolutePath());
            FileOutputStream fileOut = new FileOutputStream(testFile);

            out.println(Protocol.JOIN_TOKEN + " " + port);

            while(true){
                //Thread.sleep(1000);
                System.out.println("I'm still alive");
                //get a connection to listen for messages
                String line;
                while((line = in.readLine()) != null) {
                    System.out.println(line + " received");
                    break;
                }
                /*
                out.println("SEND");
                //try reading a file, needs a fileoutput stream
                //gets a buffer
                System.out.println("Waiting for file");
                byte[] buf = new byte[1000]; int buflen;
                while ((buflen=inData.read(buf)) != -1){
                    System.out.println("*");
                    fileOut.write(buf,0,buflen);
                    System.out.println("File Finished Reading");
                    break;

                }

                 */
                System.out.println("Done waiting, looping back");
                //inData.close();
                fileOut.close();
            }
            
        } catch(Exception e) { System.err.println("error: " + e);
        } finally {
            if (controllerSocket != null)
                try { controllerSocket.close(); } catch (IOException e) { System.err.println("error: " + e); }
            if (!uploadFolder.exists()){
                uploadFolder.delete();
            }
        }
    }

    static class DataStoreReceiver implements Runnable{

        ServerSocket ds;
        Socket cport; //server socket
        File toStore;

        DataStoreReceiver (ServerSocket ds, Socket cport, File toStore){
            this.ds = ds;
            //listPorts.add(134);
            this.cport = cport;
            this.toStore = toStore;
        }

        public void run() {
            try{
                receiveMessage();
            } catch(Exception e) { System.err.println("error: " + e);
            } finally {
                if (ds != null)
                    try {
                        System.out.println("Server Socket Closing");
                        ds.close();

                    } catch (IOException e) { System.err.println("error: " + e); }
            }

        }

        public void receiveMessage(){
            while(true) {
                try {

                    System.out.println("Accepting Connections from port: " + ds.getLocalPort());
                    Socket client = ds.accept();
                    System.out.println("Connection accepted : " + client);
                    //Textual messages
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream())); //listens from port
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true); //prints to datastore, replies
                    //Data messages (For testing)
                    InputStream inData = client.getInputStream(); //gets
                    OutputStream outData = client.getOutputStream(); //sends
                    String line;

                    //controller socket
                    PrintWriter outC = new PrintWriter(cport.getOutputStream(), true); //prints to datastore, replies

                    //out.println("Acknowledged connection to client");
                    while ((line = in.readLine()) != null) {
                        System.out.println(line + " received, now choosing what to do");
                        if (line.contains("STORE")) {
                            //get filename and size
                            String[] split = line.split(" ");
                            System.out.println("Storing file: " + split[1] + " with size " + split[2]);
                            File newFile = new File(toStore + "/" + split[1]);
                            byte[] buf = new byte[Integer.parseInt(split[2])];
                            int buflen;

                            FileOutputStream fileOut = new FileOutputStream(newFile);
                            //send ack and prepare to read
                            out.println(Protocol.ACK_TOKEN);
                            //now read in file
                            while ((buflen = inData.read(buf)) != -1) {
                                System.out.println("*Writing*");
                                fileOut.write(buf, 0, buflen);
                                break;
                            }
                            System.out.println("Finished reading file " + newFile.getName());
                            //send acknowledgement to controller
                            outC.println(Protocol.STORE_ACK_TOKEN + " " + newFile.getName());
                            fileOut.close();
                        }else if(line.contains("LOAD_DATA")){
                            System.out.println("Wants to load a file");
                            String[] split = line.split(" ");
                            System.out.println("Storing file: " + split[1]);

                            File fileToLoad = new File(toStore + "/" + split[1]);
                            if(fileToLoad.exists()){
                                FileInputStream inp = new FileInputStream(fileToLoad);
                                //time to write the file
                                byte[] buf = new byte[1000];
                                int buflen;
                                while ((buflen = inp.read(buf)) != -1) {
                                    System.out.println("*");
                                    outData.write(buf, 0, buflen);
                                }
                                System.out.println("Finished writing");
                                inp.close();
                            }else{
                                System.out.println("File does not exist");
                            }
                        } else {
                            System.out.println("Nothing special with this line");
                        }
                    }
                    System.out.println("Closing client");
                    client.close();
                } catch (Exception e) {
                    System.err.println("error: " + e);
                }
            }
        }

    }

    static class ControllerListener implements Runnable{

        ServerSocket ds;
        Socket cport; //server socket
        File toStore;

        ControllerListener (ServerSocket ds, Socket cport, File toStore){
            this.ds = ds;
            //listPorts.add(134);
            this.cport = cport;
            this.toStore = toStore;
        }

        public void run() {
            try{
                receiveMessage();
            } catch(Exception e) { System.err.println("error: " + e);
            } finally {
                if (ds != null)
                    try {
                        System.out.println("Server Socket Closing");
                        ds.close();

                    } catch (IOException e) { System.err.println("error: " + e); }
            }

        }

        public void receiveMessage(){
            while(true) {
                try {

                    System.out.println("Accepting Connections from port: " + ds.getLocalPort());
                    Socket client = ds.accept();
                    System.out.println("Connection accepted : " + client);
                    //Textual messages
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream())); //listens from port
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true); //prints to datastore, replies
                    //Data messages (For testing)
                    InputStream inData = client.getInputStream(); //gets
                    OutputStream outData = client.getOutputStream(); //sends
                    String line;

                    //controller socket
                    PrintWriter outC = new PrintWriter(cport.getOutputStream(), true); //prints to datastore, replies

                    //out.println("Acknowledged connection to client");
                    while ((line = in.readLine()) != null) {
                        System.out.println(line + " received, now choosing what to do");
                        if (line.contains("STORE")) {
                            //get filename and size
                            String[] split = line.split(" ");
                            System.out.println("Storing file: " + split[1] + " with size " + split[2]);
                            File newFile = new File(toStore + "/" + split[1]);
                            byte[] buf = new byte[Integer.parseInt(split[2])];
                            int buflen;

                            FileOutputStream fileOut = new FileOutputStream(newFile);
                            //send ack and prepare to read
                            out.println(Protocol.ACK_TOKEN);
                            //now read in file
                            while ((buflen = inData.read(buf)) != -1) {
                                System.out.println("*Writing*");
                                fileOut.write(buf, 0, buflen);
                                break;
                            }
                            System.out.println("Finished reading file " + newFile.getName());
                            //send acknowledgement to controller
                            outC.println(Protocol.STORE_ACK_TOKEN + " " + newFile.getName());
                            fileOut.close();
                        }else if(line.contains("LOAD_DATA")){
                            System.out.println("Wants to load a file");
                            String[] split = line.split(" ");
                            System.out.println("Storing file: " + split[1]);

                            File fileToLoad = new File(toStore + "/" + split[1]);
                            if(fileToLoad.exists()){
                                FileInputStream inp = new FileInputStream(fileToLoad);
                                //time to write the file
                                byte[] buf = new byte[1000];
                                int buflen;
                                while ((buflen = inp.read(buf)) != -1) {
                                    System.out.println("*");
                                    outData.write(buf, 0, buflen);
                                }
                                System.out.println("Finished writing");
                                inp.close();
                            }else{
                                System.out.println("File does not exist");
                            }
                        } else {
                            System.out.println("Nothing special with this line");
                        }
                    }
                    System.out.println("Closing client");
                    client.close();
                } catch (Exception e) {
                    System.err.println("error: " + e);
                }
            }
        }

    }
}
