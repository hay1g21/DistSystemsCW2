import javax.xml.crypto.Data;
import java.io.*;
import java.net.*;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

public class Controller {
    //controller starts first with R as an argument
    //Controller orchestrates client requests and maintains an index with the allocation of files
    //to Dstores, as well as the size of each stored file
    //Doesn't start until at least R Dstores have joined system

    //Controller: java Controller cport R timeout rebalance_period
    static int count = 0;

    // Vector to store active clients
    static Vector<DataStoreThread> activeDataStores = new Vector<>(); //holds active threads of datastores
    public static void main(String[] args) throws Exception{

        final int cport = Integer.parseInt(args[0]);
        //int R = Integer.parseInt(args[1]);
        //int timeout = Integer.parseInt(args[2]);
        //int rebalance_period = Integer.parseInt(args[3]);
        //final int cport = 12345;

        //gives R as argument
        int R = 1;
        int timeout = 1000;
        int rebalance_period = 100000;


        System.out.println("Server started on port: " + cport);
        ServerSocket ss = null; //makes server socket

        //first wants datastores to join
        //at least R amount
        //add synchronisation
        //AVOID MULTITHREADING FOR NOW
        try {
            ss = new ServerSocket(cport);
            while(true) {
                try {
                    Socket dStore = ss.accept(); //accepts a datastore socket
                    if(dStore != null){
                        System.out.println("DataStore accepted: " + dStore + " count is now " + count);

                        //new Thread(new DataStoreThread(dStore)).start(); //allows multithreading of datastores


                    }
                    //Textual messages
                    BufferedReader in = new BufferedReader(new InputStreamReader(dStore.getInputStream())); //listens from port
                    PrintWriter out = new PrintWriter(dStore.getOutputStream(), true); //prints to datastore, replies
                    //Data messages (For testing)
                    InputStream inData = dStore.getInputStream(); //gets
                    OutputStream outData = dStore.getOutputStream(); //sends
                    String line;
                    //blocks until a line is read. gives null if the connection is closed
                    while((line = in.readLine()) != null){
                        System.out.println(line+" received");
                        if(line.equals("JOIN")){
                            count++;
                            System.out.println("JOINED + " + count);
                            out.println("Hello datastore");
                            //try sending a file to it
                            File toSend = new File("basic.txt");
                            FileInputStream inp = new FileInputStream(toSend);
                            byte[] buf = new byte[1000]; int buflen;
                            while ((buflen=inp.read(buf)) != -1){
                                System.out.print("*");
                                outData.write(buf,0,buflen);
                            }
                            System.out.println("Finished reading");
                            inp.close();
                        }else{
                            System.out.println("Not a datastore, removing client");
                        }
                        if(count >= R){
                            System.out.println("Can accept client commands now");
                            /*
                            Socket client = ss.accept(); //accepts client
                            if(client != null){
                                System.out.println("Client accepted: " + client);
                            }

                             */
                        }
                    }
                    //close connection once finished
                    System.out.println("Closing");
                    dStore.close();
                } catch(Exception e) { System.err.println("error: " + e); }
            }
        } catch(Exception e) { System.err.println("error: " + e);
        } finally {
            if (ss != null)
                try {
                    ss.close();

                } catch (IOException e) { System.err.println("error: " + e); }
        }


    }
    //**avoid until ready

    static class DataStoreThread implements Runnable{
        Socket dataStore;
        DataStoreThread (Socket dataStore){
            this.dataStore = dataStore;
        }

        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(dataStore.getInputStream()));
                String line;
                while((line = in.readLine()) != null){
                    System.out.println(line+" received");
                    if(line.equals("JOIN")){
                        System.out.println("JOINED");
                    }
                }
                dataStore.close();
            } catch(Exception e) {
                System.err.println("error: " + e);
            }
        }

    }


}

/*
BufferedReader in = new BufferedReader(new InputStreamReader(dStore.getInputStream()));
                    String line;
                    while((line = in.readLine()) != null){
                        System.out.println(line+" received");
                        if(line.equals("JOIN")){
                            count++;
                            System.out.println("JOINED");
                        }
                    }
                    dStore.close();
 */
