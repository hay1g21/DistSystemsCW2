import javax.xml.crypto.Data;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;

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

        //for threads and acks
        CountDownLatch cd = new CountDownLatch(R);

        //holds list of datastore ports to send to client
        ArrayList<Integer> dSPorts = new ArrayList<Integer>();
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
                    System.out.println("Back to accepting connections: Current connections " + count);
                    Socket dStore = ss.accept(); //accepts a datastore socket
                    if(dStore != null){
                        System.out.println("Connection accepted: " + dStore);
                    }

                    //Textual messages
                    BufferedReader in = new BufferedReader(new InputStreamReader(dStore.getInputStream())); //listens from port
                    PrintWriter out = new PrintWriter(dStore.getOutputStream(), true); //prints to datastore, replies
                    //Data messages (For testing)
                    InputStream inData = dStore.getInputStream(); //gets
                    OutputStream outData = dStore.getOutputStream(); //sends

                    String line;
                    while((line = in.readLine()) != null) {
                        System.out.println(line + " received");
                        if (line.contains("JOIN")) {
                            count++;
                            System.out.println("JOINED + " + count);
                            //parse out port
                            int portNum = Integer.parseInt(line.split(" ")[1]);
                            System.out.println("Port num: " + portNum);
                            //add port num
                            dSPorts.add(portNum);
                            System.out.println("Num ports: " + dSPorts.size());
                            new Thread(new DataStoreThread(dStore, cd)).start(); //allows multithreading of datastores
                            break;
                        }
                        else {
                            System.out.println("Client is joining");
                            new Thread(new ClientThread(dStore,dSPorts,R, cd)).start(); //allows multithreading of datastores
                            break;
                        }
                    }
                    /*
                    String line;
                    //blocks until a line is read. gives null if the connection is closed
                    while((line = in.readLine()) != null){
                        System.out.println(line+" received");
                        if(line.equals("JOIN")){
                            count++;
                            System.out.println("JOINED + " + count);
                        }else{
                            System.out.println("Not a datastore but nothing done");
                        }
                        if(count >= R){
                            System.out.println("Can accept client commands now");

                            Socket client = ss.accept(); //accepts client
                            if(client != null){
                                System.out.println("Client accepted: " + client);
                            }


                        }
                    }
                    */

                    /*
                    //close connection once finished
                    System.out.println("Closing");
                    dStore.close();

                    */
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

        CountDownLatch latch;
        DataStoreThread (Socket dataStore, CountDownLatch latch){
            this.dataStore = dataStore;
            this.latch = latch;
        }

        public void run() {
            receiveMessage();
        }

        public void receiveMessage(){
            try {
                //Textual messages
                BufferedReader in = new BufferedReader(new InputStreamReader(dataStore.getInputStream())); //listens from port
                PrintWriter out = new PrintWriter(dataStore.getOutputStream(), true); //prints to datastore, replies
                //Data messages (For testing)
                InputStream inData = dataStore.getInputStream(); //gets
                OutputStream outData = dataStore.getOutputStream(); //sends
                String line;

                out.println("Acknowledged connection");
                while((line = in.readLine()) != null){
                    System.out.println(line+" received, now choosing what to do");
                    if(line.equals("SEND")) {
                        System.out.println("Sending");
                        //send file logic
                        //out.println("Hello datastore I'm sending a file");
                        Thread.sleep(1000);
                        File toSend = new File("basic.txt");
                        FileInputStream inp = new FileInputStream(toSend);
                        byte[] buf = new byte[1000];
                        int buflen;
                        while ((buflen = inp.read(buf)) != -1) {
                            System.out.println("*");
                            outData.write(buf, 0, buflen);
                        }
                        System.out.println("Finished reading");
                        inp.close();
                    }else if (line.contains("STORE_ACK")){
                        //countdown the latch
                        System.out.println("Message has been sent, decrement ack");
                        latch.countDown();

                    }else{
                        System.out.println("Nothing special with this line");
                    }
                }
                System.out.println("Datastore close");
                dataStore.close();
            } catch(Exception e) {
                System.err.println("error: " + e);
            }
        }

    }

    static class ClientThread implements Runnable{
        Socket client;
        ArrayList<Integer> listPorts;

        int R;

        CountDownLatch latch;
        ClientThread (Socket client, ArrayList<Integer> listPorts, int R, CountDownLatch latch){
            this.client = client;
            this.listPorts = listPorts;
            this.R = R;
            this.latch = latch;
            //listPorts.add(134);
        }

        public void run() {
            receiveMessage();
        }

        public void receiveMessage(){
            try {
                //Textual messages
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream())); //listens from port
                PrintWriter out = new PrintWriter(client.getOutputStream(), true); //prints to datastore, replies
                //Data messages (For testing)
                InputStream inData = client.getInputStream(); //gets
                OutputStream outData = client.getOutputStream(); //sends
                String line;

                //out.println("Acknowledged connection to client");
                while((line = in.readLine()) != null){
                    System.out.println(line+" received, now choosing what to do");
                    if(line.contains("STORE")){
                        System.out.println("Client wants to store file: ");
                        //parse message
                        String lines[] = line.split(" ");
                        String fileName = lines[1];
                        String size = lines[2];
                        System.out.println("Preparing to store " + fileName + "of size " + size);
                        //get R ports
                        String chosenPorts = "";
                        //if not enough datastores send error message
                        if(listPorts.size() < R){
                            System.out.println("Not enough datastores");
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                            break;
                        }else{
                            for(int i = 0; i < R; i++){
                                chosenPorts = chosenPorts + " " + listPorts.get(i);
                            }
                            //System.out.println(Protocol.STORE_TO_TOKEN + chosenPorts);
                            out.println(Protocol.STORE_TO_TOKEN + chosenPorts);
                            //now wait for acknowlegements
                            System.out.println("Now waiting for countdown latch : value " + latch.getCount());

                            latch.await();
                            System.out.println("Latch complete");
                            out.println(Protocol.STORE_COMPLETE_TOKEN);
                            System.out.println("Finished");
                        }
                    }else{
                        System.out.println("Nothing special with this line");
                    }
                }
                System.out.println("Closing client");
                client.close();
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
