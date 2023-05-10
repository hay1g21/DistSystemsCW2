import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Controller {
    //controller starts first with R as an argument
    //Controller orchestrates client requests and maintains an index with the allocation of files
    //to Dstores, as well as the size of each stored file
    //Doesn't start until at least R Dstores have joined system

    //Controller: java Controller cport R timeout rebalance_period
    static int count = 0;
    static int timeout;
    static CountDownLatch latch;
    static CountDownLatch removeLatch;
    // Vector to store active clients
    static Vector<DataStoreThread> activeDataStores = new Vector<>(); //holds active threads of datastores

    static Vector<PortListObject> portList = new Vector<>();
    static Vector<Integer> dSPorts = new Vector<Integer>();
    //stores list of files, not datastore related

    static Vector<FileStateObject> fileList = new Vector<FileStateObject>();

    public static void main(String[] args) throws Exception {

        final int cport = Integer.parseInt(args[0]);
        int R = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        int rebalance_period = Integer.parseInt(args[3]);
        //final int cport = 12345;

        //gives R as argument
        //int R = 1;
        //int timeout = 1000;
        //int rebalance_period = 100000;

        //for threads and acks
        //CountDownLatch cd = new CountDownLatch(R);

        latch = new CountDownLatch(R);
        removeLatch = new CountDownLatch(R);
        //holds list of datastore ports to send to client

        System.out.println("Server started on port: " + cport);
        ServerSocket ss = null; //makes server socket
        //first wants datastores to join
        //at least R amount
        //add synchronisation
        try {
            ss = new ServerSocket(cport);
            while (true) {
                try {
                    System.out.println("Back to accepting connections: Current connections " + count);
                    Socket dStore = ss.accept(); //accepts a datastore socket
                    if (dStore != null) {
                        System.out.println("Connection accepted: " + dStore);
                        System.out.println("Making new thread");
                        count++;
                        new Thread(new ReceiverThread(dStore,R)).start(); //allows multithreading of datastores
                    }
                    /*
                    //Textual messages
                    BufferedReader in = new BufferedReader(new InputStreamReader(dStore.getInputStream())); //listens from port
                    PrintWriter out = new PrintWriter(dStore.getOutputStream(), true); //prints to datastore, replies
                    //Data messages (For testing)
                    InputStream inData = dStore.getInputStream(); //gets
                    OutputStream outData = dStore.getOutputStream(); //sends

                    String line;
                    while ((line = in.readLine()) != null) {
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
                            new Thread(new DataStoreThread(dStore, R, latch)).start(); //allows multithreading of datastores
                            break;
                        } else {
                            System.out.println("Client is joining");
                            new Thread(new ClientThread(dStore, dSPorts, R, latch)).start(); //allows multithreading of datastores
                            break;
                        }
                    }

                     */
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
                } catch (Exception e) {
                    System.err.println("error: " + e);
                }
            }
        } catch (Exception e) {
            System.err.println("error: " + e);
        } finally {
            if (ss != null)
                try {
                    ss.close();

                } catch (IOException e) {
                    System.err.println("error: " + e);
                }
        }


    }
    //**avoid until ready

    static class DataStoreThread implements Runnable {
        Socket dataStore;
        int R;
        CountDownLatch latch;

        DataStoreThread(Socket dataStore, int R, CountDownLatch latch) {
            this.dataStore = dataStore;
            this.latch = latch;
            this.R = R;
        }

        public void run() {
            receiveMessage();
        }

        public void receiveMessage() {
            try {
                //Textual messages
                BufferedReader in = new BufferedReader(new InputStreamReader(dataStore.getInputStream())); //listens from port
                PrintWriter out = new PrintWriter(dataStore.getOutputStream(), true); //prints to datastore, replies
                //Data messages (For testing)
                InputStream inData = dataStore.getInputStream(); //gets
                OutputStream outData = dataStore.getOutputStream(); //sends
                String line;

                out.println("Acknowledged connection");
                while ((line = in.readLine()) != null) {
                    System.out.println(line + " received, now choosing what to do");
                    if (line.equals("SEND")) {
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
                    } else if (line.contains("STORE_ACK")) {
                        //countdown the latch
                        System.out.println("Message has been sent, decrement ack");
                        latch.countDown();
                        System.out.println("Latch now " + latch.getCount());
                        String filename = line.split(" ")[1];
                        //reset latch
                        //fileList.get(0).addPort(dataStore.getPort());
                        //add port to fileobject
                        for (FileStateObject obj : fileList) {
                            if (obj.getFileName().equals(filename)) {
                                obj.addSocket(dataStore);
                            }
                        }
                        Controller.latch = new CountDownLatch(R);
                        latch = Controller.latch;

                    } else if (line.contains("REMOVE_ACK")) {
                        //countdown the latch
                        System.out.println("Message has been sent, decrement remove ack");
                        removeLatch.countDown();
                        System.out.println("Latch now " + latch.getCount());
                        String filename = line.split(" ")[1];

                        //fileList.get(0).addPort(dataStore.getPort());
                        //remove port from fileobject
                        /*
                    for(FileStateObject obj : fileList){
                        if(obj.getFileName().equals(filename)){
                            obj.addSocket(dataStore);
                        }
                    }

                         */
                        Controller.removeLatch = new CountDownLatch(R);
                        System.out.println("*Decrement complete*");
                    } else {
                        System.out.println("Nothing special with this line");
                    }
                }
                System.out.println("Datastore close");
                dataStore.close();
            } catch (Exception e) {
                System.err.println("error: " + e);
            }
        }

    }

    static class ClientThread implements Runnable {
        Socket client;
        ArrayList<Integer> listPorts;

        int R;

        CountDownLatch latch;

        ClientThread(Socket client, ArrayList<Integer> listPorts, int R, CountDownLatch latch) {
            this.client = client;
            this.listPorts = listPorts;
            this.R = R;
            this.latch = latch;
            //listPorts.add(134);
        }

        public void run() {
            receiveMessage();
        }

        public void receiveMessage() {
            try {
                //Textual messages
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream())); //listens from port
                PrintWriter out = new PrintWriter(client.getOutputStream(), true); //prints to datastore, replies
                //Data messages (For testing)
                InputStream inData = client.getInputStream(); //gets
                OutputStream outData = client.getOutputStream(); //sends
                String line;

                //out.println("Acknowledged connection to client");
                while ((line = in.readLine()) != null) {
                    System.out.println(line + " received, now choosing what to do");
                    if (line.contains("STORE")) {
                        System.out.println("Client wants to store file: ");
                        //parse message
                        String lines[] = line.split(" ");
                        String fileName = lines[1];
                        String size = lines[2];
                        System.out.println("Preparing to store " + fileName + " of size " + size);
                        //get R ports
                        String chosenPorts = "";

                        //if not enough datastores send error message
                        if (listPorts.size() < R) {
                            System.out.println("Not enough datastores");
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        } else {
                            Boolean exists = false;
                            //if file already exists return error
                            for (FileStateObject obj : fileList) {
                                if (obj.getFileName().equals(fileName)) {
                                    System.out.println("Already exists");
                                    exists = true;
                                }
                            }
                            if (exists) {
                                out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                            } else {


                                //add file to index
                                FileStateObject obj = new FileStateObject(fileName, Integer.parseInt(size), "store in progress");
                                fileList.add(obj);
                                for (int i = 0; i < R; i++) {
                                    chosenPorts = chosenPorts + " " + listPorts.get(i);
                                    obj.addPort(listPorts.get(i));

                                }
                                //System.out.println(Protocol.STORE_TO_TOKEN + chosenPorts);
                                out.println(Protocol.STORE_TO_TOKEN + chosenPorts);
                                //now wait for acknowlegements
                                System.out.println("Now waiting for countdown latch : value " + latch.getCount());

                                latch.await();
                                System.out.println("Latch complete");
                                out.println(Protocol.STORE_COMPLETE_TOKEN);
                                //find file and update state
                                for (FileStateObject fileObject : Controller.fileList) {
                                    if (fileObject.getFileName().equals(fileName)) {
                                        //updatestate
                                        System.out.println("Updating state of " + fileObject.getFileName() + " to complete");
                                        fileObject.setState("store complete");
                                        System.out.println("State of " + fileObject.getFileName() + " now " + fileObject.getState());
                                    }
                                }


                                //PrintWriter outC = new PrintWriter(obj.getSockets().get(0).getOutputStream(), true); //prints to datastore, replies
                                //outC.println("Hello data store its me after a store");
                                //System.out.println("Finished");
                                //reset countdown latch

                                latch = Controller.latch;
                                System.out.println("Value of latch: " + latch.getCount());
                            }
                        }
                    } else if (line.contains("LOAD")) {
                        System.out.println("Client wants to load a file");
                        //parse message
                        String lines[] = line.split(" ");
                        String fileName = lines[1];
                        //prepare size
                        int size = 0;
                        System.out.println("Preparing to load " + fileName);
                        //get R ports
                        String chosenPorts = "";

                        //if not enough datastores send error message
                        if (listPorts.size() < R) {
                            System.out.println("Not enough datastores");
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        } else {
                            //check if file exists also prepare fileobject
                            FileStateObject fileObj = null;
                            Boolean exists = false;
                            for (FileStateObject obj : fileList) {
                                if (obj.getFileName().equals(fileName) && !obj.getState().equals("store in progress")) {
                                    System.out.println("Exists");
                                    size = obj.getFileSize();
                                    System.out.println("State = " + obj.getState());
                                    fileObj = obj;
                                    exists = true;

                                }
                            }
                            if (!exists) {
                                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            } else {
                                System.out.println("Now picking datastore");
                                //do one for now
                                //get list of ports that store the datastore
                                Vector<Integer> availablePorts = fileObj.getPorts();
                                chosenPorts = chosenPorts + " " + availablePorts.get(0);
                                System.out.println(Protocol.LOAD_FROM_TOKEN + chosenPorts + " " + size);
                                out.println(Protocol.LOAD_FROM_TOKEN + chosenPorts + " " + size);
                            }

                        }
                    } else if (line.contains("RELOAD")) {
                        System.out.println("Client wants to reload files");

                    } else if (line.contains("REMOVE")) {
                        System.out.println("Client wants to remove files");
                        String lines[] = line.split(" ");
                        String fileName = lines[1];

                        System.out.println("Preparing to remove " + fileName);
                        Vector<Socket> removePorts = null;
                        FileStateObject fileObj = null;
                        Boolean exists = false;
                        for (FileStateObject obj : fileList) {
                            if (obj.getFileName().equals(fileName)) {
                                System.out.println("Setting state " + obj.getState() + " to remove");
                                obj.setState("remove in progress");
                                exists = true;
                                removePorts = obj.getSockets(); //gets list of ports holding the file
                                fileObj = obj;
                            }
                        }

                        //gets datastores storing file
                        for (Socket dataStore : removePorts) {
                            PrintWriter outC = new PrintWriter(dataStore.getOutputStream(), true);
                            //send remove messgae

                            outC.println(Protocol.REMOVE_TOKEN + " " + fileObj.getFileName());
                        }
                        //wait for acks
                        System.out.println("Now waiting for countdown latch : value " + latch.getCount());

                        removeLatch.await();

                        System.out.println("Remove Latch complete");
                        out.println(Protocol.REMOVE_COMPLETE_TOKEN);
                        //find file and update state
                        for (FileStateObject fileObject : Controller.fileList) {
                            if (fileObject.getFileName().equals(fileName)) {
                                //updatestate
                                System.out.println("Updating state of " + fileObject.getFileName() + " to removed");
                                fileObject.setState("remove complete");
                                System.out.println("State of " + fileObject.getFileName() + " now " + fileObject.getState());
                            }
                        }
                        System.out.println("Remove finished");
                    } else if (line.contains("LIST")) {
                        System.out.println("Client wants a list of files: ");
                        if (listPorts.size() < R) {
                            System.out.println("Not enough datastores");
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        } else {
                            //get list of files from index
                            String message = "";
                            for (FileStateObject fileObject : fileList) {
                                message = message + " " + fileObject.getFileName();
                            }
                            System.out.println("Sending" + message);
                            out.println(Protocol.LIST_TOKEN + message);
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


    static class ReceiverThread implements Runnable {
        Socket client;


        int R;

        Boolean exists = false;

        Integer previousPort; //for reloading
        Vector<Integer> triedPorts;
        ReceiverThread(Socket client, int R) {
            this.client = client;
            this.R = R;
            //listPorts.add(134);
            previousPort = 0;
            triedPorts = new Vector<Integer>();
        }

        public void run() {
            receiveMessage();
        }

        public void receiveMessage() {
            try {
                //Textual messages
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream())); //listens from port
                PrintWriter out = new PrintWriter(client.getOutputStream(), true); //prints to datastore, replies
                //Data messages (For testing)
                InputStream inData = client.getInputStream(); //gets
                OutputStream outData = client.getOutputStream(); //sends
                String line;

                //out.println("Acknowledged connection to client");
                while ((line = in.readLine()) != null) {
                    System.out.println(line + " received, now choosing what to do");
                    //datastore stuff
                    if (line.contains("JOIN")) {
                        //Controller.count++;

                        System.out.println("DATASTORE JOINED + " + Controller.count);
                        //parse out port
                        int portNum = Integer.parseInt(line.split(" ")[1]);
                        System.out.println("Port num: " + portNum);
                        //add port num
                        dSPorts.add(portNum);
                        portList.add(new PortListObject(portNum, client.getPort()));
                        System.out.println("Num ports: " + dSPorts.size());
                        System.out.println("Num ports: in obj =  " + portList.size());
                        listAvailablePorts();

                    }
                    else if (line.contains("STORE_ACK")) {
                        //countdown the latch
                        System.out.println("Message has been successfully sent by dataStore, decrement ack from value: " + latch.getCount());
                        latch.countDown();
                        System.out.println("Latch now " + latch.getCount());
                        String filename = line.split(" ")[1];
                        //reset latch
                        //fileList.get(0).addPort(dataStore.getPort());
                        //add port to fileobject
                        for (FileStateObject obj : fileList) {
                            if (obj.getFileName().equals(filename)) {
                                obj.addSocket(client); //add to the list of datastore sockets that is storing
                            }
                        }
                        //Controller.latch = new CountDownLatch(R);
                        //latch = Controller.latch;

                    } else if (line.contains("STORE")) {
                        System.out.println("Client wants to store file: ");
                        //parse message
                        String lines[] = line.split(" ");
                        String fileName = lines[1];
                        String size = lines[2];
                        System.out.println("Preparing to store " + fileName + " of size " + size);
                        //get R ports
                        String chosenPorts = "";

                        //if not enough datastores send error message
                        if (dSPorts.size() < R) {
                            System.out.println("Not enough datastores");
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        } else {

                            //if file already exists return error
                            synchronized (this.getClass()) {
                                exists = false;
                                for (FileStateObject obj : fileList) {
                                    if (obj.getFileName().equals(fileName)) {
                                        //System.out.println(fileName + " Already exists in index");
                                        exists = true;
                                    }
                                    System.out.println("Checking...");
                                }


                                if (exists) {
                                    System.out.println("Already exists");
                                    out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                                } else {
                                    System.out.println("File assumed to not exist! Proceeding with Store");
                                    //exists = false;
                                    //add file to index
                                    FileStateObject obj = new FileStateObject(fileName, Integer.parseInt(size), "store in progress");
                                    fileList.add(obj);
                                    ArrayList<Integer> countArray = new ArrayList();
                                    //make list of how many files each is holding
                                    //calculate how many files each datastore should hold
                                    //RF/N
                                    int low = (int)Math.floor((float)R*(float)fileList.size()/(float)portList.size());
                                    int high = (int)Math.ceil((float)R*(float)fileList.size()/(float)portList.size());

                                    System.out.println("Datastores need to hold between " + low + "and " + high + " Files");

                                    for (FileStateObject file: fileList){
                                        if(file.getPorts().size() > 0){
                                            countArray.addAll(file.getPorts());
                                        }
                                    }
                                    System.out.println("Size of countarray: " + countArray.size());
                                    Random random = new Random();
                                    int rCount = 0;
                                    int index = 0;
                                    Boolean repeat = false;
                                    while(rCount < R){
                                        //datastore should store RF/N files
                                        //int chosen = random.nextInt(1, listPorts.size());
                                        //System.out.println(listPorts.size());
                                        //choose and check
                                        System.out.println("Checking " + dSPorts.get(index));
                                        int count = 0;
                                        //counts up how many files that port is storing
                                        for(int port : countArray){
                                            if(dSPorts.get(index) == port){
                                                count++;
                                            }
                                        }
                                        //if port is storing too many files ignore
                                        System.out.println(dSPorts.get(index) + " has " + count + " stored files");
                                        if(repeat){
                                            //if there is a repeat in choosing ports, forcibly add a datastore
                                            System.out.println("Imbalance so , Adding to port " + dSPorts.get(index));
                                            chosenPorts = chosenPorts + " " + dSPorts.get(index);
                                            if(!obj.getPorts().contains(dSPorts.get(index))){
                                                obj.addPort(dSPorts.get(index));
                                                rCount++;
                                            }else{
                                                System.out.println("Repeat");
                                                repeat = true;
                                            }
                                        } else if (count+1 > high) {
                                            System.out.println("Too many files, choosing a different one");

                                        } else{
                                            System.out.println("Available space, Adding to port " + dSPorts.get(index));
                                            chosenPorts = chosenPorts + " " + dSPorts.get(index);
                                            if(!obj.getPorts().contains(dSPorts.get(index))){
                                                obj.addPort(dSPorts.get(index));
                                                rCount++;
                                            }else{
                                                System.out.println("Repeat");
                                                repeat = true;
                                            }
                                        }
                                        index = (index + 1) % dSPorts.size();
                                        System.out.println("Index now" + index);
                                        //0
                                    }
                                    /*
                                    for (int i = 0; i < R; i++) {
                                        //datastore should store RF/N files
                                        //int chosen = random.nextInt(1, listPorts.size());
                                        //System.out.println(listPorts.size());
                                        //choose and check
                                        System.out.println("Checking " + dSPorts.get(i));
                                        int count = 0;
                                        for(int port : countArray){
                                            if(dSPorts.get(i) == port){
                                                count++;
                                            }
                                        }
                                        System.out.println(dSPorts.get(i) + " has " + count + " stored files");
                                        if(count+1 > high){
                                            System.out.println("Too many files, choosing a different one");
                                        }else{
                                            System.out.println("Available space, Adding to port " + dSPorts.get(i));
                                            chosenPorts = chosenPorts + " " + dSPorts.get(i);

                                            obj.addPort(dSPorts.get(i));
                                        }
                                    }

                                     */
                                    System.out.println(Protocol.STORE_TO_TOKEN + chosenPorts);
                                    out.println(Protocol.STORE_TO_TOKEN + chosenPorts);
                                    //now wait for acknowlegements
                                    System.out.println("Now waiting for countdown latch : value " + latch.getCount());

                                    latch.await(timeout, TimeUnit.MILLISECONDS); //time out of base 1000
                                    System.out.println("Latch opened with value " + latch.getCount());
                                    //check if receieved all acks. if not dont send message and remove file from index
                                    if (latch.getCount() <= 0) {
                                        System.out.println("All acks recieved");
                                        out.println(Protocol.STORE_COMPLETE_TOKEN);
                                        //find file and update state
                                        for (FileStateObject fileObject : Controller.fileList) {
                                            if (fileObject.getFileName().equals(fileName)) {
                                                //updatestate
                                                System.out.println("Updating state of " + fileObject.getFileName() + " to complete");
                                                fileObject.setState("store complete");
                                                System.out.println("State of " + fileObject.getFileName() + " now " + fileObject.getState());
                                            }
                                        }
                                    } else {
                                        System.out.println("Latch timedout! Removing " + obj.getFileName());
                                        //remove file from index

                                        fileList.remove(obj);
                                        System.out.println("Removed");
                                    }


                                    //PrintWriter outC = new PrintWriter(obj.getSockets().get(0).getOutputStream(), true); //prints to datastore, replies
                                    //outC.println("Hello data store its me after a store");
                                    //System.out.println("Finished");
                                    //reset countdown latch


                                    //increase latch value
                                    Controller.latch = new CountDownLatch(R);
                                    System.out.println("Latch closed. Value of latch: " + latch.getCount());
                                    System.out.println("New size of stored objs : " + fileList.size());
                                }
                            }
                        }
                    } else if (line.contains("LOAD") && !line.contains("RELOAD")) {
                        System.out.println("Client wants to load a file");
                        //parse message
                        String lines[] = line.split(" ");
                        String fileName = lines[1];
                        //prepare size
                        int size = 0;
                        System.out.println("Preparing to load " + fileName);
                        //get R ports
                        String chosenPorts = "";
                        //if not enough datastores send error message
                        if (dSPorts.size() < R) {
                            System.out.println("Not enough datastores");
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        } else {

                            //check if file exists also prepare fileobject
                            FileStateObject fileObj = null;
                            Boolean exists = false;
                            for (FileStateObject obj : fileList) {
                                if (obj.getFileName().equals(fileName) && !obj.getState().equals("store in progress")) {
                                    System.out.println("Exists");
                                    size = obj.getFileSize();
                                    System.out.println("State = " + obj.getState());
                                    fileObj = obj;
                                    exists = true;

                                }
                            }
                            if (!exists) {
                                System.out.println(fileName + " does not exist!");
                                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                            } else {
                                System.out.println("Now picking datastore");
                                //do one for now
                                //get list of ports that store the datastore
                                Vector<Integer> availablePorts = fileObj.getPorts();
                                chosenPorts = chosenPorts + " " + availablePorts.get(0);
                                //add to previously tried port
                                previousPort = availablePorts.get(0);
                                triedPorts = availablePorts;
                                System.out.println(Protocol.LOAD_FROM_TOKEN + chosenPorts + " " + size);
                                out.println(Protocol.LOAD_FROM_TOKEN + chosenPorts + " " + size);
                            }

                        }
                    } else if (line.contains("RELOAD")) {
                        System.out.println("Client wants to reload files");
                        System.out.println("Last port tried was : " + previousPort);
                        //parse message
                        String lines[] = line.split(" ");
                        String fileName = lines[1];
                        //prepare size
                        int size = 0;
                        System.out.println("Preparing to reload " + fileName);
                        //get R ports
                        String chosenPorts = "";
                        //remove port from tried ports
                        System.out.println("Size of listports " + dSPorts.size());
                        if(triedPorts.size() > 0){
                            System.out.println("REMOVING");
                            System.out.println("Tried ports : " + triedPorts.get(0));
                            triedPorts.remove(previousPort);
                        }
                        System.out.println("Ports left: " + triedPorts.size());
                        if (triedPorts.size() <= 0){
                            System.out.println("No ports left, send back error load");
                            out.println(Protocol.ERROR_LOAD_TOKEN);
                        }else{
                            //get file size
                            //FileStateObject fileObj = null;
                            for (FileStateObject obj : fileList) {
                                if (obj.getFileName().equals(fileName) && !obj.getState().equals("store in progress")) {
                                    System.out.println("Exists");
                                    size = obj.getFileSize();
                                    System.out.println("State = " + obj.getState());
                                    //fileObj = obj;
                                }
                            }
                            System.out.println("Selecting other port");
                            //now selects a new port
                            chosenPorts = chosenPorts + " " + triedPorts.get(0);
                            previousPort = triedPorts.get(0);
                            System.out.println(Protocol.LOAD_FROM_TOKEN + chosenPorts + " " + size);
                            out.println(Protocol.LOAD_FROM_TOKEN + chosenPorts + " " + size);

                        }
                        //check how many available ports are left

                    } //datastore stuff
                    else if (line.contains("REMOVE_ACK")) {
                        //countdown the latch
                        System.out.println("Message has been sent, decrement remove ack from value: " + removeLatch.getCount());
                        removeLatch.countDown();

                        System.out.println("Latch now " + removeLatch.getCount());
                        String filename = line.split(" ")[1];

                        //fileList.get(0).addPort(dataStore.getPort());
                        //remove port from fileobject
                        /*
                    for(FileStateObject obj : fileList){
                        if(obj.getFileName().equals(filename)){
                            obj.addSocket(dataStore);
                        }
                    }

                         */
                        //Controller.removeLatch = new CountDownLatch(R);
                        System.out.println("*Decrement complete*");
                    }
                    else if (line.contains("REMOVE")) {
                        System.out.println("Client wants to remove files");
                        String lines[] = line.split(" ");
                        String fileName = lines[1];

                        if(dSPorts.size() < R){
                            System.out.println("Not enough datastores");
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        }else {
                            synchronized (this.getClass()) {
                                System.out.println("Preparing to remove " + fileName);
                                Vector<Socket> removePorts = null;
                                FileStateObject fileObj = null;
                                Boolean exists = false;
                                for (FileStateObject obj : fileList) {
                                    if (obj.getFileName().equals(fileName)) {
                                        System.out.println("Setting state " + obj.getState() + " to remove");
                                        obj.setState("remove in progress");
                                        exists = true;
                                        removePorts = obj.getSockets(); //gets list of ports holding the file
                                        fileObj = obj;
                                    }
                                }
                                if (exists) {
                                    //gets datastores storing file
                                    for (Socket dataStore : removePorts) {
                                        //may bug out here
                                        if (!dataStore.isClosed()) {
                                            PrintWriter outC = new PrintWriter(dataStore.getOutputStream(), true);
                                            //send remove messgae

                                            outC.println(Protocol.REMOVE_TOKEN + " " + fileObj.getFileName());
                                        }
                                    }
                                    //wait for acks
                                    System.out.println("Now waiting for remove countdown latch : value " + removeLatch.getCount());

                                    removeLatch.await(timeout, TimeUnit.MILLISECONDS); //time out of base 1000
                                    System.out.println("Latch opened with value " + removeLatch.getCount());

                                    if (removeLatch.getCount() <= 0) {
                                        //find file and update state, also remove it from the index
                                        FileStateObject objToRem = null;
                                        for (FileStateObject fileObject : Controller.fileList) {
                                            if (fileObject.getFileName().equals(fileName)) {
                                                //updatestate
                                                System.out.println("Updating state of " + fileObject.getFileName() + " to removed");
                                                fileObject.setState("remove complete");

                                                System.out.println("State of " + fileObject.getFileName() + " now " + fileObject.getState());
                                                objToRem = fileObject; //removes from index

                                            }
                                        }
                                        out.println(Protocol.REMOVE_COMPLETE_TOKEN);
                                        //remove file from index
                                        fileList.remove(objToRem);
                                    } else {
                                        System.out.println("Remove ACKs timedout!, latch value: " + removeLatch.getCount());
                                    }
                                    //increase latch value
                                    Controller.removeLatch = new CountDownLatch(R);
                                } else {
                                    System.out.println(fileName + " does not exist!");
                                    out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                }
                            }
                        }
                        System.out.println("Remove finished");
                    } else if (line.contains("LIST")) {
                        System.out.println("Client wants a list of files: ");
                        if (dSPorts.size() < R) {
                            System.out.println("Not enough datastores");
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        } else {
                            //get list of files from index
                            String message = "";
                            for (FileStateObject fileObject : fileList) {
                                if(fileObject.getState().equals("store complete")){
                                    message = message + " " + fileObject.getFileName();
                                }
                            }
                            System.out.println("Sending" + message);
                            out.println(Protocol.LIST_TOKEN + message);
                        }
                    }
                    else {
                        System.out.println("Nothing special with this line");
                    }
                    System.out.println("Operation Served. Now Performing Next operation.");
                }
                System.out.println("Closing client: " + client.getPort());
                Integer toRemove = 0;
                PortListObject remObj = null;
                //find if the client port has a realting datastoreport
                for(PortListObject pobj : portList){
                    if(pobj.getcPort() == client.getPort()){
                        System.out.println("Relating port");
                        toRemove = pobj.getdPort();
                        remObj = pobj;
                    }else{
                        System.out.println("Dropping client");
                    }
                }
                Boolean dropped = dSPorts.remove(toRemove);
                portList.remove(remObj);

                if(dropped){
                    System.out.println("Datastore Dropped " + remObj.getdPort());
                }
                listAvailablePorts();
                client.close();
            } catch (Exception e) {
                System.err.println("error: " + e);
            }
        }

        public void listAvailablePorts(){
            try{
                System.out.println();
                System.out.print("Ports: ");
                for(int port : dSPorts){
                    System.out.print(port + "," );

                }
                System.out.println();
            }catch (Exception e){
                System.out.println("Error while listing ports");
                e.printStackTrace();
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
