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
    //static Vector<DataStoreThread> activeDataStores = new Vector<DataStoreThread>(); //holds active threads of datastores

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

        System.out.println("Controller started on port: " + cport + ". R = " + R);
        ServerSocket ss = null; //makes server socket
        //first wants datastores to join
        //at least R amount
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

    static class ReceiverThread implements Runnable {
        Socket client;


        int R;



        Integer previousPort; //for reloading
        Vector<Integer> triedPorts;
        ReceiverThread(Socket client, int R) {
            this.client = client;
            this.R = R;
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
                        //find corresponding latch
                        String filename = line.split(" ")[1];
                        FileStateObject storedFile = null;
                        //add port to fileobject
                        for (FileStateObject obj : fileList) {
                            if (obj.getFileName().equals(filename)) {
                                storedFile = obj;
                                //obj.addSocket(client); //add to the list of datastore sockets that is storing
                            }
                        }
                        if(storedFile != null){
                            CountDownLatch fileLatch = storedFile.getCountDownLatch();
                            System.out.println("Message has been successfully sent by dataStore, decrement ack from value: " + fileLatch.getCount());
                            fileLatch.countDown();
                            System.out.println("Latch now " + fileLatch.getCount());
                            storedFile.addSocket(client); //add to the list of datastore sockets that is storing
                        }else{
                            System.out.println("Getting store countdown latch failed");
                        }
                    } else if (line.contains("STORE")) {
                        System.out.println("Client wants to store file: ");
                        //parse message
                        String[] lines = line.split(" ");
                        String fileName = lines[1];
                        String size = lines[2];
                        System.out.println("Preparing to store " + fileName + " of size " + size);
                        //get R ports
                        String chosenPorts = "";
                        boolean exists = false;
                        //if not enough datastores send error message
                        if (dSPorts.size() < R) {
                            System.out.println("Not enough datastores");
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        } else {

                            //if file already exists return error
                            FileStateObject obj = null;
                            synchronized (this.getClass()) {
                                exists = false;
                                for (FileStateObject checkObj : fileList) {
                                    if (checkObj.getFileName().equals(fileName)) {
                                        //System.out.println(fileName + " Already exists in index");
                                        exists = true;
                                        break;
                                    }
                                    //System.out.println("Checking...");
                                }


                                if (exists) {
                                    System.out.println("Already exists");
                                    out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                                } else {
                                    System.out.println("File assumed to not exist! Proceeding with Store");
                                    //exists = false;
                                    //add file to index
                                    obj = new FileStateObject(fileName, Integer.parseInt(size), "store in progress");
                                    fileList.add(obj);
                                    ArrayList<Integer> countArray = new ArrayList<Integer>();
                                    //make list of how many files each is holding
                                    //calculate how many files each datastore should hold
                                    //RF/N
                                    int low = (int) Math.floor((float) R * (float) fileList.size() / (float) portList.size());
                                    int high = (int) Math.ceil((float) R * (float) fileList.size() / (float) portList.size());

                                    System.out.println("Datastores need to hold between " + low + "and " + high + " Files");

                                    for (FileStateObject file : fileList) {
                                        if (file.getPorts().size() > 0) {
                                            countArray.addAll(file.getPorts());
                                        }
                                    }
                                    System.out.println("Size of countarray: " + countArray.size());
                                    int rCount = 0;
                                    int index = 0;
                                    boolean repeat = false; //flag for checking any datastores
                                    boolean checkLow = true; //flag for checking datastores with low amount of file
                                    //boolean escape = false; //caution for breaking while loop
                                    while (rCount < R) {
                                        //datastore should store RF/N files - Below limit first, then normal, then any
                                        //choose and check
                                        //System.out.println("Checking " + dSPorts.get(index));
                                        int count = 0;
                                        //counts up how many files that port is storing
                                        for (int port : countArray) {
                                            if (dSPorts.get(index) == port) {
                                                count++;
                                            }
                                        }
                                        System.out.println(dSPorts.get(index) + " has " + count + " stored files");
                                        if (checkLow) {
                                            //Check for less than the limit. then add
                                            if (count < low) {
                                                //add this datastore if it hasnt repeated
                                                System.out.println("Below Limit. Available space, Adding to port " + dSPorts.get(index));
                                                chosenPorts = chosenPorts + " " + dSPorts.get(index);

                                                if (!obj.getPorts().contains(dSPorts.get(index))) {
                                                    obj.addPort(dSPorts.get(index));
                                                    rCount++;
                                                } else {
                                                    System.out.println("Repeating: Ignoring datastore");
                                                    //repeat = true;
                                                }
                                            }
                                        } else if (repeat) {
                                            //Check for any datastores
                                            //if there is a repeat in choosing ports, forcibly add a datastore
                                            System.out.println("Imbalance so Adding to port " + dSPorts.get(index));
                                            chosenPorts = chosenPorts + " " + dSPorts.get(index);
                                            if (!obj.getPorts().contains(dSPorts.get(index))) {
                                                obj.addPort(dSPorts.get(index));
                                                rCount++;
                                            } else {
                                                System.out.println("Repeating: Ignoring datastore");
                                                //repeat = true;
                                            }
                                        } else {
                                            //check normally
                                            if (count + 1 > high) {
                                                System.out.println("Too many files, choosing a different one");

                                            } else {
                                                System.out.println("Available space, Adding to port " + dSPorts.get(index));
                                                chosenPorts = chosenPorts + " " + dSPorts.get(index);
                                                if (!obj.getPorts().contains(dSPorts.get(index))) {
                                                    obj.addPort(dSPorts.get(index));
                                                    rCount++;
                                                } else {
                                                    System.out.println("Repeating: Ignoring datastore");
                                                    //repeat = true;
                                                }
                                            }
                                        }
                                        index = (index + 1) % dSPorts.size();
                                        if (index == 0 && rCount < R) {
                                            System.out.println("Loop around");
                                            if (checkLow) {
                                                System.out.println("No longer checking low files");
                                                checkLow = false;
                                            } else if (!checkLow) {
                                                System.out.println("No longer checking medium: Now checking any");
                                                repeat = true;
                                            } else if (!checkLow && repeat) {
                                                System.out.println("Breaking loop, something has gone wrong!");
                                                break;
                                            }

                                        }
                                        System.out.println("Index now" + index);
                                        //0
                                    }
                                }
                            }

                            //synchronization should end here
                            if(!exists) {
                                System.out.println(Protocol.STORE_TO_TOKEN + chosenPorts);
                                obj.setCountDownLatch(R);
                                CountDownLatch fileLatch = obj.getCountDownLatch();
                                out.println(Protocol.STORE_TO_TOKEN + chosenPorts);
                                //now wait for acknowlegements

                                //set the latch
                                System.out.println("Now waiting for countdown latch : value " + fileLatch.getCount());
                                fileLatch.await(timeout, TimeUnit.MILLISECONDS); //time out of base 1000
                                System.out.println("Latch opened with value " + fileLatch.getCount());
                                //check if receieved all acks. if not dont send message and remove file from index
                                if (fileLatch.getCount() <= 0) {
                                    System.out.println("All acks recieved");

                                    //find file and update state
                                    for (FileStateObject fileObject : fileList) {
                                        if (fileObject.getFileName().equals(fileName)) {
                                            //updatestate
                                            System.out.println("Updating state of " + fileObject.getFileName() + " to complete");
                                            fileObject.setState("store complete");
                                            System.out.println("State of " + fileObject.getFileName() + " now " + fileObject.getState());
                                            break;
                                        }
                                    }
                                    out.println(Protocol.STORE_COMPLETE_TOKEN);
                                } else {
                                    System.out.println("Latch timedout! Removing " + obj.getFileName());
                                    //remove file from index

                                    fileList.remove(obj);
                                    System.out.println("Removed");
                                }

                                //increase latch value
                                //Controller.latch = new CountDownLatch(R);
                                //System.out.println("Latch closed. Value of latch: " + fileLatch.getCount());
                                System.out.println("New size of stored files : " + fileList.size());
                            }


                        }
                    } else if (line.contains("LOAD") && !line.contains("RELOAD")) {
                        System.out.println("Client wants to load a file");
                        //parse message
                        String[] lines = line.split(" ");
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
                            boolean exists = false;
                            for (FileStateObject obj : fileList) {
                                if (obj.getFileName().equals(fileName) && obj.getState().equals("store complete")) {
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
                                Vector<Integer> availablePorts = new Vector<Integer>(fileObj.getPorts());
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
                        String[] lines = line.split(" ");
                        String fileName = lines[1];
                        //prepare size
                        int size = 0;
                        if (dSPorts.size() < R) {
                            System.out.println("Not enough datastores");
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        } else {
                            System.out.println("Preparing to reload " + fileName);
                            //get R ports
                            String chosenPorts = "";
                            //remove port from tried ports
                            System.out.println("Size of listports " + dSPorts.size());
                            if(triedPorts.size() > 0){
                                //System.out.println("REMOVING");
                                //System.out.println("Tried port : " + triedPorts.get(0));
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
                                    if (obj.getFileName().equals(fileName) && obj.getState().equals("store complete")) {
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
                        }
                    } //datastore stuff
                    else if (line.contains("REMOVE_ACK")) {
                        String filename = line.split(" ")[1];
                        //get latch from object
                        FileStateObject removedFile = null;

                        for (FileStateObject obj : fileList) {
                            //System.out.println("Does " + obj.fileName + " equal " + filename);
                            if (obj.getFileName().equals(filename)) {
                                removedFile = obj;
                            }
                        }
                        if(removedFile != null){
                            CountDownLatch removedFileLatch = removedFile.getRemCountDownLatch();
                            //countdown the latch
                            System.out.println("Message has been sent, decrement remove ack from value: " +removedFileLatch.getCount());
                            removedFileLatch.countDown();

                            System.out.println("Latch now " + removedFileLatch.getCount());


                            //Controller.removeLatch = new CountDownLatch(R);
                            System.out.println("*Decrement complete*");
                        }else{
                            System.out.println("Error during getting Remove countdown latch");
                        }
                    }
                    else if (line.contains("REMOVE")) {
                        System.out.println("Client wants to remove files");
                        String[] lines = line.split(" ");
                        String fileName = lines[1];

                        if(dSPorts.size() < R){
                            System.out.println("Not enough datastores");
                            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        }else {
                            Vector<Socket> removePorts = null;
                            FileStateObject fileObj = null;
                            boolean exists = false;
                            synchronized (this.getClass()) {
                                System.out.println("Preparing to remove " + fileName);

                                for (FileStateObject obj : fileList) {
                                    if (obj.getFileName().equals(fileName) && obj.getState().equals("store complete")) {
                                        System.out.println("Setting state " + obj.getState() + " to remove");
                                        obj.setState("remove in progress");
                                        removePorts = obj.getSockets(); //gets list of ports holding the file
                                        fileObj = obj;
                                        exists = true;
                                    }
                                }
                                //end synchronisation here
                            }
                                if (exists) {
                                    //gets datastores storing file
                                    fileObj.setRemCountDownLatch(R);
                                    CountDownLatch fileRemoveLatch = fileObj.getRemCountDownLatch();
                                    System.out.println("Now waiting for remove countdown latch : value " + fileRemoveLatch.getCount());
                                    for (Socket dataStore : removePorts) {
                                        //may bug out here
                                        if (!dataStore.isClosed()) {
                                            PrintWriter outC = new PrintWriter(dataStore.getOutputStream(), true);
                                            //send remove messgae

                                            outC.println(Protocol.REMOVE_TOKEN + " " + fileObj.getFileName());
                                        }
                                    }
                                    //wait for acks
                                    //get objects countdown latch
                                    fileRemoveLatch.await(timeout, TimeUnit.MILLISECONDS); //time out of base 1000
                                    System.out.println("Latch opened with value " + fileRemoveLatch.getCount());

                                    if (fileRemoveLatch.getCount() <= 0) {
                                        synchronized (this.getClass()){
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
                                            //remove file from index
                                            fileList.remove(objToRem);
                                        }
                                        out.println(Protocol.REMOVE_COMPLETE_TOKEN);
                                        System.out.println("Remove finished");
                                    } else {
                                        System.out.println("Remove ACKs timedout!, latch value: " + fileRemoveLatch.getCount());
                                    }
                                    //increase latch value
                                    //Controller.removeLatch = new CountDownLatch(R);
                                } else {
                                    System.out.println(fileName + " does not exist!");
                                    out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                    System.out.println("Remove Stopped");
                                }
                        }
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
                    }
                }
                //remove from list of ports and list of port objects
                boolean dropped = dSPorts.remove(toRemove);
                portList.remove(remObj);

                if(dropped){
                    if(remObj != null){
                        System.out.println("Datastore Dropped " + remObj.getdPort());
                    }
                }
                System.out.println("Dropping client");
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
