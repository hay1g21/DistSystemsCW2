import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * This is just an example of how to use the provided client library. You are expected to customise this class and/or
 * develop other client classes to test your Controller and Dstores thoroughly. You are not expected to include client
 * code in your submission.
 */
public class ClientMain {
    //A client: java Client cport timeout
    public static void main(String[] args) throws Exception {

        final int cport = Integer.parseInt(args[0]);
        int timeout = Integer.parseInt(args[1]);

        // this client expects a 'downloads' folder in the current directory; all files loaded from the store will be stored in this folder
        File downloadFolder = new File("downloads");
        if (!downloadFolder.exists())
            if (!downloadFolder.mkdir())
                throw new RuntimeException("Cannot create download folder (folder absolute path: " + downloadFolder.getAbsolutePath() + ")");

        // this client expects a 'to_store' folder in the current directory; all files to be stored in the store will be collected from this folder
        File uploadFolder = new File("clientto_store");
        if (!uploadFolder.exists())
            throw new RuntimeException("clientto_store folder does not exist");

        // launch a single client
        testClient4(cport, timeout, downloadFolder, uploadFolder);


		// launch a number of concurrent clients, each doing the same operations
        /*
		for (int i = 0; i < 10; i++) {
			new Thread() {
				public void run() {
					test2Client(cport, timeout, downloadFolder, uploadFolder);
				}
			}.start();
		}

         */
        //new Thread(() -> testClient(cport, timeout, downloadFolder, uploadFolder)).start();
        //try { Thread.sleep(1000); } catch(Exception e) { e.printStackTrace(); }
        //new Thread(() -> testClient4(cport, timeout, downloadFolder, uploadFolder)).start();
        //new Thread(() -> testClient3(cport, timeout, downloadFolder, uploadFolder)).start();
        //try { Thread.sleep(2000); } catch(Exception e) { e.printStackTrace(); }

        for(int i=0; i<5; i++){
            //new Thread(() -> testClient3(cport, timeout, downloadFolder, uploadFolder)).start();
        }







    }

    public static void test2Client(int cport, int timeout, File downloadFolder, File uploadFolder) {
        Client client = null;

        try {
            client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);
            client.connect();
            Random random = new Random(System.currentTimeMillis() * System.nanoTime());

            File fileList[] = uploadFolder.listFiles();
            for (int i = 0; i < fileList.length / 2; i++) {
                File fileToStore = fileList[random.nextInt(fileList.length)];
                try {
                    client.store(fileToStore);
                } catch (Exception e) {
                    System.out.println("Error storing file " + fileToStore);
                    e.printStackTrace();
                }
            }

            String list[] = null;
            try {
                list = list(client);
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (int i = 0; i < list.length / 4; i++) {
                String fileToRemove = list[random.nextInt(list.length)];
                try {
                    client.remove(fileToRemove);
                } catch (Exception e) {
                    System.out.println("Error remove file " + fileToRemove);
                    e.printStackTrace();
                }
            }

            try {
                list = list(client);
            } catch (IOException e) {
                e.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (client != null)
                try {
                    client.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }

    public static void testClient(int cport, int timeout, File downloadFolder, File uploadFolder) {
        Client client = null;

        try {

            client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

            try {
                client.connect();
                //client.send("Client I'm a client");
                File fileList[] = uploadFolder.listFiles();
                System.out.println();
                for(File file : fileList){
                    System.out.print(file.getName() + " ");
                }
                System.out.println();
                //System.out.println(fileList[2].getName());




                //try { client.store(fileList[2]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[3]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[4]); } catch(IOException e) { e.printStackTrace(); }

                //try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[2]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[3]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[4]); } catch(IOException e) { e.printStackTrace(); }

                //try { client.wrongStore(fileList[0].getName(), new byte[52]); } catch(IOException e) { e.printStackTrace(); }

                //try { client.wrongLoad(fileList[0].getName(), 3); } catch(IOException e) { e.printStackTrace(); }


                //put code here
                //try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[2]); } catch(IOException e) { e.printStackTrace(); }
                try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.wrongLoad(fileList[0].getName(), 3); } catch(IOException e) { e.printStackTrace(); }
                try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[2]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[3]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.load(fileList[4].getName(), downloadFolder); } catch(IOException e) { e.printStackTrace(); }
                //try { client.load(fileList[1].getName(), downloadFolder); } catch(IOException e) { e.printStackTrace(); }
                try { client.remove(fileList[0].getName()); } catch(IOException e) { e.printStackTrace(); }
                try { client.remove(fileList[1].getName()); } catch(IOException e) { e.printStackTrace(); }
                //try { client.load(fileList[0].getName(), downloadFolder); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[3]); } catch(IOException e) { e.printStackTrace(); }
                //System.out.println(downloadFolder.listFiles()[0]);
                //try { client.load(fileList[0].getName()); } catch(IOException e) { e.printStackTrace(); }
                //try { client.remove(fileList[0].getName()); } catch(IOException e) { e.printStackTrace(); }
                client.list();
                System.out.println("Finished");

            } catch (IOException e) {
                e.printStackTrace();
                return;
            }


			/*
			try { list(client); } catch(IOException e) { e.printStackTrace(); }
			
			// store first file in the to_store folder twice, then store second file in the to_store folder once
			File fileList[] = uploadFolder.listFiles();
			if (fileList.length > 0) {
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }				
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
			}
			if (fileList.length > 1) {
				try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
			}

			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }
			
			if (list != null)
				for (String filename : list)
					try { client.load(filename, downloadFolder); } catch(IOException e) { e.printStackTrace(); }
			
			if (list != null)
				for (String filename : list)
					try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
			if (list != null && list.length > 0)
				try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }
			
			try { list(client); } catch(IOException e) { e.printStackTrace(); }

			 */

        } finally {
            if (client != null)
                try {
                    client.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }

    public static void testClient4(int cport, int timeout, File downloadFolder, File uploadFolder) {
        Client client = null;

        try {

            client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

            try {
                client.connect();
                //client.send("Client I'm a client");
                File fileList[] = uploadFolder.listFiles();

                System.out.println("Finished");

                if (fileList.length > 0) {
                    try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
                    try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
                }
                if (fileList.length > 1) {
                    try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
                }

                String list[] = null;
                try { list = list(client); } catch(IOException e) { e.printStackTrace(); }

                if (list != null)
                    for (String filename : list)
                        try { client.load(filename, downloadFolder); } catch(IOException e) { e.printStackTrace(); }

                if (list != null)
                    for (String filename : list)
                        try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
                if (list != null && list.length > 0)
                    try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }

                try { list(client); } catch(IOException e) { e.printStackTrace(); }
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }


			/*
			try { list(client); } catch(IOException e) { e.printStackTrace(); }

			// store first file in the to_store folder twice, then store second file in the to_store folder once
			File fileList[] = uploadFolder.listFiles();
			if (fileList.length > 0) {
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
				try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
			}
			if (fileList.length > 1) {
				try { client.store(fileList[1]); } catch(IOException e) { e.printStackTrace(); }
			}

			String list[] = null;
			try { list = list(client); } catch(IOException e) { e.printStackTrace(); }

			if (list != null)
				for (String filename : list)
					try { client.load(filename, downloadFolder); } catch(IOException e) { e.printStackTrace(); }

			if (list != null)
				for (String filename : list)
					try { client.remove(filename); } catch(IOException e) { e.printStackTrace(); }
			if (list != null && list.length > 0)
				try { client.remove(list[0]); } catch(IOException e) { e.printStackTrace(); }

			try { list(client); } catch(IOException e) { e.printStackTrace(); }

			 */

        } finally {
            if (client != null)
                try {
                    client.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }


    public static void testClient3(int cport, int timeout, File downloadFolder, File uploadFolder) {
        Client client = null;

        try {

            client = new Client(cport, timeout, Logger.LoggingType.ON_FILE_AND_TERMINAL);

            try {
                int count = 0;
                int count2 = 0;
                client.connect();
                //client.send("Client I'm a client");
                File fileList[] = uploadFolder.listFiles();
                //System.out.println(fileList[0].getName());
                //System.out.println(fileList[1].getName());
                //System.out.println(fileList[2].getName());

                //try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.load(fileList[0].getName()); } catch(IOException e) { e.printStackTrace(); }

                //try { client.remove(fileList[0].getName()); } catch(IOException e) { e.printStackTrace(); }

                //client.list();

                //try { client.wrongStore(fileList[0].getName(), new byte[52]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.wrongLoad(fileList[0].getName(), 1); } catch(IOException e) { e.printStackTrace(); }


                //put code here
                //try { client.remove(fileList[4].getName()); } catch(IOException e) { e.printStackTrace(); }
                //try all storing
                try { client.store(fileList[0]); } catch(IOException e) { e.printStackTrace(); System.out.println("Failed to Upload"); count--; }
                //try { client.load(fileList[1].getName(), downloadFolder); } catch(IOException e) { e.printStackTrace(); }
                try { client.store(fileList[2]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[3]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.store(fileList[4]); } catch(IOException e) { e.printStackTrace();  count--; }

                //try { client.remove(fileList[0].getName()); } catch(IOException e) { e.printStackTrace(); count2--; }

                //try { client.store(fileList[2]); } catch(IOException e) { e.printStackTrace(); }
                //try { client.load(fileList[0].getName()); } catch(IOException e) { e.printStackTrace(); }


                //try { client.remove(fileList[1].getName()); } catch(IOException e) { e.printStackTrace(); count2--; }

                //System.out.println("Store Successful: " + count);
                System.out.println("Remove Successful: " + count2);
                System.out.println("Finished");

            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

        } finally {
            if (client != null)
                try {
                    client.disconnect();
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
    }

    public static String[] list(Client client) throws IOException, NotEnoughDstoresException {
        System.out.println("Retrieving list of files...");
        String list[] = client.list();

        System.out.println("Ok, " + list.length + " files:");
        int i = 0;
        for (String filename : list)
            System.out.println("[" + i++ + "] " + filename);

        return list;
    }

}
