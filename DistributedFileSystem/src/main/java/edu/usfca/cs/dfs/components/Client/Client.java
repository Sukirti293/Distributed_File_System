package edu.usfca.cs.dfs.components.Client;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import edu.usfca.cs.chat.ChatMessages;
import edu.usfca.cs.dfs.components.Compiler;
import edu.usfca.cs.dfs.components.DataStructure.SocketConnection;
import edu.usfca.cs.dfs.components.DataStructure.FileChunks;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;


//public class Client extends SimpleChannelInboundHandler<ChatMessages.MessageWrapper> {
public class Client {
    private static final Random random = new Random();
    private Set<ChatMessages.StorageNode> connectedStorageNodes = null;
    private static SocketConnection socketConnection = null;
    private String serverHost;
    private int serverPort;
    private static Socket clientSocket = null;
    private static Map<String, Long> uploadedFileSize = new HashMap<>();
    private static final Logger logger = Logger.getLogger(Client.class);
    //TODO create a proto buf

    public Client(String host, int port) {
        this.serverHost = host;
        this.serverPort = port;
    }


    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        if (args.length < 2) {
            System.err.println("Arguments: Client controller-host controller-port fileToSend");
            System.exit(1);
        }

        System.out.println("Menu \n 1. To Get All The Storage Node: getAllStorageNodes" +
                "\n 2. To see stored files names: getAllFileNames" +
                "\n 3. To see stored files names and their information: getAllFileInfo" +
                "\n 4. To upload a file: uploadFile <filename>" +
                "\n 5. To download a file: downloadFile <filename>" +
                "\n 6. To free the space: free-space"
        );

        Client client = new Client(args[0], Integer.parseInt(args[1]));
        client.connect(client);

    }

    public void connect(Client client) {
        try {
            clientSocket = new Socket(serverHost, serverPort);
            socketConnection = new SocketConnection(serverHost, serverPort);

            InputReader reader = new InputReader(client);
            Thread inputThread = new Thread(reader);
            inputThread.run();
        } catch (Exception ex) {
            System.err.println("Not able to connect with controller " + ex.toString());
        }

    }

    private static class InputReader implements Runnable {
        private final Client client;

        public InputReader(Client client) {
            this.client = client;
        }

        public void run() {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                String line = "";
                try {
                    line = reader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
                try {
                    client.decodeCommands(line);
                    logger.info("\n--------------------------------------------------------------------------------------------------------------------------");
                    logger.info("Menu \n 1. To Get All The Storage Node: getAllStorageNodes" +
                            "\n 2. To see stored files names: getAllFileNames" +
                            "\n 3. To see stored files names and their information: getAllFileInfo" +
                            "\n 4. To upload a file: uploadFile <filename>" +
                            "\n 5. To download a file: downloadFile <filename>" +
                            "\n 6. To free the space: freeSpace"
                    );
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * @param line - Command line entry to check which instruction should be executed
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws IOException
     */
    private void decodeCommands(String line) throws InterruptedException, ExecutionException, IOException {
        String filename = "";
        if (line != null || !line.isEmpty()) {
            if (line.contains(" ")) {
                String[] vals = line.split(" ");
                if (vals.length < 2) {
                    logger.info("Please provide a filename to upload");
                }
                line = vals[0];
                filename = vals[1];
            }
            switch (line) {
                case "getAllStorageNodes":
                    getAllStorageNodes();
                    break;
                case "getAllFileNames":
                    getAllStoredFilesFromSN();
                    break;
                case "getAllFileInfo":
                    getAllStoredFileInfoFromSN();
                    break;
                case "uploadFile":
                    if (filename.isEmpty())
                        logger.info("Please provide a filename to upload");
                    else {
                        logger.info("File to upload is: " + filename);
                        uploadFile(filename);
                    }
                    break;
                case "downloadFile":
                    downloadFile(socketConnection, filename);
                    break;
                case "freeSpace":
                    freeSpace();
                    break;
                default:
                    logger.info("Invalid Input");
                    break;
            }
        }
    }

    /**
     * Method to get all the online storage nodes
     */
    private void getAllStorageNodes() {
        try {
            //Client is asking controller for all the storage node info
            ChatMessages.GetStorageNodesRequest storageNodesRequestMsg = ChatMessages.GetStorageNodesRequest
                    .newBuilder()
                    .setStorageNodeRequestMessage(socketConnection.toString())
                    .build();
            ChatMessages.MessageWrapper sentMsgWrapper = ChatMessages.MessageWrapper.newBuilder()
                    .setGetStoragesNodesRequestMsg(storageNodesRequestMsg)
                    .build();
            sentMsgWrapper.writeDelimitedTo(clientSocket.getOutputStream());

            //Read response from controller
            logger.info("waiting for controller to send output");
            ChatMessages.MessageWrapper receivedMsgWrapper = ChatMessages.MessageWrapper.parseDelimitedFrom(clientSocket.getInputStream());
            if (!receivedMsgWrapper.hasGetStorageNodesResponseMsg()) {
                throw new UnsupportedOperationException("Expected storage node list response, but got something else.");
            }
            ChatMessages.GetStorageNodesResponse responseMsg = receivedMsgWrapper.getGetStorageNodesResponseMsg();

            //get all the storage nodes from the chatMessage response
            List<ChatMessages.StorageNode> storageNodeList = responseMsg.getNodesList();
            connectedStorageNodes = new HashSet<ChatMessages.StorageNode>(storageNodeList.size());
            Set<ChatMessages.StorageNode> storageNodeSet = new HashSet<>(storageNodeList);
            if (storageNodeSet.isEmpty()) {
                logger.info("No storage nodes are available");
            } else {
                for (ChatMessages.StorageNode storageNode : storageNodeSet) {
                    connectedStorageNodes.add(storageNode);
                    logger.info("Host :: " + storageNode.getHost() + " Port :: " + storageNode.getPort());
                }
                connectedStorageNodes.addAll(storageNodeSet);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    /**
     * This method is used to get all the file names from all the storage nodes
     */
    private void getAllStoredFilesFromSN() {
        try {
            //Sending a request message to controller
            ChatMessages.MessageWrapper.newBuilder()
                    .setGetFilesRequestMsg(ChatMessages.GetFilesRequest.newBuilder().build())
                    .build()
                    .writeDelimitedTo(clientSocket.getOutputStream());

            //Get response from controller
            ChatMessages.MessageWrapper msgWrapper = ChatMessages.MessageWrapper.parseDelimitedFrom(clientSocket.getInputStream());
            if (!msgWrapper.hasGetFilesResponseMsg()) {
                throw new IllegalStateException("Expected GetFilesResponse message, got: " + msgWrapper);
            }
            ChatMessages.GetFilesResponse msg = msgWrapper.getGetFilesResponseMsg();

            if (msg.getFilesList().isEmpty()) {
                logger.info("No files available");
            }

            for (ChatMessages.DownloadFileResponse downloadFileResponse : msg.getFilesList()) {
                String filename = downloadFileResponse.getFilename();
                logger.info("File stored in DFS :: "+filename);
            }
        } catch (Exception exception) {
            logger.error("Exception while getting the flies stored in the SNs " + exception.toString());
        }

    }

    /**
     * This method is used to get all the file names and corresponding information from all the storage nodes
     */
    private void getAllStoredFileInfoFromSN() {
        try {

            Set<Integer> missingChunks = new TreeSet<>();
            //Sending a request message to controller
            ChatMessages.MessageWrapper.newBuilder()
                    .setGetFilesRequestMsg(ChatMessages.GetFilesRequest.newBuilder().build())
                    .build()
                    .writeDelimitedTo(clientSocket.getOutputStream());

            //Get response from controller
            ChatMessages.MessageWrapper msgWrapper = ChatMessages.MessageWrapper.parseDelimitedFrom(clientSocket.getInputStream());
            if (!msgWrapper.hasGetFilesResponseMsg()) {
                throw new IllegalStateException("Expected GetFilesResponse message, got: " + msgWrapper);
            }
            ChatMessages.GetFilesResponse msg = msgWrapper.getGetFilesResponseMsg();

            if (msg.getFilesList().isEmpty()) {
                logger.info("No files available");
            }else{
                for (ChatMessages.DownloadFileResponse downloadFileResponse : msg.getFilesList()) {
                    String filename = downloadFileResponse.getFilename();
                    logger.info("Filename: " + filename);
                    for (ChatMessages.DownloadFileResponse.ChunkLocation chunkLocation : downloadFileResponse.getChunkLocationsList()) {
                        //logger.info("Chunk-", chunkLocation.getSequenceNo() + " stored at");
                        logger.info(String.format("Chunk #%02d at ", chunkLocation.getSequenceNo()));
                        SortedSet<SocketConnection> storageNodes = new TreeSet<>();
                        for (ChatMessages.StorageNode msgStorageNode : chunkLocation.getStorageNodesList()) {
                            //storageNodes.add(new SocketConnection(msgStorageNode.getHost(), msgStorageNode.getPort()));
                            logger.info(msgStorageNode.getHost());
                        }
                        //logger.info("List of Storage Nodes: " + storageNodes);
                        //logger.info(storageNodes);
                    }
                }
            }

        } catch (Exception exception) {
            logger.error("Exception while getting the flies stored in the SNs " + exception.toString());
        }
    }

    /**
     * @param filename - Filename to upload
     *                 This method is used to upload the files and store it in SN.
     *                 Random SN is assigned
     *                 Chunks are created
     *                 chunks are allocated to random SN
     *                 Connecting the client to storage node socket to store the data in the SN
     */
    private void uploadFile(String filename) {
        ChatMessages.StorageNode recentSN;
        getAllStorageNodes();
        try {
            if (connectedStorageNodes == null) {
                logger.error("No storage nodes are available, Can't upload in a empty system");
                System.exit(1);
            }

            if (!Compiler.checkFileIfExists(filename)) {
                logger.error("File to upload doesn't exists");
                System.exit(1);
            }
//            else {
//
//                long value = new File(filename).length();
//                uploadedFileSize.put(filename, value);
//            }

            //random SN assigned
            int storageNodeValue = random.nextInt(connectedStorageNodes.size());
            int nbStorageNodes = connectedStorageNodes.size();
//            FileChunks[] chunks = FileChunks.createChunks(
//                    filename,
//                    1000000,
//                    Compiler.clientChunkFolder);
            String storageDirectory = "";
            try{
                Path c = Paths.get(Compiler.clientChunkFolder);
                //System.out.println(c);
                Path root = c.getRoot();
                //System.out.println(root);
                storageDirectory = String.valueOf(root.resolve(Compiler.clientChunkFolder));
            }catch (Exception ex){
                ex.printStackTrace();
            }


            FileChunks[] chunks = FileChunks.createChunks(
                    filename,
                    10485760,
                    storageDirectory);

            //allocating chunks to storage node
            for (FileChunks chunk : chunks) {
                storageNodeValue = (storageNodeValue + 1) % nbStorageNodes;
                //logger.info(" Chunk value " + chunk.getSeqNumber() + " to be stored in SN: " + storageNodeValue);
                recentSN = (ChatMessages.StorageNode) connectedStorageNodes.toArray()[storageNodeValue];

                //Connecting the client to storage node socket;
                String hostName = recentSN.getHost();
                if (recentSN.getHost().contains(".")) {
                    String[] vals = recentSN.getHost().split("\\.");
                    if (vals.length > 1) {
                        hostName = vals[0];
                    }
                }
                Socket clientToSN = new Socket(hostName, recentSN.getPort());

                File chunkFile = chunk.getChunkLocalPath().toFile();
                FileInputStream fis = new FileInputStream(chunkFile);
                ByteString data = ByteString.readFrom(fis);
                fis.close();

                ChatMessages.MessageWrapper chatWrapper = ChatMessages.MessageWrapper.newBuilder()
                        .setStoreChunkMsgRequest(
                                ChatMessages.StoreChunk.newBuilder()
                                        .setFileName(chunk.getFileName())
                                        .setSequenceNo(chunk.getSeqNumber())
                                        .setChecksum(chunk.getChecksum())
                                        .setData(data)
                                        .build()
                        )
                        .build();

                chatWrapper.writeDelimitedTo(clientToSN.getOutputStream());

                logger.info("Successfully stored chunk-" + chunk.getSeqNumber() + " on the storageNode " + recentSN.getHost());

                if (!chunkFile.delete()) {
                    System.out.println("Chunk file " + chunkFile.getName() + " could not be deleted.");
                }
                clientToSN.close();
            }
        } catch (Exception exception) {
            logger.error("Exception occurred " + exception.toString());
            exception.printStackTrace();
        }
        logger.info("Upload DONE !!");
    }

    /**
     * @param socketConnection - address to the SN
     * @param filename         - filename
     * @throws IOException          -
     * @throws ExecutionException   -
     * @throws InterruptedException -
     *                              Method used to download an entire file
     */
    private static void downloadFile(SocketConnection socketConnection, String filename) throws IOException, ExecutionException, InterruptedException {
        Set<Integer> missingSeqNo = new TreeSet<>();
        Socket controllerSocket = socketConnection.getSocket();
        logger.info("Asking controller " + socketConnection + " about file " + filename);
        sendDownloadFileMsg(filename, controllerSocket);

        ChatMessages.DownloadFileResponse downloadFileResponseMsg = receiveDownloadFileResponse(controllerSocket);
        List<ChatMessages.DownloadFileResponse.ChunkLocation> allLocations = downloadFileResponseMsg.getChunkLocationsList();

//        Set<Integer> seqSet = new TreeSet<>();
//        for (int i = 0; i < allLocations.size(); i++) {
//            seqSet.add(allLocations.get(i).getSequenceNo());
//        }
//        for (int i = 0; i < FileChunks.totalChunkCount.get(filename); i++) {
//            if (!seqSet.contains(i + 1)) {
//                missingSeqNo.add(i + 1);
//            }
//        }

        SortedSet<FileChunks> chunks = downloadChunks(filename, downloadFileResponseMsg, missingSeqNo);

        if (chunks.size() == 0) {
            logger.error("Received chunk size as zero");
        }
        File file = FileChunks.createFile(chunks, filename);
        long bytes = Files.size(file.toPath());
        double megabytes = bytes / 1e6;
        megabytes = convertToDecimal(megabytes); // round to two decimals
        logger.info("The File Size, created from downloaded chunks : " + megabytes + " MB");

//        if (uploadedFileSize.get(filename) == file.length()) {
//            logger.info("Hurray!!!!!! got the same file that you have uploaded");
//        } else {
//            logger.error("Error in the downloaded file the, downloaded filesize : " + file.length() + " where as uploaded filesize is : " + uploadedFileSize);
//        }

        // Cleanup
        logger.debug("Deleting all chunks from local filesystem");
        for (FileChunks chunk : chunks) {
            chunk.getChunkLocalPath().toFile().delete();
        }
    }

    private static void sendDownloadFileMsg(String filename, Socket controllerSocket) throws IOException {
        ChatMessages.MessageWrapper msg = ChatMessages.MessageWrapper.newBuilder()
                .setDownloadFileMsg(
                        ChatMessages.DownloadFile.newBuilder()
                                .setFileName(filename)
                                .build()
                )
                .build();
        msg.writeDelimitedTo(controllerSocket.getOutputStream());
    }

    private static ChatMessages.DownloadFileResponse receiveDownloadFileResponse(Socket controllerSocket) throws IOException {
        logger.info("Enter receiveDownloadFileResponse");
        ChatMessages.MessageWrapper msgWrapper = ChatMessages.MessageWrapper.parseDelimitedFrom(controllerSocket.getInputStream());
        if (!msgWrapper.hasDownloadFileResponseMsg()) {
            throw new IllegalStateException("Controller is supposed to give back the DownloadFileResponse but got " + msgWrapper);
        }
        logger.info("return getDownloadFileResponseMsg");
        return msgWrapper.getDownloadFileResponseMsg();
    }


    private static SortedSet<FileChunks> downloadChunks(String filename, ChatMessages.DownloadFileResponse downloadFileResponseMsg,
                                                        Set<Integer> missingSeqSet) {
//        FileChunks missingFileChunks = null;
//        try{
//
//            if(!missingSeqSet.isEmpty()){
//                ChatMessages.MissingSeqNo missingSeqNo = ChatMessages.MissingSeqNo.newBuilder().setFilename(filename).addAllSequenceNo(missingSeqSet).build();
//                ChatMessages.MessageWrapper messageWrapper = ChatMessages.MessageWrapper.newBuilder()
//                        .setGetMissingSeqNo(missingSeqNo).build();
//                messageWrapper.writeDelimitedTo(clientSocket.getOutputStream());
//
//                logger.info("Waiting for response from controller to get the missing chunks");
//                ChatMessages.MessageWrapper msgWrapper = ChatMessages.MessageWrapper.parseDelimitedFrom(clientSocket.getInputStream());
//                if (msgWrapper.hasErrorMsg()) {
//                    throw new IllegalStateException("Controller is supposed to give back the Missing Files with chunk but got " + messageWrapper);
//                }
//
//                missingFileChunks = processStoreChunkMsg(msgWrapper);
//
//            }
//
//        }catch (Exception exception){
//            exception.printStackTrace();
//        }

        int nThreads = Compiler.threadsInExecutor;
        logger.info("Started thread pool");
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        int taskCounter = 0;

        List<DownloadChunkTask> tasks = new ArrayList<>();

        Map<Integer, List<SocketConnection>> chunkLocations = parseChunkLocations(downloadFileResponseMsg);
        for (Map.Entry<Integer, List<SocketConnection>> entry : chunkLocations.entrySet()) {
            int sequenceNo = entry.getKey();
            List<SocketConnection> nodes = entry.getValue();
            DownloadChunkTask task = new DownloadChunkTask(filename, sequenceNo, nodes);
            taskCounter = taskCounter + 1;
            tasks.add(task);
            executor.submit(task);
        }

        if (tasks.size() != taskCounter) {
            logger.error("tasks size is :: " + tasks.size() + " where as task counter value is " + taskCounter);
        }
        SortedSet<FileChunks> chunks = new TreeSet<>();

        try {
            logger.info("Waiting for all " + tasks.size() + " download tasks to finish...");
            List<Future<FileChunks>> futures = executor.invokeAll(tasks);
            int index = 0;
            for (int sequenceNo : chunkLocations.keySet()) {
                chunks.add(futures.get(index).get());
                index++;
            }

        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            try {
                logger.info("Inside finally block ");
                try {
                    logger.info("Attempting to shutdown executor");
                    executor.shutdown();
                    executor.awaitTermination(5, TimeUnit.SECONDS);
                } finally {
                    if (!executor.isTerminated()) {
                        logger.error("Some tasks didn't finish.");
                    }
                    executor.shutdownNow();
                    logger.info("ExecutorService shutdown finished.");
                }
            } catch (Exception exception) {
                exception.printStackTrace();
            } finally {
                logger.info("Finally block is DONE!!!!!!!!!!");
            }
        }

//        if(missingFileChunks != null){
//            logger.info("Missing :: before adding missing chunks the chunk size "+chunks.size());
//            chunks.add(missingFileChunks);
//            logger.info("Missing :: after adding missing chunks the chunk size "+chunks.size());
//        }

        return chunks;
    }

    private static Map<Integer, List<SocketConnection>> parseChunkLocations(ChatMessages.DownloadFileResponse downloadFileResponseMsg) {
        Map<Integer, List<SocketConnection>> result = new HashMap<>();
        for (ChatMessages.DownloadFileResponse.ChunkLocation chunkLocation : downloadFileResponseMsg.getChunkLocationsList()) {
            List<SocketConnection> nodes = new ArrayList<>();
            for (ChatMessages.StorageNode node : chunkLocation.getStorageNodesList()) {
                nodes.add(new SocketConnection(node.getHost(), node.getPort()));
            }
            result.put(chunkLocation.getSequenceNo(), nodes);
        }
        return result;
    }

    private static FileChunks downloadChunk(String filename, int sequenceNo, Socket socket) throws Exception {
        sendDownloadChunkRequest(filename, sequenceNo, socket);

        ChatMessages.MessageWrapper msgWrapper = ChatMessages.MessageWrapper.parseDelimitedFrom(socket.getInputStream());
        if (!msgWrapper.hasStoreChunkMsgRequest()) {
            throw new IllegalStateException("Response to DownloadChunk should have been StoreChunk. Got: " + TextFormat.printToString(msgWrapper));
        }

        return processStoreChunkMsg(msgWrapper);
    }

    private static void sendDownloadChunkRequest(String filename, int sequenceNo, Socket socket) throws IOException {
        ChatMessages.MessageWrapper requestMsg = ChatMessages.MessageWrapper.newBuilder()
                .setDownloadChunkMsg(
                        ChatMessages.DownloadChunk.newBuilder()
                                .setFilename(filename)
                                .setSequenceNo(sequenceNo)
                                .build()
                )
                .build();
        requestMsg.writeDelimitedTo(socket.getOutputStream());
    }

    private static FileChunks processStoreChunkMsg(ChatMessages.MessageWrapper msgWrapper) throws Exception {

        String expectedChecksum = "";
        ChatMessages.StoreChunk storeChunkMsg
                = msgWrapper.getStoreChunkMsgRequest();
        expectedChecksum = storeChunkMsg.getChecksum();

        logger.info("Checksum for " + storeChunkMsg.getSequenceNo() + " is " + expectedChecksum);

        //String storageDirectory = Paths.get("").toAbsolutePath().toString() + Compiler.clientChunkFolder;
        //String storageDirectory = String.valueOf(Paths.get(Compiler.clientChunkFolder));

        Path c = Paths.get(Compiler.clientChunkFolder);
        //System.out.println(c);
        Path root = c.getRoot();
        //System.out.println(root);
        String storageDirectory = String.valueOf(root.resolve(Compiler.clientChunkFolder));
        System.out.println(storageDirectory);


        File storageDirectoryFile = new File(String.valueOf(storageDirectory));
        if (!storageDirectoryFile.exists()) {
            if (!storageDirectoryFile.mkdir()) {
                logger.error("Could not create storage directory.");
            }
        }

        // Store chunk file
        String chunkFilename = storeChunkMsg.getFileName() + "-chunk" + storeChunkMsg.getSequenceNo();
        Path chunkFilePath = Paths.get(storageDirectory, chunkFilename);
        File chunkFile = chunkFilePath.toFile();
        if (chunkFile.exists()) {
            if (!chunkFile.delete()) {
                throw new RuntimeException("Unable to delete existing file before overwriting");
            }
        }
        logger.debug("Storing to file " + chunkFilePath);
        FileOutputStream fos = new FileOutputStream(chunkFile);
        storeChunkMsg.getData().writeTo(fos);
        fos.close();

        String actualChecksum = FileChunks.md5sum(chunkFile);
        if (actualChecksum == null || !actualChecksum.equals(expectedChecksum)) {
            logger.error("actualChecksum val :: " + actualChecksum + "\nexpectedChecksum val " + expectedChecksum);
            throw new Exception("Either error in creating Checksum or actual checksum and expected checksum are not same");
        }

        return new FileChunks(storeChunkMsg.getFileName(), storeChunkMsg.getSequenceNo(), Files.size(chunkFilePath), expectedChecksum, chunkFilePath);
    }

    private static class DownloadChunkTask implements Callable<FileChunks> {

        private final String filename;
        private final int sequenceNo;
        private final List<SocketConnection> storageNodes;

        public DownloadChunkTask(String filename, int sequenceNo, List<SocketConnection> storageNodes) {
            this.filename = filename;
            this.sequenceNo = sequenceNo;
            this.storageNodes = storageNodes;
        }

        @Override
        public FileChunks call() throws Exception {
            for (SocketConnection storageNode : storageNodes) {
                try {
                    //here need to check fault tolerent mechanism
                    //I gues sit is not looping through all the replicas
                    return downloadChunk(filename, sequenceNo, storageNode.getSocket());
                } catch (ConnectException ce) {
                    ce.printStackTrace();
                    logger.error("Error " + ce);
                }
            }
            throw new ConnectException("Couldn't retrieve one good chunk (correct checksum) or connect to any of: " + storageNodes);
        }
    }

    /**
     * Asks the controller for freespace
     */
    private void freeSpace() {
        try {
            ChatMessages.MessageWrapper.newBuilder()
                    .setGetFreeSpaceRequestMsg(ChatMessages.GetFreeSpaceRequest.newBuilder().build())
                    .build()
                    .writeDelimitedTo(clientSocket.getOutputStream());

            ChatMessages.MessageWrapper responseMsgWrapper = ChatMessages.MessageWrapper.parseDelimitedFrom(clientSocket.getInputStream());
            if (!responseMsgWrapper.hasGetFreeSpaceResponseMsg()) {
                throw new IllegalStateException("Expected get free space response message, but got: " + responseMsgWrapper);
            }

            ChatMessages.GetFreeSpaceResponse msg = responseMsgWrapper.getGetFreeSpaceResponseMsg();
            long freeSpace = msg.getFreeSpace();
            long totalSpace = msg.getTotalSpace();

            //logger.info("FreeSpace in DFS :: " + freeSpace );
//            double gigabytesFreeSpace = freeSpace / 1e9;
//            double gigabytesTotalSpace = totalSpace / 1e9;
//
//            gigabytesFreeSpace = gigabytesTotalSpace - gigabytesFreeSpace;
//            gigabytesFreeSpace = convertToDecimal(gigabytesFreeSpace);
//            gigabytesTotalSpace = convertToDecimal(gigabytesTotalSpace);

            //logger.info("gigabytes " + gigabytes);
            logger.info("Free space on DFS: " + Compiler.byteToHumanFormat(totalSpace - freeSpace));
            logger.info("Total space on DFS: " + Compiler.byteToHumanFormat( totalSpace));
        } catch (Exception ex) {
            ex.printStackTrace();
        }


    }

    private static double convertToDecimal(double d) {
        return ((int) Math.round(100 * d)) / 100.0;
    }

}
