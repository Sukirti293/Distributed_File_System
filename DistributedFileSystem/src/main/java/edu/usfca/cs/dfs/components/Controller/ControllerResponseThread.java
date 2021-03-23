package edu.usfca.cs.dfs.components.Controller;

import edu.usfca.cs.chat.ChatMessages;
import edu.usfca.cs.chat.MessageQueue;
import edu.usfca.cs.chat.SemaphoreLocking;
import edu.usfca.cs.dfs.components.DataStructure.ChunkStoredInNode;
import edu.usfca.cs.dfs.components.DataStructure.SocketConnection;
import edu.usfca.cs.dfs.components.filterAndStorage.BloomFilter;
import edu.usfca.cs.dfs.components.filterAndStorage.DataStorage;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class ControllerResponseThread implements Runnable {
    private final Set<SocketConnection> storageNodes;
    private final Map<SocketConnection, Date> heartbeats;
    private Socket socket;
    private final Map<SocketConnection, MessageQueue> messageQueues;
    private final DataStorage dataStorage;
    private final Set<SocketConnection> visitedStorageNodes = new HashSet<>();
    private static final Logger logger = Logger.getLogger(ControllerResponseThread.class);
    private SemaphoreLocking semaphoreLocking;

    public ControllerResponseThread(SemaphoreLocking semaphoreLocking, Set<SocketConnection> storageNodes,
                                    Map<SocketConnection, Date> heartbeats, Map<SocketConnection, MessageQueue> messageQueues, DataStorage dataStorage, Socket socket) {
        this.storageNodes = storageNodes;
        this.heartbeats = heartbeats;
        this.socket = socket;
        this.messageQueues = messageQueues;
        this.dataStorage = dataStorage;
        this.semaphoreLocking = semaphoreLocking;
        BasicConfigurator.configure();
    }


    /**
     * checks the InputStream from storageNode
     * chceks whether it is null or have values
     */
    @Override
    public void run() {
        int notRespondingInterval = 0;
        while (socket.isConnected()) {
            System.out.println("Controller is connected .....");
            try {
                ChatMessages.MessageWrapper msgWrapper = ChatMessages.MessageWrapper.parseDelimitedFrom(socket.getInputStream());

                if (msgWrapper == null) {
                    System.out.println("No message coming from " + socket.getRemoteSocketAddress());
                    notRespondingInterval++;
                    if (notRespondingInterval == 30) {
                        System.out.println("It's been too long not receiving any message  " + socket.getRemoteSocketAddress()
                                + "\n closing the storageNode socket stream");
                        socket.close();
                        return;
                    } else {
                        continue;
                    }
                }

                if (msgWrapper.hasHeartbeatMsg()) {
                    heartbeatRequestMsg(socket, msgWrapper);
                } else if (msgWrapper.hasGetStoragesNodesRequestMsg()) {
                    getStorageNodesRequestMsg(socket);
                } else if (msgWrapper.hasGetFilesRequestMsg()) {
                    getAllStoredFiles(socket);
                } else if (msgWrapper.hasDownloadFileMsg()) {
                    processDownloadFile(socket, msgWrapper);
                } else if (msgWrapper.hasGetFreeSpaceRequestMsg()) {
                    processGetFreeSpaceRequestMsg(socket);
                } else if (msgWrapper.hasChunkCorruptedMsg()) {
                    processChunkCorruptedMsg(msgWrapper);
                } else if (msgWrapper.hasGetMissingSeqNo()) {
                    processMissingSeqNoMsg(socket, msgWrapper);
                } else {
                    System.out.println("Invalid message ");
                }

            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error reading from socket");
            }
        }
        System.out.println("Socket disconnected!!");
        removeMessageQueue();
    }


    /**
     * @param socket
     * @param msgWrapper
     * @throws IOException Process heartbeats from storage nodes and responding back to storage node
     */
    private void heartbeatRequestMsg(Socket socket, ChatMessages.MessageWrapper msgWrapper) throws IOException {
        ChatMessages.Heartbeat msg = msgWrapper.getHeartbeatMsg();
        SocketConnection storageNode = new SocketConnection(msg.getStorageNodeHost(), msg.getStorageNodePort());

        this.semaphoreLocking.setSocketConnection(storageNode);
        //create heartbeat map<Sn,date>
        heartbeats.put(storageNode, new Date());
        //currently available SN list
        storageNodes.add(storageNode);

        //megQue -<SN, message>
        if (!messageQueues.containsKey(storageNode)) {
            messageQueues.put(storageNode, new MessageQueue());
        }

        if (isFirstHeartbeat(storageNode)) {
            onFirstHeartbeat(storageNode);
        } else {
            Map<String, SortedSet<Integer>> fileChunks = processFileChunksFromStorageNode(msg.getFileChunksList(), storageNode);
            System.out.println("Received heartbeat from " + storageNode + " with file chunks: " + fileChunks);
        }

        System.out.println("Received heartbeat message from storageNode " + storageNode.getHost()
                + "\n Sending Response back to storageNode");
        ChatMessages.MessageWrapper.newBuilder()
                .setHeartbeatAckMsg(ChatMessages.HeartbeatAck.newBuilder().build())
                .build()
                .writeDelimitedTo(socket.getOutputStream());
    }


    /**
     * @param socket
     * @throws IOException fetch the list of storage nodes and send the response back to client is required
     */
    private void getStorageNodesRequestMsg(Socket socket) throws IOException {
        List<ChatMessages.StorageNode> storageNodeList = new ArrayList<>();
        for (SocketConnection storageNode : storageNodes) {
            storageNodeList.add(ChatMessages.StorageNode.newBuilder()
                    .setHost(storageNode.getHost())
                    .setPort(storageNode.getPort())
                    .build()
            );
        }
        //Sending the storage node response back to the client with msgWrapper
        ChatMessages.GetStorageNodesResponse storageNodesResponse = ChatMessages.GetStorageNodesResponse.newBuilder()
                .addAllNodes(storageNodeList)
                .build();
        ChatMessages.MessageWrapper.newBuilder()
                .setGetStorageNodesResponseMsg(storageNodesResponse)
                .build().writeDelimitedTo(socket.getOutputStream());
    }

    /**
     * @param socket
     * @param msgWrapper
     * @throws IOException
     */
    private void processDownloadFile(Socket socket, ChatMessages.MessageWrapper msgWrapper) throws IOException {
        ChatMessages.DownloadFile msg = msgWrapper.getDownloadFileMsg();
        String fileName = msg.getFileName();
        BloomFilter file = dataStorage.getFile(fileName);

        if (file == null) {
            ChatMessages.MessageWrapper responseMsg = ChatMessages.MessageWrapper.newBuilder()
                    .setErrorMsg(ChatMessages.Error.newBuilder().setText("BloomFiler Error: Msg from controller processDownloadFile - File not found").build())
                    .build();
            responseMsg.writeDelimitedTo(socket.getOutputStream());
            return;
        }

        SortedSet<ChunkStoredInNode> chunks = file.getChunks();

        ChatMessages.DownloadFileResponse.Builder downloadFileResponseBuilder = ChatMessages.DownloadFileResponse.newBuilder();
        downloadFileResponseBuilder.setFilename(fileName);

        for (ChunkStoredInNode chunk : chunks) {
            ChatMessages.DownloadFileResponse.ChunkLocation.Builder chunkLocationBuilder = ChatMessages.DownloadFileResponse.ChunkLocation.newBuilder()
                    .setSequenceNo(chunk.getSequenceNo());
            for (SocketConnection storageNode : chunk.getReplicaLocations()) {
                chunkLocationBuilder.addStorageNodes(ChatMessages.StorageNode.newBuilder()
                        .setHost(storageNode.getHost())
                        .setPort(storageNode.getPort())
                        .build());
            }
            downloadFileResponseBuilder.addChunkLocations(chunkLocationBuilder.build());
        }

        ChatMessages.DownloadFileResponse internalMsg = downloadFileResponseBuilder.build();
        ChatMessages.MessageWrapper outMsgWrapper = ChatMessages.MessageWrapper.newBuilder()
                .setDownloadFileResponseMsg(internalMsg)
                .build();
        logger.debug("Telling client where " + fileName + " parts are");
        outMsgWrapper.writeDelimitedTo(socket.getOutputStream());
    }

    /**
     * @param socket _ socket
     * @throws Exception To store the filename and it's corresponding chunk information which is list of seqNo and storage node(replication)
     */
    private void getAllStoredFiles(Socket socket) throws Exception {
        Map<SocketConnection, ChatMessages.StorageNode> storageNodeMap = new HashMap<>();
        for (SocketConnection address : storageNodes) {
            storageNodeMap.put(address,
                    ChatMessages.StorageNode.newBuilder()
                            .setHost(address.getHost())
                            .setPort(address.getPort())
                            .build());
        }


        List<ChatMessages.DownloadFileResponse> downloadFileResponseMessages = new ArrayList<>();
        for (String filename : dataStorage.getFilenames()) {
            BloomFilter file = dataStorage.getFile(filename);
            if (file == null) {
                ChatMessages.MessageWrapper responseMsg = ChatMessages.MessageWrapper.newBuilder()
                        .setErrorMsg(ChatMessages.Error.newBuilder().setText("BloomFiler Error: Msg from controller while getAllStoredFiles - File not found").build())
                        .build();
                responseMsg.writeDelimitedTo(socket.getOutputStream());
                return;
            }

            List<ChatMessages.DownloadFileResponse.ChunkLocation> chunkLocations = new ArrayList<>();
            for (ChunkStoredInNode chunk : file.getChunks()) {
                List<ChatMessages.StorageNode> thisMsgStorageNodes = new ArrayList<>();
                for (SocketConnection address : chunk.getReplicaLocations()) {
                    thisMsgStorageNodes.add(storageNodeMap.get(address));
                }
                chunkLocations.add(
                        ChatMessages.DownloadFileResponse.ChunkLocation.newBuilder()
                                .setSequenceNo(chunk.getSequenceNo())
                                .addAllStorageNodes(thisMsgStorageNodes)
                                .build()
                );
            }

            downloadFileResponseMessages.add(
                    ChatMessages.DownloadFileResponse.newBuilder()
                            .setFilename(filename)
                            .addAllChunkLocations(chunkLocations)
                            .build()
            );
        }

        ChatMessages.MessageWrapper.newBuilder()
                .setGetFilesResponseMsg(
                        ChatMessages.GetFilesResponse.newBuilder()
                                .addAllFiles(downloadFileResponseMessages)
                                .build()
                )
                .build()
                .writeDelimitedTo(socket.getOutputStream());
    }

    /**
     * @param storageNode
     * @return
     */
    private boolean isFirstHeartbeat(SocketConnection storageNode) {
        for (SocketConnection sn : visitedStorageNodes) {
            return !sn.equals(storageNode);
        }
        return true;
    }

    /**
     * @param storageNode
     * @throws IOException
     */
    private void onFirstHeartbeat(SocketConnection storageNode) throws IOException {
        System.out.println("Received first heartbeat from " + storageNode + " on this connection");
        visitedStorageNodes.add(storageNode);

        //fetching all the availble files from the storagenode
        //logger.info("The storage node host is " + storageNode.getHost() + " :: " + storageNode.getPort());

        try (Socket storageNodeSocket = storageNode.getSocket()) {
            //System.out.println("Fetching complete list of files from : " + storageNode);

            // Message send from controller -> SN
            ChatMessages.MessageWrapper.newBuilder()
                    .setGetStorageNodeFilesRequest(ChatMessages.GetStorageNodeFilesRequest.newBuilder().build())
                    .build()
                    .writeDelimitedTo(storageNodeSocket.getOutputStream());

            // Controller send the response back to SN
            ChatMessages.MessageWrapper responseMsgWrp = ChatMessages.MessageWrapper.parseDelimitedFrom(storageNodeSocket.getInputStream());
            if (!responseMsgWrp.hasGetStorageNodeFilesResponse()) {
                logger.error("Expected Storage Node Files Response from " + storageNode + " but got: " + responseMsgWrp);
            } else {
                Map<String, SortedSet<Integer>> fileChunks = processFileChunksFromStorageNode(
                        responseMsgWrp.getGetStorageNodeFilesResponse().getFilesList(),
                        storageNode);
                //System.out.println("Got back the complete list of files from " + storageNode + ": " + fileChunks);
            }
        }
    }


    /**
     * @param fileChunksMessages
     * @param storageNode
     * @return filechunk
     */
    private Map<String, SortedSet<Integer>> processFileChunksFromStorageNode(List<ChatMessages.FileChunks> fileChunksMessages, SocketConnection storageNode) {
        Map<String, SortedSet<Integer>> fileChunks = toFileChunksMap(fileChunksMessages);
        for (Map.Entry<String, SortedSet<Integer>> entry : fileChunks.entrySet()) {
            String filename = entry.getKey();
            SortedSet<Integer> sequenceNos = entry.getValue();
            for (Integer sequenceNo : sequenceNos) {
                dataStorage.publishChunk(filename, sequenceNo, storageNode);
            }
        }
        return fileChunks;
    }

    /**
     * @param pbFileChunks
     * @return
     */
    //Sn you cannot send the chunks automatically so we have to send it through a map
    private Map<String, SortedSet<Integer>> toFileChunksMap(List<ChatMessages.FileChunks> pbFileChunks) {
        Map<String, SortedSet<Integer>> result = new HashMap<>();
        for (ChatMessages.FileChunks pbFileChunk : pbFileChunks) {
            String filename = pbFileChunk.getFilename();
            TreeSet<Integer> sequenceNos = new TreeSet<>(pbFileChunk.getSequenceNosList());
            result.put(filename, sequenceNos);
        }
        return result;
    }

    private void processGetFreeSpaceRequestMsg(Socket socket) throws IOException {
        long globalFreeSpace = 0L;
        long globalTotalSpace = 0L;

        List<Future<Map<String, Long>>> tasks = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            for (SocketConnection storageNode : storageNodes) {
                GetFreeSpaceTask task = new GetFreeSpaceTask(storageNode);
                tasks.add(executor.submit(task));
            }

            for (Future<Map<String, Long>> task : tasks) {
                try {
                    Long freeSpace = task.get().get("freeSpace");
                    Long totalSpace = task.get().get("totalSpace");
                    logger.info("freeSpace and totalSpace from each task is " + freeSpace + " "+totalSpace);
                    globalFreeSpace += freeSpace;
                    globalTotalSpace += totalSpace;
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("Free space information not complete", e);
                }
            }

            ChatMessages.MessageWrapper.newBuilder()
                    .setGetFreeSpaceResponseMsg(
                            ChatMessages.GetFreeSpaceResponse.newBuilder()
                                    .setFreeSpace(globalFreeSpace)
                                    .setTotalSpace(globalTotalSpace)
                                    .build()
                    )
                    .build()
                    .writeDelimitedTo(socket.getOutputStream());
        } finally {
            try {
                logger.trace("Attempting to shutdown executor");
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Interrupted", e);
            } finally {
                if (!executor.isTerminated()) {
                    logger.error("Some tasks didn't finish.");
                }
                executor.shutdownNow();
                logger.trace("ExecutorService shutdown finished.");
            }
        }
    }

    private static class GetFreeSpaceTask implements Callable<Map<String, Long>> {
        private final SocketConnection storageNode;

        private GetFreeSpaceTask(SocketConnection storageNode) {
            this.storageNode = storageNode;
        }

        @Override
        public Map<String, Long> call() throws Exception {
            long freeSpace;
            long totalSpace;
            Map<String, Long> mapp = new HashMap<>();
            Socket socket = storageNode.getSocket();

            ChatMessages.MessageWrapper.newBuilder()
                    .setGetFreeSpaceRequestMsg(ChatMessages.GetFreeSpaceRequest.newBuilder().build())
                    .build()
                    .writeDelimitedTo(socket.getOutputStream());

            ChatMessages.MessageWrapper responseMsgWrapper = ChatMessages.MessageWrapper.parseDelimitedFrom(socket.getInputStream());
            if (!responseMsgWrapper.hasGetFreeSpaceResponseMsg()) {
                throw new IllegalStateException("Expected get free space response message, but got: " + responseMsgWrapper);
            }
            ChatMessages.GetFreeSpaceResponse resp = responseMsgWrapper.getGetFreeSpaceResponseMsg();
            freeSpace = resp.getFreeSpace();
            totalSpace = resp.getTotalSpace();
            mapp.put("freeSpace",freeSpace);
            mapp.put("totalSpace",totalSpace);
            socket.close();

            return mapp;
        }
    }

    private void removeMessageQueue() {
        if (semaphoreLocking.getSocketConnection() != null) {
            messageQueues.remove(semaphoreLocking.getSocketConnection());
        }
    }

    private void processChunkCorruptedMsg(ChatMessages.MessageWrapper msgWrapper) {
        ChatMessages.ChunkCorrupted msg = msgWrapper.getChunkCorruptedMsg();
        String filename = msg.getFilename();
        int sequenceNo = msg.getSequenceNo();
        String storageNodeHost = msg.getStorageNode().getHost();
        int storageNodePort = msg.getStorageNode().getPort();
        SocketConnection storageNode = new SocketConnection(storageNodeHost, storageNodePort);
        logger.warn("Received notice of corrupted file " + filename + " chunk #" + sequenceNo + " on " + storageNode);
        dataStorage.onChunkCorrupted(filename, sequenceNo, storageNode);
    }

    private void processMissingSeqNoMsg(Socket socket, ChatMessages.MessageWrapper msgWrapper) throws IOException {
        Boolean flag = false;
        ChatMessages.MessageWrapper wrapperFromSN = null;
        for (SocketConnection storageNode : storageNodes) {
            msgWrapper.writeDelimitedTo(storageNode.getSocket().getOutputStream());
            wrapperFromSN = ChatMessages.MessageWrapper.parseDelimitedFrom(storageNode.getSocket().getInputStream());
            if (wrapperFromSN.hasGetMissingSeqNoResponse()) {
                flag = true;
                break;
            }
        }
        if (!flag) {
            ChatMessages.Error error = ChatMessages.Error.newBuilder().setText("No chunk file").build();
            ChatMessages.MessageWrapper messageWrapper = ChatMessages.MessageWrapper.newBuilder().setErrorMsg(error).build();
        } else {
            logger.error("Missing: msg Found");
        }
        wrapperFromSN.writeDelimitedTo(socket.getOutputStream());
    }
}
