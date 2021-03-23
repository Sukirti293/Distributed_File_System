package edu.usfca.cs.dfs.components.StorageNode;

import com.google.protobuf.ByteString;
import edu.usfca.cs.chat.ChatMessages;
import edu.usfca.cs.dfs.components.Compiler;
import edu.usfca.cs.dfs.components.DataStructure.FileChunks;
import edu.usfca.cs.dfs.components.DataStructure.SocketConnection;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class StorageNodeRequestThread implements Runnable {
    private final Socket socket;
    private final Map<String, SortedSet<FileChunks>> map;
    private final Lock chunksLock;
    private final Map<SocketConnection, Socket> socketConnection = new HashMap<>();
    private static Logger logger = Logger.getLogger(StorageNodeRequestThread.class);

    public StorageNodeRequestThread(Socket socket, Map<String, SortedSet<FileChunks>> map, Lock chunksLock) {
        this.socket = socket;
        this.map = map;
        this.chunksLock = chunksLock;
    }

    @Override
    public void run() {
        int countExceptions = 0;
        int nullMessage = 0;

        while (!socket.isClosed()) {
            try {

                ChatMessages.MessageWrapper msg = ChatMessages.MessageWrapper.parseDelimitedFrom(
                        socket.getInputStream());

                if (msg == null) {
                    nullMessage++;
                    if (nullMessage == 10) {
                        logger.trace("Closing socket becasue the null count is more than 10 :: " + socket.getRemoteSocketAddress());
                        socket.close();
                        return;
                    } else {
                        continue;
                    }
                }
                if (msg.hasStoreChunkMsgRequest()) {
                    processStoreChunkMsg(msg);
                } else if (msg.hasOrderSendChunkMsg()) {
                    processOrderSendChunkMsg(msg);
                } else if (msg.hasDownloadChunkMsg()) {
                    processDownloadChunkMsg(socket, msg);
                } else if (msg.hasGetFreeSpaceRequestMsg()) {
                    processGetFreeSpaceRequestMsg(socket);
                } else if (msg.hasGetStorageNodeFilesRequest()) {
                    processGetStorageNodeFilesRequestMsg(socket);
                } else if (msg.hasGetMissingSeqNo()) {
                    processGetMissingSeqNo(socket, msg);
                }
            } catch (IOException e) {
                countExceptions++;
                if (countExceptions == 50) {
                    e.printStackTrace();
                    //System.exit(1);
                }
            }
        }
        System.out.println("Connection closed for :: " + socket.getRemoteSocketAddress());
    }

    //process each chunk file got from controller
    private void processGetStorageNodeFilesRequestMsg(Socket socket) throws IOException {
        Set<ChatMessages.FileChunks> result;

        chunksLock.lock();
        try {
            result = new LinkedHashSet<>();
            for (Map.Entry<String, SortedSet<FileChunks>> entry : map.entrySet()) {
                String filename = entry.getKey();
                ArrayList<Integer> sequenceNos = new ArrayList<>(entry.getValue().size());

                for (FileChunks chunk : entry.getValue()) {
                    sequenceNos.add(chunk.getSeqNumber());
                }
                //Send the chunk info to Controller for multiple file chunks
                ChatMessages.FileChunks fileChunksMsg = ChatMessages.FileChunks.newBuilder()
                        .setFilename(filename)
                        .addAllSequenceNos(sequenceNos)
                        .build();
                result.add(fileChunksMsg);
            }
        } finally {
            chunksLock.unlock();
        }

        //Send the entire set of file chunks to controller
        ChatMessages.MessageWrapper.newBuilder()
                .setGetStorageNodeFilesResponse(
                        ChatMessages.GetStorageNodeFilesResponse.newBuilder()
                                .addAllFiles(result)
                                .build()
                )
                .build()
                .writeDelimitedTo(socket.getOutputStream());
    }

    private void processGetFreeSpaceRequestMsg(Socket socket) throws IOException {
//        logger.info("StorageNode free space is :: " + new File(Paths.get("").toAbsolutePath().toString() + Compiler.storageNodeChunkFolder).getFreeSpace());


        Path  c = Paths.get(Compiler.storageNodeChunkFolder);
        //System.out.println(c.toString());
        Path root = c.getRoot();
        //System.out.println(root.toString());
        Path val = root.resolve(Compiler.storageNodeChunkFolder);
        //System.out.println(val);

        logger.info("StorageNode free space is :: " + new File(String.valueOf(val)).getFreeSpace());
        ChatMessages.MessageWrapper msg = ChatMessages.MessageWrapper.newBuilder()
                .setGetFreeSpaceResponseMsg(
                        ChatMessages.GetFreeSpaceResponse.newBuilder()
                                //.setFreeSpace(new File(Compiler.storageNodeChunkFolder).getFreeSpace())
                                .setFreeSpace(new File(String.valueOf(val)).getUsableSpace())
                                .setTotalSpace(new File(String.valueOf(val)).getTotalSpace())
                                .build()
                )
                .build();
        msg.writeDelimitedTo(socket.getOutputStream());
    }

    private void processDownloadChunkMsg(Socket socket, ChatMessages.MessageWrapper messageWrapper) throws IOException {
        ChatMessages.DownloadChunk msg = messageWrapper.getDownloadChunkMsg();
        String filename = msg.getFilename();
        int sequenceNo = msg.getSequenceNo();
        sendChunk(filename, sequenceNo, socket);
    }

    private void processOrderSendChunkMsg(ChatMessages.MessageWrapper msgWrapper) throws IOException {
        ChatMessages.OrderSendChunk msg = msgWrapper.getOrderSendChunkMsg();
        String host = msg.getStorageNode().getHost();
        int port = msg.getStorageNode().getPort();
        SocketConnection storageNode = new SocketConnection(host, port);
        String filename = msg.getFileChunk().getFilename();
        int sequenceNo = msg.getFileChunk().getSequenceNo();
        if (socketConnection.get(storageNode) == null || socketConnection.get(storageNode).isClosed()) {
            socketConnection.put(storageNode, storageNode.getSocket());
        }
        Socket socket = socketConnection.get(storageNode);

        sendChunk(filename, sequenceNo, socket);
    }

    private void sendChunk(String filename, int sequenceNo, Socket socket) throws IOException {
        // Retrieve the chunk on local filesystem

        Path c = Paths.get(Compiler.storageNodeChunkFolder);
        //System.out.println(c.toString());
        Path root = c.getRoot();
        //System.out.println(root.toString());
        Path val = root.resolve(Compiler.storageNodeChunkFolder);
        //System.out.println(val);

        String chunkFileName = filename + "-chunk" + sequenceNo;
        Path chunkPath = Paths.get(String.valueOf(val), chunkFileName);
        //Path chunkPath = Paths.get(Compiler.storageNodeChunkFolder, chunkFileName);
        File chunkFile = chunkPath.toFile();
        if (!chunkFile.exists()) {
            throw new IllegalStateException("I don't have " + chunkPath.toString() + ". Can't send it to another storage node.");
        }

        // send a store chunk message
        FileInputStream fis = new FileInputStream(chunkFile);
        String expectedChecksum = new String(Files.readAllBytes(Paths.get(String.valueOf(val), chunkFileName + ".md5"))).split(" ")[0];
        //String expectedChecksum = new String(Files.readAllBytes(Paths.get(Compiler.storageNodeChunkFolder, chunkFileName + ".md5"))).split(" ")[0];
        String actualChecksum = FileChunks.md5sum(chunkFile);
        if (actualChecksum == null || !actualChecksum.equals(expectedChecksum)) {
            logger.error("Error in actual checksum and expected checksum");
        }
        ChatMessages.MessageWrapper msg = ChatMessages.MessageWrapper.newBuilder()
                .setStoreChunkMsgRequest(
                        ChatMessages.StoreChunk.newBuilder()
                                .setFileName(filename)
                                .setSequenceNo(sequenceNo)
                                .setData(ByteString.readFrom(fis))
                                .setChecksum(expectedChecksum)
                                .build()
                ).build();
        fis.close();
        msg.writeDelimitedTo(socket.getOutputStream());
    }

    /**
     * @param msgWrapper
     * @throws IOException This is how the data stores ina  SN
     *                     get all the required values from the Client and create chunks and Write that to the storahe node
     *                     system, during fetching just read from the storage node
     */
    private void processStoreChunkMsg(ChatMessages.MessageWrapper msgWrapper) throws IOException {
        ChatMessages.StoreChunk storeChunkMsg = msgWrapper.getStoreChunkMsgRequest();


        Path c = Paths.get(Compiler.storageNodeChunkFolder);
        //System.out.println(c.toString());
        Path root = c.getRoot();
        //System.out.println(root.toString());
        Path val = root.resolve(Compiler.storageNodeChunkFolder);
        //System.out.println(val);


        String storageDirectory = String.valueOf(val);
        //String storageDirectory = Compiler.storageNodeChunkFolder;
        //creating the dir if not exists
        File storageDirectoryFile = new File(storageDirectory);
        if (!storageDirectoryFile.exists()) {
            storageDirectoryFile.mkdir();
        }

        //Creating file names and create chunk files
        String chunkFilename = storeChunkMsg.getFileName() + "-chunk" + storeChunkMsg.getSequenceNo();
        Path chunkFilePath = Paths.get(storageDirectory, chunkFilename);
        File chunkFile = chunkFilePath.toFile();

        //check if the chunk file exists
        if (chunkFile.exists()) {
            if (!chunkFile.delete()) {
                throw new RuntimeException("Unable to delete existing file before overwriting");
            }
        }

        //Output steam - writing to the dir
        FileOutputStream fos = new FileOutputStream(chunkFile);
        storeChunkMsg.getData().writeTo(fos);
        fos.close();

        //creating new checksum for that chunk and validate with the previous chunk message checksum value
//        if(!chunkFile.exists()){
//            File chunkFileTemp = chunkFilePath.toFile();
//            //Output steam - writing to the dir
//            FileOutputStream fosTemp = new FileOutputStream(chunkFileTemp);
//            storeChunkMsg.getData().writeTo(fosTemp);
//            fos.close();
//        }

        String actualChecksum = FileChunks.md5sum(chunkFile);
        if (actualChecksum == null || !actualChecksum.equals(storeChunkMsg.getChecksum())) {
            logger.error("Error in creating actual checksum at for seqNo " + storeChunkMsg.getSequenceNo());
            throw new RuntimeException("Error in creating actual checksum at sn");
        }

        //creating md5 file which contains the checksum value with filename
        Path checksumFilePath = Paths.get(storageDirectory, chunkFilename + ".md5");
        //logger.info("checksumFilePath: " + checksumFilePath);
        String str = storeChunkMsg.getChecksum() + "  " + chunkFilename + "\n";
        FileWriter inputFileWriter = new FileWriter(checksumFilePath.toString());
        inputFileWriter.write(str);
        inputFileWriter.close();

        //create new chunk with the new chunk
        FileChunks chunk = new FileChunks(storeChunkMsg.getFileName(), storeChunkMsg.getSequenceNo(), Files.size(chunkFilePath), storeChunkMsg.getChecksum(), chunkFilePath);
        StorageNode.addToMap(chunk, map, chunksLock);
    }

    private void processGetMissingSeqNo(Socket socket, ChatMessages.MessageWrapper msg) {
        try {

            Path c = Paths.get(Compiler.storageNodeChunkFolder);
            //System.out.println(c.toString());
            Path root = c.getRoot();
            //System.out.println(root.toString());
            Path val = root.resolve(Compiler.storageNodeChunkFolder);
            //System.out.println(val);

            String storageDirectory = String.valueOf(val);
            //String storageDirectory = Compiler.storageNodeChunkFolder;
            //logger.info("Missing Path: " + storageDirectory);
            List<ChatMessages.StoreChunk> listFileChunks = new ArrayList<>();

            //creating the dir if not exists
            File storageDirectoryFile = new File(storageDirectory);
            if (!storageDirectoryFile.exists()) {
                logger.error("Missing :: Directory doesn't exist");
            }

            //Creating file names and create chunk files
            //logger.info("Missing Sequence List" + msg.getGetMissingSeqNo().getSequenceNoList());
            for (int i : msg.getGetMissingSeqNo().getSequenceNoList()) {
                //logger.info("Enter missing seq list loop");
                String chunkFilename = msg.getGetMissingSeqNo().getFilename() + "-chunk" + i;
                //logger.info("Missing chunkFilename: " + chunkFilename);
                File directoryPath = new File(storageDirectory);
                //List of all files and directories
                List<String> contents = Arrays.asList(directoryPath.list());
                //logger.info("Missing contents: " + contents);
                System.out.println("List of files and directories in the specified directory:");
                for (String content : contents) {
                    //logger.info("Missing content:  " + contents.get(i));
                    if (content.contains(".md5")) {
                        continue;
                    }
                    if (contents.contains(chunkFilename)) {
                        //logger.info("Missing :: Got the missing file with seqN0 " + i);
                        Path chunkFilePath = Paths.get(storageDirectory, chunkFilename);
                        File chunkFile = chunkFilePath.toFile();
                        FileInputStream fis = new FileInputStream(chunkFile);
                        ByteString data = ByteString.readFrom(fis);
                        //logger.info("Missing Data: " + data);
                        fis.close();
                        String actualChecksum = FileChunks.md5sum(chunkFile);
                        //logger.info("Missing Checksum: " + actualChecksum);
                        ChatMessages.StoreChunk chatWrapper = ChatMessages.StoreChunk.newBuilder()
                                .setFileName(msg.getGetMissingSeqNo().getFilename())
                                .setSequenceNo(i)
                                .setChecksum(actualChecksum)
                                .setData(data)
                                .build();

                        listFileChunks.add(chatWrapper);
                        //logger.info("Missing ListChunks: " + listFileChunks);
                    }
                }
                //logger.info("Missing :: created chunk list " + listFileChunks);
                ChatMessages.MissingSeqNoResponse missingSeqNoResponse = ChatMessages.MissingSeqNoResponse.newBuilder().addAllStoreChunk(listFileChunks).build();
                ChatMessages.MessageWrapper missingMsg = ChatMessages.MessageWrapper.newBuilder().setGetMissingSeqNoResponse(missingSeqNoResponse).build();
                missingMsg.writeDelimitedTo(socket.getOutputStream());
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }

    }
}

