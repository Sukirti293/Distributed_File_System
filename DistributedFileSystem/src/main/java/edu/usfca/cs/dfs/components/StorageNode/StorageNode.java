package edu.usfca.cs.dfs.components.StorageNode;

import edu.usfca.cs.chat.ChatMessages;
import edu.usfca.cs.dfs.components.Compiler;
import edu.usfca.cs.dfs.components.Controller.Controller;
import edu.usfca.cs.dfs.components.DataStructure.FileChunks;
import edu.usfca.cs.dfs.components.DataStructure.SocketConnection;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StorageNode {

    private final int port;
    private final SocketConnection socketConnection;
    private final Map<String, SortedSet<FileChunks>> chunks;
    private static Logger logger = Logger.getLogger(StorageNode.class);

    public StorageNode(int port, SocketConnection socketConnection) throws IOException {
        this.port = port;
        this.socketConnection = socketConnection;
        this.chunks = readFileChunks();
    }

    /**
     * @param args
     * @throws Exception get port for storage node, get controller hostname and controller port
     */
    public static void main(String[] args)
            throws Exception {
        BasicConfigurator.configure();
        if (args.length != 3) {
            logger.error("Arguments required : storagePort CntrlHostName Cntrlport");
            System.exit(1);
        }
        int storageNodePort = Integer.parseInt(args[0]);
        String controllerHostname = args[1];
        int controllerPort = Integer.parseInt(args[2]);


        //TODO make it non-blocking as this is blocking io
        SocketConnection socketConnection = new SocketConnection(controllerHostname, controllerPort);
        StorageNode sn = new StorageNode(storageNodePort, socketConnection);
        sn.connect();
    }

    /**
     * @throws Exception
     */
    private void connect()
            throws Exception {

        //As multiple storage nodes will access this method, ReentrantLock is used for provide synchronization shared resources
        Lock chunksLock = new ReentrantLock();
        SocketConnection snSocket = new SocketConnection(getStorageNodeHostName(), port);

        //Send Heartbeats to controller
        HeartBeatRequestThread heartBeatRequestThread = new HeartBeatRequestThread(snSocket, socketConnection, chunks, chunksLock);
        Thread heartBeatThread = new Thread(heartBeatRequestThread);
        heartBeatThread.start();

        //checks for chunk failure and any chunk has failed then it reports to the controller
        ChunkFailureThread chunkFailure = new ChunkFailureThread(snSocket, chunks, chunksLock, socketConnection);
        Thread chunkFailureThread = new Thread(chunkFailure);
        chunkFailureThread.start();

        ServerSocket snServerSocket = new ServerSocket(port);
        while (true) {
            Socket socket = snServerSocket.accept();

            //logger.info("StorageNode is listening .....");
            StorageNodeRequestThread snRequest = new StorageNodeRequestThread(socket, chunks, chunksLock);
            Thread snRequestThread = new Thread(snRequest);
            snRequestThread.start();
        }
    }


    /**
     * @param chunk
     * @param chunks
     * @param lock   This method is only used if storage node got disconnected and then reconnected back again
     */
    public static void addToMap(FileChunks chunk, Map<String, SortedSet<FileChunks>> chunks, Lock lock) {
        String fileName = chunk.getFileName();
        lock.lock();

        try {
            if (chunks.get(fileName) == null) {
                chunks.put(fileName, new TreeSet<FileChunks>());
                logger.debug("Thread " + Thread.currentThread().getName() + " didn't see an entry for file name '" + fileName + "'.");
            }

            chunks.get(fileName).add(chunk);
            for (Map.Entry<String, SortedSet<FileChunks>> entry : chunks.entrySet()) {
                String key = entry.getKey();
                SortedSet<FileChunks> value = entry.getValue();
//                logger.info("inside addToMap method with value :" + value.toString());
            }
//            ChatMessages.StoreChunkMsgResponse reponseToClient = ChatMessages.
//                    StoreChunkMsgResponse.newBuilder().setSequenceNos(chunk.getSeqNumber()).build();
//            ChatMessages.MessageWrapper messageWrapper = ChatMessages.MessageWrapper.newBuilder().
//                    setStoreChunkMsgResponse(reponseToClient).build();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return this returns the map which takes file name as the key
     * and set of chunks (File chunk) as a value
     * This function is used to retrive all the available chunks stored in a SN
     * @throws IOException
     */
    private Map<String, SortedSet<FileChunks>> readFileChunks() throws IOException {
        Map<String, SortedSet<FileChunks>> result = new HashMap<>();
        try {
            Lock reentrantLock = new ReentrantLock();
            Path c = Paths.get(Compiler.storageNodeChunkFolder);
            //System.out.println(c.toString());
            Path root = c.getRoot();
            //System.out.println(root.toString());
            Path chunkPath = root.resolve(Compiler.storageNodeChunkFolder);
            //System.out.println(chunkPath.toString());
            //String storageChunksAbsPath = Paths.get("").toAbsolutePath().toString() + Compiler.storageNodeChunkFolder;
            //String storageChunksAbsPath = Compiler.storageNodeChunkFolder;
//            Path chunkPath = Paths.get(storageChunksAbsPath);
            File chunkDir = chunkPath.toFile();
            Pattern chunkFileNamePattern = Pattern.compile("(.*?)-chunk([0-9]+)");

            //TODO needs to delete this directory after use
            if (!chunkDir.exists()) {
                return result;
            } else {
                //String absPath = chunkFile.getAbsolutePath();
                //System.out.println(absPath);
                if (!chunkDir.isDirectory()) {
                    throw new IllegalArgumentException("Not a directory, please provide the correct file path");
                }
            }

            DirectoryStream<Path> directoryStream = Files.newDirectoryStream(chunkPath);
            for (Path path : directoryStream) {
                //Avoid the checksum files
                if (path.toString().endsWith(".md5")) {
                    continue;
                }

                File chunkFile = path.toFile();
                //TODO implemenete bloom filter rather than matcher
                Matcher matcher = chunkFileNamePattern.matcher(chunkFile.getName());
                if (!matcher.find()) {
                    throw new IllegalArgumentException("Malformed chunk file name " + chunkFile.getName());
                }
                String originalFileName = matcher.group(1);
                int seqNumber = Integer.parseInt(matcher.group(2));
                FileChunks chunk = new FileChunks(originalFileName, seqNumber, chunkFile.length(), "", path);
                chunk.calculateChecksum();
                Path checksumFilePath = Paths.get(path.toString() + ".md5");
                String expectedChecksum = new String(Files.readAllBytes(checksumFilePath)).split(" ")[0];
                String actualChecksum = FileChunks.md5sum(chunkFile);
                if (actualChecksum == null || !actualChecksum.equals(expectedChecksum)) {
                    logger.error("Error in Checksum");
                }
                addToMap(chunk, result, reentrantLock);
            }
            return result;

        } catch (Exception e) {
            logger.error("StorageNode :: " + e.toString());
        }
        return result;

    }

    /**
     * @return
     */
    public String getStorageNodeHostName() {
        try {
            String controllerHostName = "";
            controllerHostName = InetAddress.getLocalHost().getHostName();

            System.out.println("SNode host is :: " + controllerHostName);
            System.out.println("SNode port is :: " + port);
            return controllerHostName;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }


}
