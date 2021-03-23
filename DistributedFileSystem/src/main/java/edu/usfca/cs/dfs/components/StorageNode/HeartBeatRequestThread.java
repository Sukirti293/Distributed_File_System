package edu.usfca.cs.dfs.components.StorageNode;

import edu.usfca.cs.chat.ChatMessages;
import edu.usfca.cs.dfs.components.DataStructure.FileChunks;
import edu.usfca.cs.dfs.components.DataStructure.SocketConnection;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class HeartBeatRequestThread implements Runnable {
    private final SocketConnection storageNode;
    private final SocketConnection socketConnection;
    private final Map<String, SortedSet<FileChunks>> map;
    private Map<String, SortedSet<FileChunks>> lastChunks = new HashMap<>();
    private final Lock chunksLock;
    private static final Logger logger = Logger.getLogger(StorageNode.class);

    public HeartBeatRequestThread(SocketConnection storageNode, SocketConnection socketConnection, Map<String, SortedSet<FileChunks>> map, Lock chunksLock) {
        this.storageNode = storageNode;
        this.socketConnection = socketConnection;
        this.map = map;
        this.chunksLock = chunksLock;
    }

    @Override
    public void run() {
        int heartbeatTime = 5000;

        while (true) {
            try {
                System.out.println("Storage node is now trying to connect to controller.....");
                Socket socket = socketConnection.getSocket();

                /*
                Inner while loop is created because heartbeat creation hit for every 5secs
                heartbeat msg has values like hostname, port and chunks available in a SN
                */
                while (true) {
                    ChatMessages.Heartbeat heartbeatMsg = ChatMessages.Heartbeat.newBuilder()
                            .setStorageNodeHost(storageNode.getHost())
                            .setStorageNodePort(storageNode.getPort())
                            .addAllFileChunks(getNewChunks())
                            .build();
                    ChatMessages.MessageWrapper msgWrapper =
                            ChatMessages.MessageWrapper.newBuilder()
                                    .setHeartbeatMsg(heartbeatMsg)
                                    .build();
                    logger.info("Sending heartbeat to controller " + socketConnection);
                    msgWrapper.writeDelimitedTo(socket.getOutputStream());

                    logger.info("Waiting for response from controller");
                    ChatMessages.MessageWrapper response = ChatMessages.MessageWrapper.parseDelimitedFrom(socket.getInputStream());
                    if (response.hasHeartbeatAckMsg()) {
                        logger.info("Yeah!!! ... got the Response from controller");
                        for (Map.Entry<String, SortedSet<FileChunks>> entry : map.entrySet()) {
                            lastChunks.put(entry.getKey(), new TreeSet<>(entry.getValue()));
                        }
                    } else {
                        logger.error("Error in runnable in heartbeat");
                    }


                    Thread.sleep(heartbeatTime);
                }
            } catch (Exception e) {
                logger.error("Can't send heartbeats to controller " + e.toString() + "\nController might be unavailable");
            }

            try {
                logger.info("Retrying the connection.....");
                Thread.sleep(heartbeatTime);
            } catch (InterruptedException e) {
                logger.error("Interrupted while sleeping");
            }
        }
    }

    private Collection<ChatMessages.FileChunks> getNewChunks() {
        chunksLock.lock();
        try {
            Set<ChatMessages.FileChunks> result = new LinkedHashSet<>();
            for (Map.Entry<String, SortedSet<FileChunks>> entry : newChunks(lastChunks, map).entrySet()) {
                String filename = entry.getKey();
                ArrayList<Integer> sequenceNos = new ArrayList<>(entry.getValue().size());

                for (FileChunks chunk : entry.getValue()) {
                    sequenceNos.add(chunk.getSeqNumber());
                }

                ChatMessages.FileChunks fileChunksMsg = ChatMessages.FileChunks.newBuilder()
                        .setFilename(filename)
                        .addAllSequenceNos(sequenceNos)
                        .build();
                result.add(fileChunksMsg);
            }
            return result;
        } finally {
            chunksLock.unlock();
        }
    }


    //TODO Debug
    private Map<String, SortedSet<FileChunks>> newChunks(final Map<String, SortedSet<FileChunks>> oldChunkMap, final Map<String, SortedSet<FileChunks>> newChunkMap) {
        Map<String, SortedSet<FileChunks>> diff = new HashMap<>();
        for (Map.Entry<String, SortedSet<FileChunks>> entry : newChunkMap.entrySet()) {
            if (!oldChunkMap.containsKey(entry.getKey()) && !entry.getValue().isEmpty()) {
                diff.put(entry.getKey(), entry.getValue());
            } else {

                SortedSet<FileChunks> oldChunks = oldChunkMap.get(entry.getKey());
                SortedSet<FileChunks> newChunks = new TreeSet<>();
                for (FileChunks chunk : entry.getValue()) {
                    if (!oldChunks.contains(chunk)) {
                        newChunks.add(chunk);
                    }
                }
                if (!newChunks.isEmpty()) {
                    diff.put(entry.getKey(), newChunks);
                }
            }
        }
        return diff;
    }

}

