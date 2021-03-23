package edu.usfca.cs.dfs.components.StorageNode;

import edu.usfca.cs.chat.ChatMessages;
import edu.usfca.cs.dfs.components.DataStructure.FileChunks;
import edu.usfca.cs.dfs.components.DataStructure.SocketConnection;
import io.netty.channel.Channel;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class ChunkFailureThread implements Runnable {
    private final SocketConnection storageNode;
    private final Map<String, SortedSet<FileChunks>> map;
    private final Lock chunkLock;
    private final SocketConnection cntrlSocket;


    public ChunkFailureThread(SocketConnection storageNode, Map<String, SortedSet<FileChunks>> map, Lock chunkLock, SocketConnection cntrlSocket) {
        this.storageNode = storageNode;
        this.map = map;
        this.chunkLock = chunkLock;
        this.cntrlSocket = cntrlSocket;
    }

    @Override
    public void run() {
        while (true) {
            chunkLock.lock();
            try {
                /**
                 * Suppose SNs have multiple maps which take one name and set of chunks stored in a SN
                 * take one set and create an arraylist, add each corrupted chunks there
                 * The for each corrupted chunks
                 */
                for (SortedSet<FileChunks> chunksSortedSet : map.values()) {
                    List<FileChunks> corruptedChunks = new ArrayList<>();
                    for (FileChunks chunk : chunksSortedSet) {
                        if (chunk.isCorrupted()) {
                            corruptedChunks.add(chunk);
                        }
                    }
                    /**
                     * Delete the corrupted file and the respective checksum file
                     */
                    for (FileChunks chunk : corruptedChunks) {
                        File chunkFile = chunk.getChunkLocalPath().toFile();
                        File checksumFile = new File(chunkFile.getAbsolutePath() + ".md5");
                        chunkFile.delete();
                        checksumFile.delete();
                        chunksSortedSet.remove(chunk);
                        try {
                            sendChunkFailureMsg(chunk);
                        } catch (Exception exception) {
                            exception.printStackTrace();
                        }
                    }
                }
            } finally {
                chunkLock.unlock();
            }

            /**
             * remove the keys for the failed chunks
             */
            for (String filename : new HashSet<>(map.keySet())) {
                if (map.get(filename).isEmpty()) {
                    map.remove(filename);
                }
            }

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @param chunk
     * @throws IOException Storage node sends failure chunks information to the controller
     */
    private void sendChunkFailureMsg(FileChunks chunk) throws IOException {
        ChatMessages.MessageWrapper msg = ChatMessages.MessageWrapper.newBuilder()
                .setChunkCorruptedMsg(
                        ChatMessages.ChunkCorrupted.newBuilder()
                                .setFilename(chunk.getFileName())
                                .setSequenceNo(chunk.getSeqNumber())
                                .setStorageNode(
                                        ChatMessages.StorageNode.newBuilder()
                                                .setHost(storageNode.getHost())
                                                .setPort(storageNode.getPort())
                                                .build())
                )
                .build();


        Socket socket = cntrlSocket.getSocket();
        msg.writeDelimitedTo(socket.getOutputStream());
        socket.close();
    }
}
