package edu.usfca.cs.dfs.components.replication;

import edu.usfca.cs.chat.ChatMessages;
import edu.usfca.cs.chat.MessageQueue;
import edu.usfca.cs.dfs.components.Compiler;
import edu.usfca.cs.dfs.components.DataStructure.ChunkStoredInNode;
import edu.usfca.cs.dfs.components.DataStructure.SocketConnection;
import edu.usfca.cs.dfs.components.filterAndStorage.DataStorage;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicaThread implements Runnable {

    private final Set<SocketConnection> storageNodes;
    private final Map<SocketConnection, MessageQueue> messageQueues;
    private final DataStorage dataStorage;
    private static Logger logger = Logger.getLogger(ReplicaThread.class);

    public ReplicaThread(Set<SocketConnection> storageNodes, DataStorage dataStorage, Map<SocketConnection, MessageQueue> messageQueues) {
        this.storageNodes = storageNodes;
        this.messageQueues = messageQueues;
        this.dataStorage = dataStorage;
    }

    /**
     * This method checks whether a chunk is undeReplicated
     * if true, then it creats more replicas
     */
    @Override
    public void run() {
        try {
            int expireTime = Compiler.replicaCheckDuration;
            while (true) {
                List<ChunkStoredInNode> underReplicatedChunks = dataStorage.isUnderReplicated();
                if (underReplicatedChunks != null && !underReplicatedChunks.isEmpty()) {
                    createReplicas(underReplicatedChunks);
                }

                Thread.sleep(expireTime);
            }
        } catch (Exception exception) {
            exception.toString();
        }


    }

    private void createReplicas(List<ChunkStoredInNode> underReplicatedChunks) {
        int minReplicas = Compiler.totalNoOfReplicas;
        for (ChunkStoredInNode chunk : underReplicatedChunks) {
            Set<SocketConnection> unusedStorageNodes = new HashSet<>(storageNodes);
            unusedStorageNodes.removeAll(chunk.getReplicaLocations());

            if (unusedStorageNodes.isEmpty()) {
                System.out.println("There are not enough SNs available for creating min number of replicas");
                return;
            }
            int missingReplicas = minReplicas - chunk.getReplicaCount();
            Set<SocketConnection> additionalNodes = Compiler.chooseNrandomOrMin(missingReplicas, unusedStorageNodes);
            if (additionalNodes.size() < missingReplicas) {
                logger.info("For " + chunk.getSequenceNo() + " we need " + missingReplicas + " but have only " + additionalNodes.size());

            }

            for (SocketConnection additionalNode : additionalNodes) {
                ChatMessages.MessageWrapper msg = buildOrderChunkMsg(chunk, additionalNode);
                SocketConnection senderNode = Compiler.chooseNrandomOrMin(1, chunk.getReplicaLocations()).iterator().next();
                logger.info("Telling " + senderNode + " to transfer " + chunk + " to " + additionalNode);
                messageQueues.get(senderNode).queue(msg);
            }
        }

    }

    private ChatMessages.MessageWrapper buildOrderChunkMsg(ChunkStoredInNode chunk, SocketConnection additionalNode) {
        return ChatMessages.MessageWrapper.newBuilder()
                .setOrderSendChunkMsg(
                        ChatMessages.OrderSendChunk.newBuilder()
                                .setStorageNode(
                                        ChatMessages.StorageNode.newBuilder()
                                                .setHost(additionalNode.getHost())
                                                .setPort(additionalNode.getPort())
                                                .build()
                                )
                                .setFileChunk(
                                        ChatMessages.FileChunk.newBuilder()
                                                .setFilename(chunk.getFilename())
                                                .setSequenceNo(chunk.getSequenceNo())
                                                .build()
                                )
                                .build()
                )
                .build();
    }
}
