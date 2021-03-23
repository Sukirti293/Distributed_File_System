package edu.usfca.cs.dfs.components.filterAndStorage;

import edu.usfca.cs.dfs.components.Compiler;
import edu.usfca.cs.dfs.components.Controller.Controller;
import edu.usfca.cs.dfs.components.DataStructure.ChunkStoredInNode;
import edu.usfca.cs.dfs.components.DataStructure.SocketConnection;
import org.apache.log4j.Logger;

import java.util.*;

public class DataStorage {

    private final Map<String, BloomFilter> files = new HashMap<>();
    private static final Logger logger = Logger.getLogger(Controller.class);

    /**
     * @return Checking whether the chuck is under replicated
     */
    public synchronized List<ChunkStoredInNode> isUnderReplicated() {
        List<ChunkStoredInNode> chunks = new ArrayList<>();
        int minReplicas = Compiler.totalNoOfReplicas;
        for (BloomFilter file : files.values()) {
            for (ChunkStoredInNode chunk : file.getChunks()) {
                if (chunk.getReplicaCount() < minReplicas) {
                    chunks.add(chunk);
                }
            }
        }
        return chunks;
    }


    public synchronized void onStorageNodeOffline(SocketConnection storageNode) {
        // Chunks that are modified and that we need to remove later in case
        // there are no more replicas left
        List<ChunkStoredInNode> modifiedChunks = new ArrayList<>();

        for (BloomFilter file : files.values()) {
            for (ChunkStoredInNode chunk : file.getChunks()) {
                Set<SocketConnection> locations = chunk.getReplicaLocations();
                if (locations.contains(storageNode)) {
                    locations.remove(storageNode);
                    modifiedChunks.add(chunk);
                }
            }
        }

        cleanup(modifiedChunks);
    }

    private void cleanup(List<ChunkStoredInNode> modifiedChunks) {
        // Files that are modified and that we need to remove later in case
        // there are no chunks left
        List<BloomFilter> modifiedFiles = new ArrayList<>();

        // Remove chunks that have no replicas
        for (ChunkStoredInNode chunk : modifiedChunks) {
            if (chunk.getReplicaCount() == 0) {
                BloomFilter file = files.get(chunk.getFilename());
                file.removeChunk(chunk.getSequenceNo());
                modifiedFiles.add(file);
            }
        }

        // Remove files that have no chunks
        for (BloomFilter file : modifiedFiles) {
            if (file.getChunkCount() == 0) {
                files.remove(file.getFilename());
            }
        }
    }

    /**
     * @param filename
     * @param sequenceNo
     * @param storageNode Informing data storage one SN has  the particular filename and chunks respectively
     */
    public synchronized void publishChunk(String filename, int sequenceNo, SocketConnection storageNode) {
        try {
            //TODO Convert into BloomFilter
            if (!files.containsKey(filename)) {
                files.put(filename, new BloomFilter(100, 3, filename));
            }

            BloomFilter file = files.get(filename);
            if (file == null) {
                logger.error("BloomFilter Error : File is not available in the Map");
                return;
            }else{
                if(!file.get(filename.getBytes())){
                    logger.error("BloomFilter Error : File is not available in bloom filter");
                    return;
                }
            }
            if (!file.hasChunk(sequenceNo)) {
                file.addChunk(new ChunkStoredInNode(filename, sequenceNo));
            }
            //Gives all rthe replica location for particular chunk seqNo (2,3,5 SN)
            Set<SocketConnection> replicaLocations = file.getChunk(sequenceNo).getReplicaLocations();
            if (replicaLocations == null) {
                logger.error("No replicas available");
            }
            if (!replicaLocations.contains(storageNode)) {
                replicaLocations.add(storageNode);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }

    }

    public synchronized void getMissingChunks(String filename, List<Integer> sequenceNo, SocketConnection storageNode) {
        try {

            if (files.containsKey(filename)) {
                BloomFilter file = files.get(filename);
                if (file == null) {
                    logger.error("File is not available");
                    return;
                }
                for (int i : sequenceNo) {
                    logger.info("Missing file.getChunk(i) value :: " + file.getChunk(i));
                }
            } else {
                logger.error("No file");
            }

        } catch (Exception exception) {
            exception.printStackTrace();
        }

    }

    public synchronized SortedSet<String> getFilenames() {
        return new TreeSet<>(files.keySet());
    }

    public synchronized BloomFilter getFile(String filename) {
        BloomFilter bloomFilter = files.get(filename);
        if(bloomFilter == null){

        }else{
            boolean fileCheck = bloomFilter.get(filename.getBytes());
            if(fileCheck){
                return files.get(filename);
            }
        }
        return null;
    }

    public void onChunkCorrupted(String filename, int sequenceNo, SocketConnection storageNode) {
        try {
            if (!files.containsKey(filename)) return;

            ChunkStoredInNode chunk = files.get(filename).getChunk(sequenceNo);
            chunk.getReplicaLocations().remove(storageNode);
            cleanup(Collections.singletonList(chunk));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
