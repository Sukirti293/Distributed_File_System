package edu.usfca.cs.dfs.components.filterAndStorage;

import com.sangupta.murmur.Murmur1;
import com.sangupta.murmur.Murmur3;
import edu.usfca.cs.dfs.components.Controller.ControllerResponseThread;
import edu.usfca.cs.dfs.components.DataStructure.ChunkStoredInNode;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.text.DecimalFormat;
import java.util.*;

public class BloomFilter implements Comparable<BloomFilter> {

    private long numberOfBits;
    private int numberOfHashs;
    private BitSet bitSet;
    private Random random;
    private int[] hashSeed;
    private DecimalFormat decimalFormat;
    private int insertionCounter = 0;
    private final Map<Integer, ChunkStoredInNode> chunks = new HashMap<>();
    private final String filename;
    private Map<String, BitSet> bloomFilterMap = new TreeMap<>();
    private static final Logger logger = Logger.getLogger(BloomFilter.class);

    public BloomFilter(int numberOfBits, int numberOfHashs, String filename) {
        this.numberOfBits = numberOfBits;
        this.numberOfHashs = numberOfHashs;
        bitSet = new BitSet(numberOfBits);
        hashSeed = new int[numberOfHashs];
        decimalFormat = new DecimalFormat("0.00");
        random = new Random();
        generateRandomizedSeed(hashSeed);
        this.filename = filename;
        if(!get(filename.getBytes())){
            putFileName(filename.getBytes());
        }
    }

    void putFileName(byte[] data) {
        for (int i = 0; i < numberOfHashs; i++) {
            long hashVal = Murmur3.hash_x86_32(data, data.length, hashSeed[i]);
            bitSet.set((int) (Math.abs(hashVal) % numberOfBits), true);
        }
        //System.out.println(bitSet);
        insertionCounter = insertionCounter + 1;
        //System.out.println("Error Percentage :: " + decimalFormat.format(falsePositiveProb() * 100) + "%");
    }

    void put(String filename, byte[] data) {
        for (int i = 0; i < numberOfHashs; i++) {
            long hashVal = Murmur3.hash_x86_32(data, data.length, hashSeed[i]);
            bitSet.set((int) (Math.abs(hashVal) % numberOfBits), true);
            bloomFilterMap.put(filename, bitSet);
        }
        //System.out.println(bitSet);
        insertionCounter = insertionCounter + 1;
        //System.out.println("Error Percentage :: " + decimalFormat.format(falsePositiveProb() * 100) + "%");
    }

    boolean get(byte[] data) {
        for (int i = 0; i < numberOfHashs; i++) {
            long hashVal = Murmur3.hash_x86_32(data, data.length, hashSeed[i]);
            if (!bitSet.get((int) (Math.abs(hashVal) % numberOfBits))) {
                return false;
            }
        }
        return true;
    }

    float falsePositiveProb() {
        float rate = 0;
        rate = (float) 1 / (float) numberOfBits;
        float val = (float) Math.pow((1 - rate), numberOfHashs * insertionCounter);
        rate = (float) Math.pow((1 - val), numberOfHashs);
        //System.out.println("Error rate :: " + rate);
        return rate;
    }

    private void generateRandomizedSeed(int[] hashSeed) {
        for (int i = 0; i < hashSeed.length; i++) {
            hashSeed[i] = random.nextInt();
        }
    }

    public SortedSet<ChunkStoredInNode> getChunks() {
        return new TreeSet<>(chunks.values());
    }

    public ChunkStoredInNode getChunk(int sequenceNo) {
        return chunks.get(sequenceNo);
    }

    public void addChunk(ChunkStoredInNode chunkRef) {
        this.chunks.put(chunkRef.getSequenceNo(), chunkRef);
    }

    public boolean hasChunk(int sequenceNo) {
        return this.chunks.containsKey(sequenceNo);
    }

    public int getChunkCount() {
        return this.chunks.size();
    }

    @Override
    public int compareTo(BloomFilter o) {
        return this.filename.compareTo(o.filename);
    }

    public String getFilename() {
        return filename;
    }

    public void removeChunk(int sequenceNo) {
        chunks.remove(sequenceNo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BloomFilter dfsFile = (BloomFilter) o;
        return Objects.equals(filename, dfsFile.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename);
    }


//    public SortedSet<ChunkStoredInNode> getChunks() {
//        return new TreeSet<>(chunks.values());
//    }
//
//    public int getChunkCount() {
//        return this.chunks.size();
//    }
//
//    public void removeChunk(int sequenceNo) {
//        chunks.remove(sequenceNo);
//    }
//
//    public String getFilename() {
//        return filename;
//    }
//
//    public ChunkStoredInNode getChunk(int sequenceNo) {
//        return chunks.get(sequenceNo);
//    }
//
//    public void addChunk(ChunkStoredInNode chunkRef) {
//        put(chunkRef.getFilename(), intToByteArray(chunkRef.getSequenceNo()));
//        this.chunks.put(chunkRef.getSequenceNo(), chunkRef);
//    }
//
//
    private byte[] intToByteArray(final int i) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeInt(i);
            dos.flush();
        } catch (Exception exception) {
            logger.error("Exception in intToByteArray conversion " + exception.toString());
        }
        return bos.toByteArray();
    }


}
