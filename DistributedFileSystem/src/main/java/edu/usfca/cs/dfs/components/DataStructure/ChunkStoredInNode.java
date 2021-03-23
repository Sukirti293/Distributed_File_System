package edu.usfca.cs.dfs.components.DataStructure;


import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Provides the info about chunk location
 * i.e. chunks are stored in a SN
 * This is just tell controller which chunks stored in which storage node
 * during download, so that client will for and fetch the files from that SN
 */
public class ChunkStoredInNode implements Comparable<ChunkStoredInNode> {

    private final String filename;
    private final int sequenceNo;
    private Set<SocketConnection> replicaLocations = new TreeSet<>();

    public ChunkStoredInNode(String filename, int sequenceNo) {
        this.filename = filename;
        this.sequenceNo = sequenceNo;
    }

    public int getReplicaCount() {
        return replicaLocations.size();
    }

    public Set<SocketConnection> getReplicaLocations() {

        return replicaLocations;
    }

    public String getFilename() {
        return filename;
    }

    public int getSequenceNo() {
        return sequenceNo;
    }

    @Override
    public String toString() {
        return "ChunkStoredInNode{" +
                "filename='" + filename + '\'' +
                ", sequenceNo=" + sequenceNo +
                ", replicaLocations=" + replicaLocations +
                '}';
    }

    @Override
    public int compareTo(ChunkStoredInNode o) {
        if (!this.filename.equals(o.filename)) {
            return this.filename.compareTo(o.filename);
        }
        return Integer.compare(this.sequenceNo, o.sequenceNo);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChunkStoredInNode chunkRef = (ChunkStoredInNode) o;
        return sequenceNo == chunkRef.sequenceNo &&
                Objects.equals(filename, chunkRef.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, sequenceNo);
    }


}
