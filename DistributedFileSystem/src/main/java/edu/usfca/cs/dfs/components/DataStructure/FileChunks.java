package edu.usfca.cs.dfs.components.DataStructure;

import edu.usfca.cs.dfs.components.Client.Client;
import edu.usfca.cs.dfs.components.Compiler;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;

public class FileChunks implements Comparable<FileChunks> {
    private final String fileName;
    private final long fileSize;
    private final int seqNumber;
    private String checksum;
    private final Path chunkLocalPath;
    public static Map<String, Integer> totalChunkCount = new HashMap<>();
    private static final Logger logger = Logger.getLogger(FileChunks.class);

    public String getFileName() {
        return fileName;
    }

    public long getFileSize() {
        return fileSize;
    }

    public int getSeqNumber() {
        return seqNumber;
    }

    public String getChecksum() {
        return checksum;
    }

    //TODO imp to check absolute filepath :: not working
    public Path getChunkLocalPath() {
        return chunkLocalPath;
    }

    public FileChunks(String filename, int seqNumber, long fileSize, String checksum, Path chunkLocalPath) {
        this.fileName = filename;
        this.seqNumber = seqNumber;
        this.checksum = checksum;
        this.fileSize = fileSize;
        this.chunkLocalPath = chunkLocalPath;
    }


    public static FileChunks[] createChunks(String filename, long chunkSize, String outputDir) throws IOException {
        try {
            int chunkCount;
            FileChunks[] chunks;
            FileInputStream fileInputStream;

            //TODO check file exists
            //Mandatory checks about the file and dir
            File fileToUpload = new File(filename);
            if (!fileToUpload.exists() && fileToUpload.isDirectory()) {
                throw new RuntimeException("File doesn't exist " + outputDir);
            }
            fileInputStream = new FileInputStream(fileToUpload);
            long fileSize = fileToUpload.length();
            if (fileToUpload.length() == 0) {
                logger.error("File is empty");
                throw new IllegalArgumentException("File is empty");
            }

            //to how many chunks for a particular file
            chunkCount = (int) (fileSize / chunkSize);
            if (fileSize % chunkSize != 0) {
                chunkCount++;
            }
            //totalChunkCount.put(filename, chunkCount);
            fileInputStream.close();

            //Write to the output dir and check
//            outputDir = Paths.get("").toAbsolutePath().toString() + outputDir;

            Path c = Paths.get(Compiler.clientChunkFolder);
            Path root = c.getRoot();
            outputDir = String.valueOf(root.resolve(Compiler.clientChunkFolder));
            System.out.println(outputDir);

            File dirFile = new File(outputDir);
            if (!dirFile.exists()) {
                if (!dirFile.mkdirs()) {
                    throw new RuntimeException("Cannot create directory " + outputDir);
                }
            }

            //creating an array of chunks with size of chunk count and checking for last chuck
            chunks = new FileChunks[chunkCount];
            fileInputStream = new FileInputStream(fileToUpload);
            int lastChunkNo = chunkCount - 1;

            //Create and write file chunks into a temporary directory  - to ensure that bytes are been passed to the chuck - entire chunk size
            int index = 1;
            for (int i = 0; i < lastChunkNo; i++) {
                Path chunkPath = Paths.get(outputDir, filename + "-chunk" + index);
                chunks[i] = new FileChunks(filename, index, chunkSize, "", chunkPath);
                writeToChunkFile(fileInputStream, chunkPath, chunkSize);
                chunks[i].calculateChecksum();
                index = index + 1;
            }

            long lastChunkSize = chunkSize;
            long chunksCapacity = chunkCount * chunkSize;
            if (chunksCapacity > fileSize) {
                lastChunkSize = chunkSize - (chunksCapacity - fileSize);
            }

            // extra chunk(.6MB)
            Path lastChunkFilePath = Paths.get(outputDir, filename + "-chunk" + index);
            chunks[lastChunkNo] = new FileChunks(filename, index, lastChunkSize, "", lastChunkFilePath);
            writeToChunkFile(fileInputStream, lastChunkFilePath, lastChunkSize);
            chunks[lastChunkNo].calculateChecksum();
            return chunks;

        } catch (Exception e) {
            System.err.println("FileChunks  :: " + e.toString());
            return null;
        }
    }

    private static void writeToChunkFile(FileInputStream fileInputStream, Path chunkPath, long chunkSize) throws IOException {
        Files.deleteIfExists(chunkPath);
        File chunkFile = Files.createFile(chunkPath).toFile();
        FileOutputStream fileOutputStream = new FileOutputStream(chunkFile);

        //4KB buffer size :: default size is being used
        //Create byte array to read data in chunks
        int bufferSize = 4096;
        byte[] buf = new byte[bufferSize];
        int bufferReads = (int) (chunkSize / bufferSize);
        if (chunkSize % bufferSize != 0) {
            bufferReads++;
        }
        int lastJ = bufferReads - 1;

        for (int j = 0; j < lastJ; j++) {
            if (fileInputStream.read(buf) == -1) {
                throw new IllegalStateException("Not enough data to read from file");
            }
            fileOutputStream.write(buf);
        }

        //capacity check of each chunk and make sure that all the data is been stored
        long lastChunkSize = bufferSize;
        long totalCapacity = bufferReads * bufferSize;
        if (totalCapacity > chunkSize) {
            lastChunkSize = bufferSize - (totalCapacity - chunkSize);
        }

        //last chunk with less data is been neglected initially and is written at the end
        int lastBufReadSize = (int) lastChunkSize;
        byte[] lastBuf = new byte[lastBufReadSize];
        if (fileInputStream.read(lastBuf, 0, lastBufReadSize) == -1) {
            throw new IllegalStateException("Not enough data to read from file");
        }
        fileOutputStream.write(lastBuf);
        fileOutputStream.close();
    }

    public void calculateChecksum() throws Exception {
        String md5checksumString = md5sum(chunkLocalPath.toFile());
        if (md5checksumString != null || !md5checksumString.isEmpty())
            this.checksum = md5checksumString;
        else
            throw new Exception("FileChunks :: Couldn't able to generate checksum");
    }

    /**
     * @param file
     * @return
     * @throws IOException Creating a md5 sum checksum
     */
    public static String md5sum(File file) throws IOException {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[1024];
            int c;
            FileInputStream fileInputStream = new FileInputStream(file);
            while ((c = fileInputStream.read(buf)) != -1) {
                md.update(buf, 0, c);
            }

            byte[] digest = md.digest();
            fileInputStream.close();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }

            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public static File createFile(SortedSet<FileChunks> chunks, String outputFilePathname) throws IOException {
        // Check first chunk is #0
        int firstSequenceNo = chunks.first().getSeqNumber();
        if (firstSequenceNo != 1) {
            throw new IllegalArgumentException("Chunk #0 could not be found");
        }

        // Check all chunks are here
        int lastSequenceNo = chunks.last().getSeqNumber();
        int nbChunksExpected = lastSequenceNo - firstSequenceNo + 1;
        if (chunks.size() != nbChunksExpected) {
            throw new IllegalArgumentException("Last sequence no is " + lastSequenceNo + " so should have " + nbChunksExpected + " chunks, but there are only " + chunks.size());
        }

        // Check all chunks have the same filename
        String filename = chunks.first().fileName;
        for (FileChunks chunk : chunks) {
            if (!chunk.fileName.equals(filename)) {
                throw new IllegalArgumentException("Not all chunks have the same filename.");
            }
        }

        // Assemble file
        // (could avoid iterating twice, but creates messy code)
        File outputFile = new File(outputFilePathname);
        if (outputFile.exists() && outputFile.length() != 0) {
            if (!outputFile.delete()) {
                throw new RuntimeException("Could not delete existing file");
            }
        }
        FileOutputStream fos = new FileOutputStream(outputFile);
        for (FileChunks chunk : chunks) {
            File chunkFile = chunk.getChunkLocalPath().toFile();
            FileInputStream fis = new FileInputStream(chunkFile);
            byte[] readBuf = new byte[4096];
            int c;
            while ((c = fis.read(readBuf)) != -1) {
                fos.write(readBuf, 0, c);
            }
            fis.close();
        }
        fos.close();

        return outputFile;
    }


    public boolean isCorrupted() {
        String oldSum = this.checksum;
        try {
            String newSum = md5sum(chunkLocalPath.toFile());
            return !oldSum.equals(newSum);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileChunks chunk = (FileChunks) o;
        return seqNumber == chunk.seqNumber &&
                Objects.equals(fileName, chunk.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, seqNumber);
    }

    @Override
    public int compareTo(FileChunks o) {
        if (!this.fileName.equals(o.fileName)) {
            return this.fileName.compareTo(o.fileName);
        }
        return Integer.compare(this.seqNumber, o.seqNumber);
    }

//    @Override
//    public String toString() {
//        return "Chunk[" + chunkLocalPath.toFile().getName() + "]";
//    }


    @Override
    public String toString() {
        return "FileChunks{" +
                "fileName='" + fileName + '\'' +
                ", fileSize=" + fileSize +
                ", seqNumber=" + seqNumber +
                ", checksum='" + checksum + '\'' +
                ", chunkLocalPath=" + chunkLocalPath +
                '}';
    }
}
