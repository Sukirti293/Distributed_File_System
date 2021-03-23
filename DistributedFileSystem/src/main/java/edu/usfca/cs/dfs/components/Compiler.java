package edu.usfca.cs.dfs.components;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;

public class Compiler {

    public static final int totalNoOfReplicas = 3;
    public static final String clientChunkFolder = "/tmp/ClientChunks";
    public static final String storageNodeChunkFolder = "/tmp/SNChunks";
    public static final int responseInterval = 5000;
    public static final int expireInterval = 50000;
    public static final int threadsInExecutor = 4;
    public static final int replicaCheckDuration = 10000;

    public static <E> Set<E> chooseNrandomOrMin(int n, Set<E> set) {
        List<E> list = new ArrayList<>(set);
        if (n >= list.size()) {
            return new HashSet<>(set);
        }
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, n));
    }

    public static boolean checkFileIfExists(String filename) {
        String path = Paths.get("").toAbsolutePath().toString() + "/" + filename;
        File file = new File(path);
        if (file.exists())
            return true;
        else
            return false;
    }

    public static String byteToHumanFormat(long size){
        long n = 1000;
        String s = "";
        double kb = size / n;
        double mb = kb / n;
        double gb = mb / n;
        double tb = gb / n;
        if(size < n) {
            s = size + " Bytes";
        } else if(size >= n && size < (n * n)) {
            s =  String.format("%.2f", kb) + " KB";
        } else if(size >= (n * n) && size < (n * n * n)) {
            s = String.format("%.2f", mb) + " MB";
        } else if(size >= (n * n * n) && size < (n * n * n * n)) {
            s = String.format("%.2f", gb) + " GB";
        } else if(size >= (n * n * n * n)) {
            s = String.format("%.2f", tb) + " TB";
        }
        return s;

    }
}
