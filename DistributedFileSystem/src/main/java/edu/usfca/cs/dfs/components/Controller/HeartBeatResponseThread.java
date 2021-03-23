package edu.usfca.cs.dfs.components.Controller;

import edu.usfca.cs.dfs.components.Compiler;
import edu.usfca.cs.dfs.components.DataStructure.SocketConnection;
import edu.usfca.cs.dfs.components.filterAndStorage.DataStorage;
import org.apache.log4j.Logger;

import java.util.*;

public class HeartBeatResponseThread implements Runnable {

    private Set<SocketConnection> allStorageNode;
    private Map<SocketConnection, Date> heartbeats;
    private final DataStorage dataStorage;
    private static Logger logger = Logger.getLogger(HeartBeatResponseThread.class);

    public HeartBeatResponseThread(Set<SocketConnection> allStorageNode, Map<SocketConnection, Date> heartbeats, DataStorage dataStorage) {
        this.allStorageNode = allStorageNode;
        this.heartbeats = heartbeats;
        this.dataStorage = dataStorage;
    }

    @Override
    public void run() {
        int responseInterval = Compiler.responseInterval;
        int expireInterval = Compiler.expireInterval;
        try {
            while (true) {

                List<SocketConnection> deleteSNList = new ArrayList<>();

                for (Map.Entry<SocketConnection, Date> entry : heartbeats.entrySet()) {
                    Date lastHeartbeat = entry.getValue();
                    Date now = new Date();
                    long differenceMilliseconds = now.getTime() - lastHeartbeat.getTime();
                    if (differenceMilliseconds > expireInterval) {
                        SocketConnection storageNode = entry.getKey();
                        logger.info(storageNode.getHost() + " is idle for " + differenceMilliseconds / 1000 + " seconds...");
                        if (allStorageNode.contains(storageNode)) {
                            logger.info("Remove the storage nodes from the active storage node lists, might take few seconds");
                            allStorageNode.remove(storageNode);
                        }
                        //TODO get the replication for the storage node for up and running
                        dataStorage.onStorageNodeOffline(storageNode);
                        deleteSNList.add(storageNode);
                    }
                }

                for (SocketConnection storageNode : deleteSNList) {
                    heartbeats.remove(storageNode);
                }

                Thread.sleep(responseInterval);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
