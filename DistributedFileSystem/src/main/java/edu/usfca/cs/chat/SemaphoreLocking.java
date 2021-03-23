package edu.usfca.cs.chat;

import edu.usfca.cs.dfs.components.DataStructure.SocketConnection;
import org.apache.log4j.Logger;

import java.util.concurrent.Semaphore;

public class SemaphoreLocking {


    private static final Logger logger = Logger.getLogger(SemaphoreLocking.class);
    private SocketConnection socketConnection;
    private Semaphore semaphore = new Semaphore(0);

    public SocketConnection getSocketConnection() {
        //logger.info("inside getSocketConnection with socketConnection " + socketConnection);
        while (socketConnection == null) {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                logger.error("Could not acquire semaphore for storage", e);
            }
        }

        return socketConnection;
    }

    public void setSocketConnection(SocketConnection socketConnection) {
        if (this.socketConnection == null) {
            this.socketConnection = socketConnection;
            //logger.info("inside setSocketConnection with host " + socketConnection.getHost());
            semaphore.release();
        } else {
            this.socketConnection = socketConnection;
        }
    }

}
