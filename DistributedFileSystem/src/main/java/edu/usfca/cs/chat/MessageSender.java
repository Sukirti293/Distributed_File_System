package edu.usfca.cs.chat;

import edu.usfca.cs.dfs.components.DataStructure.SocketConnection;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;

public class MessageSender implements Runnable {
    private static final Logger logger = Logger.getLogger(MessageSender.class);

    private final SemaphoreLocking semaphoreLocking;
    private final Map<SocketConnection, MessageQueue> messageQueues;
    private Socket mSocket;

    public MessageSender(SemaphoreLocking semaphoreLocking, Map<SocketConnection, MessageQueue> messageQueues) {
        this.semaphoreLocking = semaphoreLocking;
        this.messageQueues = messageQueues;
    }


    @Override
    public void run() {
        SocketConnection storageNode = semaphoreLocking.getSocketConnection();
        MessageQueue messageQueue = messageQueues.get(storageNode);
        try {
            Socket socket = getSocket(storageNode);
            while (!socket.isClosed()) {
                try {
                    ChatMessages.MessageWrapper msg = messageQueue.next();
                    //logger.trace("Sending message to " + socket.getRemoteSocketAddress() + ": " + msg);
                    msg.writeDelimitedTo(socket.getOutputStream());
                } catch (InterruptedException e) {
                    logger.error("Could not get next message from queue", e);
                } catch (IOException e) {
                    logger.error("Could not send message", e);
                    try {
                        socket.close();
                    } catch (IOException ioe) {
                        logger.error("Could not close mSocket", ioe);
                    } // try close mSocket
                } // try message send
            } // while
        } catch (IOException e) {
            logger.error("Could not create mSocket to " + storageNode);
        } // try get mSocket
    } // end of method run()

    private Socket getSocket(SocketConnection storageNodeAddress) throws IOException {
        if (mSocket == null || mSocket.isClosed()) {
            mSocket = storageNodeAddress.getSocket();
        }
        return mSocket;
    }
}
