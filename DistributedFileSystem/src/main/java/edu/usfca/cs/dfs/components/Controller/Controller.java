package edu.usfca.cs.dfs.components.Controller;

import edu.usfca.cs.chat.MessageQueue;
import edu.usfca.cs.chat.MessageSender;
import edu.usfca.cs.chat.SemaphoreLocking;
import edu.usfca.cs.dfs.components.DataStructure.SocketConnection;
import edu.usfca.cs.dfs.components.filterAndStorage.DataStorage;
import edu.usfca.cs.dfs.components.replication.ReplicaThread;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;


public class Controller {

    private final int port;

    private final Set<SocketConnection> addresses = new HashSet<>();
    private final Map<SocketConnection, Date> heartbeats = new HashMap<>();
    private final Map<SocketConnection, MessageQueue> messageQueues = new HashMap<>();
    private static DataStorage dataStorage;
    private static final Logger logger = Logger.getLogger(Controller.class);


    public Controller(int port) {
        this.port = port;
    }

    /**
     * @return Display the controller host and port name
     */
    public String getControllerHostName() {
        try {
            String controllerHostName = "";
            controllerHostName = InetAddress.getLocalHost().getHostName();
            //logger.info("Cntrl Host is :: " + controllerHostName + " port is :: " + port);
            return controllerHostName;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param args
     * @throws Exception Takes port for controller
     */
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        if (args.length != 1) {
            System.err.println("Argument required : port");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        new Controller(port).connect();
    }

    /**
     * connect method starts the haertbeat tracker which keeps the heartbeats of a storagenode
     * start the controller server with the given port
     * controller response thread to send information to client
     */
    @SuppressWarnings("InfiniteLoopStatement")
    public void connect() {
        try {
            getControllerHostName();

            dataStorage = new DataStorage();

            //Create replicas : one time call
            ReplicaThread replicas = new ReplicaThread(addresses, dataStorage, messageQueues);
            Thread replicationThread = new Thread(replicas);
            replicationThread.start();

            //Runnable thread created for heartbeat for all the storage nodes : one time call
            HeartBeatResponseThread heartBeatResponse = new HeartBeatResponseThread(addresses, heartbeats, dataStorage);
            Thread heartBeatResponseThread = new Thread(heartBeatResponse);
            heartBeatResponseThread.start();


            ServerSocket serverSocket = new ServerSocket(port);
            while (true) {
                Socket socket = serverSocket.accept();


                //Transfer information from controller to client
                //basically send the list of storagenode information to client

                //Need to use some locking mechanism as while uploading all the SNs are facing race condition
                SemaphoreLocking semaphoreLocking = new SemaphoreLocking();

                ControllerResponseThread controllerResponseThread = new ControllerResponseThread(semaphoreLocking, addresses, heartbeats, messageQueues, dataStorage, socket);
                Thread controllerToClient = new Thread(controllerResponseThread);
                controllerToClient.start();


                //Thread.sleep(1000);
                MessageSender messageSender = new MessageSender(semaphoreLocking, messageQueues);
                Thread messageSenderThread = new Thread(messageSender);
                messageSenderThread.start();
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            //System.out.println("Exception occurred in Controller socket connection " + exception.toString());
        }

    }
}
