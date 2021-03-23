package edu.usfca.cs.chat;

import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MessageQueue {

    private List<ChatMessages.MessageWrapper> messages = new LinkedList<>();
    private Lock listLock = new ReentrantLock();
    private static final Logger logger = Logger.getLogger(MessageQueue.class);

    private Semaphore waitForNextMessageSema = new Semaphore(0);

    //TODO how semaphore works ?

    /**
     * @param msg As we can't share socket connections with multiple thread
     *            we are creating messagequeue
     */
    public void queue(ChatMessages.MessageWrapper msg) {
        logger.info("Inside queue message with msg " + msg);
        listLock.lock();
        messages.add(msg);
        listLock.unlock();
        waitForNextMessageSema.release();
    }

    /**
     * Get next, waiting until available
     *
     * @return oldest message in queue
     * @throws InterruptedException in case of synchronization issues
     */
    public ChatMessages.MessageWrapper next() throws InterruptedException {
        //logger.info("Inside next message of messageQueue class ");
        do {
            waitForNextMessageSema.acquire();
        } while (messages.size() == 0);

        //logger.info("Inside messages.size() value : " + messages.size());
        listLock.lock();
        try {
            ChatMessages.MessageWrapper msg = messages.get(0);
            messages.remove(0);
            //logger.info("Inside next method with value msg : " + msg);
            return msg;
        } finally {
            listLock.unlock();
        }
    }
}
