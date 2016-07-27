package org.bigdata.workshop;

import java.util.StringJoiner;
import java.util.concurrent.BlockingQueue;

/**
 * Created by opetridean on 7/26/16.
 */
public class TwiterKafkaBridge {

    private CustomKafkaProducer customKafkaProducer;
    private BlockingQueue<String> tweetsQueue;


    public TwiterKafkaBridge(CustomKafkaProducer customKafkaProducer, BlockingQueue<String> tweetsQueue) {
        this.customKafkaProducer = customKafkaProducer;
        this.tweetsQueue = tweetsQueue;
    }

    /**
     * Produce for the topic brexit
     *
     * @throws InterruptedException
     */
    public void producerForBrexit() throws InterruptedException {
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            String msg = tweetsQueue.take();
            customKafkaProducer.send("brexit", "", msg);
            System.out.println(msg);
        }
    }

    /**
     * Produce for the topic workshop
     *
     * @throws InterruptedException
     */
    public void producerForWorkshop() throws InterruptedException {
        //Add code here
    }


}
