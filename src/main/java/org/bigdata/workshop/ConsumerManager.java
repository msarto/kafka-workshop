package org.bigdata.workshop;

import com.google.common.io.ByteSource;
import com.google.common.io.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by opetridean on 7/26/16.
 */
public class ConsumerManager {


    public static void main(String[] args) throws InterruptedException, IOException {
        ExecutorService executorService = Executors.newCachedThreadPool();

        URL url = Resources.getResource("twitter-kafka.properties");
        final ByteSource byteSource = Resources.asByteSource(url);
        Properties properties = new Properties();
        InputStream inputStream = byteSource.openBufferedStream();
        properties.load(inputStream);

        for (int i = 0; i < 10; i++) {
            //todo: allow time to generate custom stuff
            Thread.sleep(10);
            properties.put("group.id", System.nanoTime() + "");
            executorService.execute(new CustomConsumer(properties));
        }

    }

}
