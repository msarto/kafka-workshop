package org.bigdata.workshop;

import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

/**
 * Created by opetridean on 7/26/16.
 */
public class TwitterKafkaExample {


    public static void main(String[] args) throws IOException, InterruptedException {
        URL url = Resources.getResource("twitter-kafka.properties");
        final ByteSource byteSource = Resources.asByteSource(url);

        Properties properties = new Properties();
        InputStream inputStream = byteSource.openBufferedStream();
        properties.load(inputStream);

        TwitterStreamExample twitterStreamExample = new TwitterStreamExample();
        CustomKafkaProducer customKafkaProducer = new CustomKafkaProducer(properties);
        BlockingQueue<String> tweetsQueue = twitterStreamExample.getStreamQueue(properties, Lists.newArrayList("#Brexit"));

        TwiterKafkaBridge twiterKafkaBridge = new TwiterKafkaBridge(customKafkaProducer, tweetsQueue);
        twiterKafkaBridge.producerForBrexit();

    }
}
