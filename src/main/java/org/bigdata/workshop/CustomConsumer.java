package org.bigdata.workshop;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by opetridean on 7/26/16.
 */
public class CustomConsumer implements Runnable {
    Properties props;

    public CustomConsumer(Properties properties) {
        this.props = properties;
    }

    @Override
    public void run() {
        long threadId = Thread.currentThread().getId();

        System.out.println("Started consumer on thread " + threadId + " with group id " + props.getProperty("group.id"));
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("workshop"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("thread = %d offset = %d, key = %s, value = %s \n", threadId, record.offset(), record.key(), record.value());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        URL url = Resources.getResource("twitter-kafka.properties");
        final ByteSource byteSource = Resources.asByteSource(url);
        Properties properties = new Properties();
        InputStream inputStream = byteSource.openBufferedStream();
        properties.load(inputStream);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(new CustomConsumer(properties));
    }
}
