package org.bigdata.workshop;

import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by opetridean on 7/26/16.
 */
public class CustomKafkaProducer implements Runnable {
    private Properties props;
    private Producer<String, String> producer;

    public CustomKafkaProducer(Properties properties) {
        this.props = properties;
        producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("workshop", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();
    }

    public void send(String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    public static void main(String[] args) throws IOException {
        URL url = Resources.getResource("twitter-kafka.properties");
        final ByteSource byteSource = Resources.asByteSource(url);
        Properties properties = new Properties();
        InputStream inputStream = byteSource.openBufferedStream();
        properties.load(inputStream);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(new CustomKafkaProducer(properties));
    }

}
