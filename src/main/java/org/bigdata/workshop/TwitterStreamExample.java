package org.bigdata.workshop;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by opetridean on 7/26/16.
 */
public class TwitterStreamExample {

    public static void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        endpoint.trackTerms(Lists.newArrayList("#Brexit"));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        // Authentication auth = new BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            String msg = queue.take();
            System.out.println(msg);
        }

        client.stop();
    }

    public BlockingQueue<String> getStreamQueue(Properties properties, List<String> terms) {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // add some track terms
        endpoint.trackTerms(terms);

        Authentication auth = new OAuth1(properties.getProperty("twitter.consumer.api.key"), properties.getProperty("twitter.consumer.api.secret"),
                properties.getProperty("twitter.consumer.access.token"), properties.getProperty("twitter.consumer.access.secret"));
        // Authentication auth = new BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();

        return queue;
    }


    public static void main(String[] args) {
        try {
            TwitterStreamExample.run("hIjSRdv6WMlSetmTMdN5sdJdz", "4x3hhvux1UPzvrOprUhPGYSPr8lc80gQKx0GAZesx0WTNiw7dZ",
                    "730361888014479360-JWBRI7GvFjFvFGRIkwtDZDUsbB9epoW", "8dopHoNlqgEBNcoK1NBjSQG3BXZYUzL4RnOq9pWv3G6xt");
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }



}
