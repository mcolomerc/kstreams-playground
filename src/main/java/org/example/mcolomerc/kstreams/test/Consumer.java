package org.example.mcolomerc.kstreams.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private static Logger logger = LogManager.getLogger(Consumer.class);
    private static final String TOPIC = "left-right-topic";
    private static final Properties props = new Properties();

    public static void main(final String[] args) throws IOException {
        try (InputStream input = Consumer.class.getClassLoader().getResourceAsStream("confluent.properties")) {
            // load a properties file
            props.load(input);
            // get the property value and print it out
            System.out.println(props.getProperty("bootstrap.servers"));
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        logger.debug("Producer started");
        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(Arrays.asList(TOPIC));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    logger.info(
                            String.format("Consumed event from topic %s: key = %-10s value = %s", TOPIC, key, value));
                }
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}

