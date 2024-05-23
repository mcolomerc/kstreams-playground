package org.example.mcolomerc.kstreams.test;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class Producer {
    private static Logger logger = LogManager.getLogger(Producer.class);
    public static final String LEFT_TOPIC  = "left-topic";
    public static final String RIGHT_TOPIC  = "right-topic";
    private static final Properties props = new Properties();

    public static void main(final String[] args) throws IOException {
        try (InputStream input = Producer.class.getClassLoader().getResourceAsStream("confluent.properties")) {
            // load a properties file
            props.load(input);
            // get the property value and print it out
            logger.info(props.getProperty("bootstrap.servers"));
        } catch (IOException ex) {
            System.out.println("IOException");
            System.out.println (ex.getMessage());
            ex.printStackTrace();
        }
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        logger.info("Producer started");

        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)) {
            int i = 100;
            // 0
            produceRecord (producer, LEFT_TOPIC, String.valueOf(i), "LEFT " + Long.toString(i) + " - " + Timestamp.from(Instant.now()));
            produceRecord (producer, RIGHT_TOPIC, String.valueOf(i), "RIGHT " + Long.toString(i) + " - " + Timestamp.from(Instant.now()));
            i= i +1; // 1
            Thread.sleep(1000L);
            //---
            produceRecord (producer, LEFT_TOPIC, String.valueOf(i), "LEFT " + Long.toString(i) + " - " + Timestamp.from(Instant.now()));
            i= i +1; // 2
            Thread.sleep(2000L);
            //---
            produceRecord (producer, RIGHT_TOPIC, String.valueOf(i), "RIGHT " + Long.toString(i) + " - " + Timestamp.from(Instant.now()));
            produceRecord (producer, RIGHT_TOPIC, String.valueOf(i), "RIGHT " + Long.toString(i) + " - " + Timestamp.from(Instant.now()));
            i= i +1;   // 3
            Thread.sleep(2000L);
            //---
            produceRecord (producer, LEFT_TOPIC, String.valueOf(i), "LEFT " + Long.toString(i) + " - " + Timestamp.from(Instant.now()));
            i= i +1;   // 4
            Thread.sleep(6000L);
            //---
            produceRecord (producer, RIGHT_TOPIC, String.valueOf(i), "RIGHT " + Long.toString(i) + " - " + Timestamp.from(Instant.now()));
            produceRecord (producer, LEFT_TOPIC, String.valueOf(i), "LEFT " + Long.toString(i) + " - " + Timestamp.from(Instant.now()));

            producer.flush();
            logger.info(String.format("Successfully produced messages to topics called :: %s | %s", LEFT_TOPIC, RIGHT_TOPIC));
        } catch (final Exception e) {
            logger.error("Exception");
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private static void produceRecord (KafkaProducer <String, String> producer, String topic, String key, String payload) {
        try {
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, payload);
            logger.info(String.format("Producer :: Topic: %s Key %s: - Value: %s :: ", topic, key, payload));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata m, Exception e) {
                    if (e != null) {
                        logger.error("onCompletion exception");
                        logger.error(e.getMessage());
                        e.printStackTrace();
                    } else {
                        logger.info(String.format("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset()));
                    }
                }
            });
            Random random = new Random();
            long wait = random.nextInt(10 + 1 - 1) + 1;
            logger.info("wait(ms): " + wait);
            Thread.sleep(wait);
        } catch (Exception e) {
            System.out.println (e.getMessage());
            e.printStackTrace();
        }
    }

}
