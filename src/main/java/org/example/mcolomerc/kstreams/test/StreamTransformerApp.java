package org.example.mcolomerc.kstreams.test;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class StreamTransformerApp {
    private static Logger logger =  LoggerFactory.getLogger(StreamTransformerApp.class.getName());
    public static final String LEFT_TOPIC  = "left-topic";
    public static final String RIGHT_TOPIC  = "right-topic";
    public static final String OUT_TOPIC  = "left-right-topic";
    protected static Properties properties;
    protected static KafkaStreams kafkaStreams;

    public static void main(String[] args) throws Exception {
        logger.info ("Starting Stream App...");
        try {
            properties = new Properties();
            if (args.length > 0) {
                properties.load(new FileInputStream(args[0]));
            } else {
                properties.load(StreamTransformerApp.class.getResourceAsStream("/confluent.properties"));
            }
            logger.info (String.valueOf(properties));
            System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
            StreamOuterJoinerApp streamApp = new StreamOuterJoinerApp();
            properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                    CreatedAtTimestampExtractor.class.getName());
            // Default serde for keys of data records (here: built-in serde for String type)
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            // Default serde for values of data records (here: built-in serde for Long type)
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "outer-joiner-app");
            StreamsBuilder builder = new StreamsBuilder();
            buildTopology(builder);
            Topology topology = builder.build();
            logger.info(topology.describe().toString());

            kafkaStreams = new KafkaStreams(topology, properties);
            kafkaStreams.setUncaughtExceptionHandler((e) -> {
                logger.error(null, e);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            });
            kafkaStreams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }


    public static void buildTopology(StreamsBuilder builder) throws ExecutionException, InterruptedException {
        logger.info ("Building topology...");
        KStream<String, String> leftStream = builder.stream(LEFT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withName("left-store"));

        KStream<String, String> rightStream = builder.stream(RIGHT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withName("right-store"));

        // Create a state store for the processor
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("store"),
                Serdes.String(),
                Serdes.String()));

        // Perform the outer join
        KStream<String, String> joinedStream = leftStream.outerJoin(
                rightStream,
                (leftValue, rightValue) -> {
                    String left = leftValue != null ? leftValue : "null";
                    String right = rightValue != null ? rightValue : "null";
                    return "left :: " + left + ", right :: " + right;
                },
                JoinWindows.of(Duration.ofSeconds(5)), // window size
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        // Add the custom processor to the topology
        joinedStream.process(() -> new ProcessorJoiner(), "store")
                .peek((key, value) -> logger.info("Generated record key: " + key + " value:  " + value))
                .to(OUT_TOPIC);


    }
}

