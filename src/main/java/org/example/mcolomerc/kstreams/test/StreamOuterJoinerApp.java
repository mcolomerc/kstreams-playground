package org.example.mcolomerc.kstreams.test;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamOuterJoinerApp extends StreamApp {

    static Logger logger = LoggerFactory.getLogger(StreamOuterJoinerApp.class.getName());

    public static final String LEFT_TOPIC  = "left-topic";
    public static final String RIGHT_TOPIC  = "right-topic";
    public static final String OUT_TOPIC  = "left-right-topic";

    public static void main(String[] args) throws Exception {
        Properties extraProperties = new Properties();
        try {
            StreamOuterJoinerApp streamApp = new StreamOuterJoinerApp();
            extraProperties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                    CreatedAtTimestampExtractor.class.getName());
            extraProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "outer-joiner-app");
            streamApp.run(args, extraProperties);
        } catch (Exception e) {
            System.out.println(e);
        }

    }

    @Override
    public void buildTopology(StreamsBuilder builder) throws ExecutionException, InterruptedException {
        KStream<String, String> leftStream = builder.stream(LEFT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withName("left-store"));

        KStream<String, String> rightStream = builder.stream(RIGHT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withName("right-store"));

        ValueJoiner<String, String, String> joiner = (leftValue,
                                                      rightValue) -> "left :: " + leftValue + ", right :: " + rightValue;
        // Perform the outer join
        KStream<String, String> joinedStream = leftStream.outerJoin(
                rightStream,
                joiner,
                JoinWindows.of(Duration.ofSeconds(1))
        );
        //Out
        joinedStream.to(OUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    }

}