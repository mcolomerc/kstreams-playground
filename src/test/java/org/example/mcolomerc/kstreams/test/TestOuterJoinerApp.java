package org.example.mcolomerc.kstreams.test;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOuterJoinerApp {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> leftTopic;
    private TestInputTopic<String, String> rightTopic;
    private TestOutputTopic<String, String> outputTopic;

    private final Instant recordBaseTime = Instant.parse("2019-06-01T10:00:00Z");
    private final Duration advance1Min = Duration.ofMinutes(1);

    final List<TestRecord<String, String>> leftRecords = List.of(
            new TestRecord("1", "h", null, 1000L),
            new TestRecord("2", "e", null, 2000L),
            new TestRecord("3", "l", null, 3000L),
            new TestRecord("3", "l", null, 4000L),
            new TestRecord("4", "o", null, 5000L),
            new TestRecord("5", "w", null, 6000L),
            new TestRecord("4", "o", null, 7000L),
            new TestRecord("1", "h", null, 15000L)
    );

    final List<TestRecord<String, String>> rightRecords = List.of(
            new TestRecord("1", "h", null, 1000L),
            new TestRecord("2", "e", null, 2000L),
            new TestRecord("3", "l", null, 3000L),
            new TestRecord("3", "l", null, 4000L),
            new TestRecord("4", "o", null, 5000L),
            new TestRecord("5", "w", null, 6000L),
            new TestRecord("4", "o", null, 7000L),
            new TestRecord("1", "h", null, 15000L)
    );

    final List<TestRecord<String, String>> expectedRecords = List.of(
            new TestRecord("1-Window{startMs=1000, endMs=1000}", "h", null, Instant.ofEpochMilli(1L).plusMillis(1000L))
    );

    private Properties getConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                CreatedAtTimestampExtractor.class.getName());
        config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/aggregated-values");
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-outer-join-app");
        return config;
    }


    @Before
    public void setup() throws ExecutionException, InterruptedException {
        final StreamsBuilder builder = new StreamsBuilder();
        StreamOuterJoinerApp app = new StreamOuterJoinerApp();
        app.buildTopology(builder);

        testDriver = new TopologyTestDriver(builder.build(), getConfig());
        leftTopic = testDriver.createInputTopic(StreamOuterJoinerApp.LEFT_TOPIC, new StringSerializer(), new StringSerializer(),
                recordBaseTime, advance1Min);
        rightTopic = testDriver.createInputTopic(StreamOuterJoinerApp.RIGHT_TOPIC, new StringSerializer(), new StringSerializer(),
                recordBaseTime, advance1Min);

        outputTopic = testDriver.createOutputTopic(StreamOuterJoinerApp.OUT_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    @Test
    public void shouldConcatenateWithReduce() {
        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(createTopology(), getConfig(), Instant.ofEpochMilli(1L))) {

            leftRecords.forEach((record -> {
                leftTopic.pipeInput(record.key(), record.value(), record.timestamp());
                topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(1));
            }));

            rightRecords.forEach((record -> {
                rightTopic.pipeInput(record.key(), record.value(), record.timestamp());
                topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(1));
            }));

            var actualRecords = outputTopic.readRecordsToList();
            actualRecords.forEach(System.out::println);
            assertThat(actualRecords).hasSameElementsAs(expectedRecords);

        }
    }
}
