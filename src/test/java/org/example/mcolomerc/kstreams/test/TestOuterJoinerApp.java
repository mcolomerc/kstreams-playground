package org.example.mcolomerc.kstreams.test;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;

import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOuterJoinerApp {
    private TopologyTestDriver testDriver;
    final StreamsBuilder builder = new StreamsBuilder();
    private TestInputTopic<String, String> leftTopic;
    private TestInputTopic<String, String> rightTopic;
    private TestOutputTopic<String, String> outputTopic;
    private final Instant recordBaseTime = Instant.parse("2019-06-01T10:00:00Z");
    private final Duration advance1Sec = Duration.ofSeconds(1);

    final List<TestRecord<String, String>> expectedRecords = List.of(
            new TestRecord("1", "left :: left 1, right :: null", null, Instant.ofEpochMilli(1L).plusMillis(1000L)),
            new TestRecord("1", "left :: left 1, right :: right 1", null, Instant.ofEpochMilli(1L).plusMillis(2000L)),
            new TestRecord("1", "left :: null, right :: right 2", null, Instant.ofEpochMilli(1L).plusMillis(4000L)),
            new TestRecord("1", "left :: left 2, right :: right 2", null, Instant.ofEpochMilli(1L).plusMillis(4000L)),
            new TestRecord("1", "left :: null, right :: right 3", null, Instant.ofEpochMilli(1L).plusMillis(6000L)),
            new TestRecord("1", "left :: left 4, right :: null", null, Instant.ofEpochMilli(1L).plusMillis(8000L))
    );

    private Properties getConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                CreatedAtTimestampExtractor.class.getName());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-outer-join-app");
        return config;
    }

    @Before
    public void setup() throws ExecutionException, InterruptedException {
        try {
            StreamOuterJoinerApp app = new StreamOuterJoinerApp();
            app.buildTopology(builder);

            testDriver = new TopologyTestDriver(builder.build(), getConfig());
            leftTopic = testDriver.createInputTopic(StreamOuterJoinerApp.LEFT_TOPIC, new StringSerializer(), new StringSerializer(),
                    recordBaseTime, advance1Sec);
            rightTopic = testDriver.createInputTopic(StreamOuterJoinerApp.RIGHT_TOPIC, new StringSerializer(), new StringSerializer(),
                    recordBaseTime, advance1Sec);

            outputTopic = testDriver.createOutputTopic(StreamOuterJoinerApp.OUT_TOPIC, new StringDeserializer(), new StringDeserializer());
        } catch (Exception e) {
            System.out.println (e.getStackTrace());
        }
    }

    @Test
    public void shouldOuterJoin() {
        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), getConfig(), Instant.ofEpochMilli(1L))) {
            // window
            leftTopic.pipeInput(new TestRecord("1", "left 1", null, Instant.ofEpochMilli(1L).plusMillis(1000L)));
            rightTopic.pipeInput(new TestRecord("1", "right 1", null, Instant.ofEpochMilli(1L).plusMillis(2000L)));
            // window
            rightTopic.pipeInput(new TestRecord("1", "right 2", null, Instant.ofEpochMilli(1L).plusMillis(4000L)));
            leftTopic.pipeInput(new TestRecord("1", "left 2", null, Instant.ofEpochMilli(1L).plusMillis(4000L)));
            // window
            rightTopic.pipeInput(new TestRecord("1", "right 3", null, Instant.ofEpochMilli(1L).plusMillis(6000L)));
            // window 5
            leftTopic.pipeInput(new TestRecord("1", "left 4", null, Instant.ofEpochMilli(1L).plusMillis(8000L)));
            System.out.println("OUTPUT  ");
            var actualRecords = outputTopic.readRecordsToList();
            actualRecords.forEach(System.out::println);
            assertThat(actualRecords).hasSameElementsAs(expectedRecords);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
