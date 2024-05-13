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
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;


public class TestOuterAggregateApp {

    private TopologyTestDriver testDriver;
    final StreamsBuilder builder = new StreamsBuilder();
    private TestInputTopic<String, String> leftTopic;
    private TestInputTopic<String, String> rightTopic;
    private TestOutputTopic<String, String> outputTopic;
    private final Instant recordBaseTime = Instant.parse("2019-06-01T10:00:00Z");
    private final Duration advance1Sec = Duration.ofSeconds(1);

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
            StreamOuterAggregateApp app = new StreamOuterAggregateApp();
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
            leftTopic.pipeInput(new TestRecord("1", "left 1", null, Instant.ofEpochMilli(1L).plusMillis(800L)));
            rightTopic.pipeInput(new TestRecord("1", "right 1", null, Instant.ofEpochMilli(1L).plusMillis(900L)));
            //window
            rightTopic.pipeInput(new TestRecord("1", "right 2", null, Instant.ofEpochMilli(1L).plusMillis(2500L)));
            //window
            leftTopic.pipeInput(new TestRecord("1", "left 3", null, Instant.ofEpochMilli(1L).plusMillis(3600L)));
            // window
            rightTopic.pipeInput(new TestRecord("1", "right 4", null, Instant.ofEpochMilli(1L).plusMillis(5000L)));
            leftTopic.pipeInput(new TestRecord("1", "left 4", null, Instant.ofEpochMilli(1L).plusMillis(5100L)));
            leftTopic.pipeInput(new TestRecord("1", "left 4", null, Instant.ofEpochMilli(1L).plusMillis(5500L)));
            rightTopic.pipeInput(new TestRecord("1", "right 4", null, Instant.ofEpochMilli(1L).plusMillis(5800L)));
            // window
            rightTopic.pipeInput(new TestRecord("1", "right 5", null, Instant.ofEpochMilli(1L).plusMillis(7000L)));
            rightTopic.pipeInput(new TestRecord("1", "right 5", null, Instant.ofEpochMilli(1L).plusMillis(7600L)));
           //window
            leftTopic.pipeInput(new TestRecord("1", "left 6", null, Instant.ofEpochMilli(1L).plusMillis(9000L)));
            leftTopic.pipeInput(new TestRecord("1", "left 6", null, Instant.ofEpochMilli(1L).plusMillis(9800L)));
            //  Just to close the previous window.
            leftTopic.pipeInput(new TestRecord("1", "left 7", null, Instant.ofEpochMilli(1L).plusMillis(11200L)));

            assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue("1", "left :: left 1, right :: right 1")));
            assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue("1", "left :: null, right :: right 2")));
            assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue("1", "left :: left 3, right :: null")));
            assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue("1", "left :: left 4, right :: right 4")));
            assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue("1", "left :: null, right :: right 5")));
            assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue("1", "left :: left 6, right :: null")));

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
