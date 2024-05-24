package org.example.mcolomerc.kstreams.test;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class ProcessorJoiner implements Processor<String, String, String, String> {

    private KeyValueStore<String, String> store;
    private ProcessorContext<String, String> localContext;
    static Logger logger = LoggerFactory.getLogger(ProcessorJoiner.class.getName());

    @Override
    public void process(Record<String, String> record) {
        logger.info ("Storing record...[{}] => [{}] - [{}]", record.key(),record.value(), record.timestamp());
        store.put(record.key(), record.value());
    }

    @Override
    public void init(final ProcessorContext<String, String> context) {
        this.localContext = context;
        this.store = localContext.getStateStore("store");
        this.localContext.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
            @Override
            public void punctuate(long timestamp) {
                store.all().forEachRemaining(record -> {
                    logger.info ("FORWARD...[{}] => [{}]", record.key, record.value);
                    context.forward(new Record<>(record.key, record.value, timestamp));
                    store.delete(record.key);
                });
                context.commit();
            }
        });
    }

    @Override
    public void close() {
        // No cleanup required
    }
}