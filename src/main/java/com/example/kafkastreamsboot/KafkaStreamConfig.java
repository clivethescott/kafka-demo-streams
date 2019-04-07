package com.example.kafkastreamsboot;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

@Configuration
@Slf4j
public class KafkaStreamConfig {


    private static final String WORD_COUNT_TOPOLOGY = "WORD_COUNT_TOPOLOGY";
    private static final String WORD_COUNT_STREAMS = "WORD_COUNT_STREAMS";
    private Pattern nonWordPattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

    @Bean(name = WORD_COUNT_STREAMS, initMethod = "start", destroyMethod = "close")
    public KafkaStreams wordCountStreams(@Qualifier(WORD_COUNT_TOPOLOGY) Topology topology,
                                         KafkaProperties kafkaProperties) {

        final Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaProperties.getStreams().getApplicationId());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir"));

        final KafkaStreams wordCountStreams = new KafkaStreams(topology, props);
        return wordCountStreams;
    }

    @Bean(name = WORD_COUNT_TOPOLOGY)
    public Topology wordCountTopology() {

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(Topics.WORD_COUNT);

        KTable<String, Long> wordCounts = textLines
                .flatMapValues(this::toWords)
                .groupBy((key, word) -> word)
                .count();

        wordCounts.toStream()
                .foreach(this::process);

        return builder.build();
    }

    private List<String> toWords(String sentence) {
        if (sentence == null) {
            return Collections.emptyList();
        }
        return Arrays.asList(nonWordPattern.split(sentence.toLowerCase()));
    }

    private void process(String word, Long count) {
        log.info("------> Got word ---<" + word + "> with a count of " + count);
    }
}
