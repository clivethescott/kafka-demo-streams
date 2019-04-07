package com.example.kafkastreamsboot;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Properties;

import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME;

@Configuration
@Slf4j
public class KafkaStreamConfig {

    @Bean(name = DEFAULT_STREAMS_BUILDER_BEAN_NAME)
    public StreamsBuilderFactoryBean streamsBuilderFactoryBean(KafkaProperties kafkaProperties) {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaProperties.getStreams().getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir"));

        final StreamsBuilderFactoryBean factory = new StreamsBuilderFactoryBean();
        factory.setAutoStartup(true);
        factory.setStreamsConfiguration(props);

        return factory;
    }


    @Bean(name = "wordCountStream")
    public KStream<String, String> wordCountStream(
            @Qualifier(DEFAULT_STREAMS_BUILDER_BEAN_NAME) StreamsBuilder wordCountStreamBuilder) {

        final Serde<String> stringSerde = Serdes.String();
        return wordCountStreamBuilder.stream(Topics.WORD_COUNT, Consumed.with(stringSerde, stringSerde));
    }
}
