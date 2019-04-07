package com.example.kafkastreamsboot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

@Component
@RequiredArgsConstructor
@Slf4j
public class WordCountStreamProcessor implements InitializingBean {

    private Pattern nonWordPattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

    @Qualifier("wordCountStream")
    private final KStream<String, String> wordCountStream;

    void doWork() {

        KTable<String, Long> wordCounts = wordCountStream
                .flatMapValues(this::toWords)
                .groupBy((key, value) -> value)
                .count();

        wordCounts.toStream()
                .foreach(this::process);
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

    @Override
    public void afterPropertiesSet() throws Exception {
        doWork();
    }
}
