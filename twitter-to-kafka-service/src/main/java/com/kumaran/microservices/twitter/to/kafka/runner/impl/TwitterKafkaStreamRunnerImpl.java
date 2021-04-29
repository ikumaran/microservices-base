package com.kumaran.microservices.twitter.to.kafka.runner.impl;

import com.kumaran.microservices.twitter.to.kafka.config.TwitterToKafkaConfigData;
import com.kumaran.microservices.twitter.to.kafka.listener.TwitterKafkaStatusListener;
import com.kumaran.microservices.twitter.to.kafka.runner.StreamRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
@Slf4j
public class TwitterKafkaStreamRunnerImpl implements StreamRunner {

    private TwitterStream twitterStream;

    private final TwitterToKafkaConfigData twitterToKafkaConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    public TwitterKafkaStreamRunnerImpl(TwitterToKafkaConfigData twitterToKafkaConfigData, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaConfigData = twitterToKafkaConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException {
        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterKafkaStatusListener);
        String[] keywords = twitterToKafkaConfigData.getKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        log.info("Started filtering streams with keyword {}", Arrays.toString(keywords));
    }

    @PreDestroy
    public void destroy() {
        if (twitterStream != null) {
            log.info("Closing streams");
            twitterStream.shutdown();
        }
    }

}
