package com.kumaran.microservices.twitter.to.kafka;

import com.kumaran.microservices.twitter.to.kafka.runner.StreamRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import twitter4j.TwitterException;

@SpringBootApplication
@Slf4j
@ComponentScan(basePackages = "com.kumaran.microservices")
public class TwitterToKafkaApplication implements CommandLineRunner {

    private final StreamRunner streamRunner;

    private final TwitterStreamInitializer twitterStreamInitializer;

    public TwitterToKafkaApplication(StreamRunner streamRunner, TwitterStreamInitializer twitterStreamInitializer) {
        this.streamRunner = streamRunner;
        this.twitterStreamInitializer = twitterStreamInitializer;
    }

    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaApplication.class, args);
    }

    @Override
    public void run(String... args) throws TwitterException {
        log.info("Initialized app");
        twitterStreamInitializer.init();
        streamRunner.start();
    }
}
