package com.kumaran.microservices.twitter.to.kafta;

import com.kumaran.microservices.twitter.to.kafka.config.TwitterToKafkaConfigData;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest (classes = TwitterToKafkaConfigData.class)
public class TwitterToKafkaApplicationTest {

    @Test
    public void contextLoad() {

    }
}
