package com.kumaran.microservices.twitter.to.kafka;

import com.kumaran.microservices.kafka.admin.client.KafkaAdminClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TwitterStreamInitializer {

    private final KafkaAdminClient kafkaAdminClient;

    public TwitterStreamInitializer(KafkaAdminClient kafkaAdminClient) {
        this.kafkaAdminClient = kafkaAdminClient;
    }

    public void init() {
        kafkaAdminClient.createTopic();
        kafkaAdminClient.checkSchemaRegistry();
        log.info("Initialized topics");
    }
}
