package com.kumaran.microservices.twitter.to.kafka.listener;

import com.kumaran.microservices.kafka.producer.service.TwitterKafkaProducer;
import com.kumaran.microservices.twitter.to.kafka.config.KafkaConfigData;
import com.kumaran.microservices.twitter.to.kafka.transformer.TwitterStatusToAvroTransformer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Slf4j
@Component
public class TwitterKafkaStatusListener extends StatusAdapter {

    private final KafkaConfigData kafkaConfigData;

    private final TwitterKafkaProducer<Long, TwitterAvroModel> twitterKafkaProducer;

    private final TwitterStatusToAvroTransformer twitterStatusToAvroTransformer;

    public TwitterKafkaStatusListener(KafkaConfigData kafkaConfigData, TwitterKafkaProducer<Long, TwitterAvroModel> twitterKafkaProducer, TwitterStatusToAvroTransformer twitterStatusToAvroTransformer) {
        this.kafkaConfigData = kafkaConfigData;
        this.twitterKafkaProducer = twitterKafkaProducer;
        this.twitterStatusToAvroTransformer = twitterStatusToAvroTransformer;
    }

    @Override
    public void onStatus(Status status) {
        log.info("Twitter status {}", status.getText());
        TwitterAvroModel twitterAvroModel = twitterStatusToAvroTransformer.toAvroTransformer(status);
        twitterKafkaProducer.send(kafkaConfigData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
    }
}
