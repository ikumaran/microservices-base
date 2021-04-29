package com.kumaran.microservices.kafka.producer.service.impl;

import com.kumaran.microservices.kafka.producer.service.TwitterKafkaProducer;
import com.microservices.demo.kafka.avro.model.TwitterAvroModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PreDestroy;

@Service
@Slf4j
public class TwitterKafkaProducerImpl implements TwitterKafkaProducer<Long, TwitterAvroModel> {

    private final KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;

    public TwitterKafkaProducerImpl(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PreDestroy
    public void destroy() {
        if (kafkaTemplate != null) {
            log.info("Destroying the resource");
            kafkaTemplate.destroy();
        }
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        log.info("sending message {} to {}, with key {}", message, topicName, key);
        ListenableFuture<SendResult<Long, TwitterAvroModel>> kafkaFutureResult = kafkaTemplate.send(topicName, key, message);
        kafkaFutureResult.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.error("Message not added to topic {} with message {}", topicName, message.toString(), throwable);
            }

            @Override
            public void onSuccess(SendResult<Long, TwitterAvroModel> longTwitterAvroModelSendResult) {
                RecordMetadata metadata = longTwitterAvroModelSendResult.getRecordMetadata();
                log.debug("Message added successfully, with Topic {}; Partition {}; Offset {}; Timestamp {}; at time {} ",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp(),
                        System.nanoTime());
            }
        });
    }
}
