package com.example.emailservice.kafka;


import com.example.basedomains.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private static final Logger LOGGER= LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = {"${spring.kafka.consumer.topic.name}","${spring.kafka.consumer.topic.second-name}"},groupId = "${spring.kafka.consumer.group-id}")
    public void consumerOrder(OrderEvent event, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic){
        LOGGER.info("Received from topic: {}",topic);
        LOGGER.info("Order event received in the email service: {}",event.toString());
    }
}
