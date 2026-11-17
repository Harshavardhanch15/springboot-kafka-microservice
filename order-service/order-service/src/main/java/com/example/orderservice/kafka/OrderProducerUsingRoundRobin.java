package com.example.orderservice.kafka;

import com.example.basedomains.dto.OrderEvent;
import com.example.orderservice.config.KafkaPartitionManager;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderProducerUsingRoundRobin {
    private static final Logger LOGGER= LoggerFactory.getLogger(OrderProducerUsingRoundRobin.class);
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final KafkaPartitionManager kafkaPartitionManager;
    private final String topicName;

    public OrderProducerUsingRoundRobin(KafkaTemplate<String, OrderEvent> kafkaTemplate, KafkaPartitionManager kafkaPartitionManager, @Value("${spring.kafka.topic.second-name}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaPartitionManager = kafkaPartitionManager;
        this.topicName = topicName;
    }
    public void sendMessage(OrderEvent event){
        int partition=kafkaPartitionManager.getNextPartition();
        LOGGER.info("Sending order event to partition: {}",partition);
        kafkaTemplate.send(
                topicName,
                partition,
                event.getOrder().getOrderId(),
                event
        ).whenComplete(
                (result,exception)->{
                    if (exception!=null){
                        LOGGER.error("Failed to send order event",exception);
                    }
                    else {
                        LOGGER.info(
                                "Message sent successfully. Partition : {}, Offset: {}",
                                result.getProducerRecord().partition(),
                                result.getRecordMetadata().offset()
                        );
                    }
                }
        );
    }
}
