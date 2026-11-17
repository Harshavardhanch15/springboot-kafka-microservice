package com.example.orderservice.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class KafkaPartitionManager implements InitializingBean {

    private static final Logger LOGGER= LoggerFactory.getLogger(KafkaPartitionManager.class);
    private final AdminClient adminClient;

    @Value("${spring.kafka.topic.second-name}")
    private String topicName;

    private final AtomicInteger partitionCount=new AtomicInteger(0);
    private final AtomicInteger counter= new AtomicInteger(0);

    public KafkaPartitionManager(AdminClient adminClient){
        this.adminClient=adminClient;
    }

    // Execute once when Spring Boot starts
    @Override
    public void afterPropertiesSet()  {
        refreshPartitionCount();
    }

    // Fetch partition count from Kafka
    public void refreshPartitionCount(){
        try {
            TopicDescription description  = adminClient.describeTopics(
                    Collections.singleton(topicName)
            ).topicNameValues()
                    .get(topicName)
                    .get(10, TimeUnit.SECONDS);
           int count= description.partitions().size();
            if (count <= 0) {
                throw new IllegalStateException(
                        "Topic has no partitions: " + topicName
                );
            }
           int oldCount = partitionCount.getAndSet(count);
            LOGGER.info("Kafka topic: {}, Previous partitions: {}, Current partitions: {}",
                    topicName, oldCount, count);

        }
        catch (Exception e){
            throw new IllegalStateException(
                    "Unable to fetch partitions for topic: " + topicName,
                    e
            );
        }
    }

    //Refresh partition count every 60 seconds
    @Scheduled(fixedDelayString = "${kafka.topic.partition-refresh-ms:60000}")
    public void scheduledRefresh(){
        try {
            refreshPartitionCount();
        }
        catch (Exception e){
            LOGGER.error(
                    "Partition refresh failed. Keeping previous partition count.",
                    e
            );
        }
    }
    // Return next partition using Round Robin
    public int getNextPartition(){
        int count=partitionCount.get();
        if (count<=0){
            throw new IllegalStateException(
                    "Kafka partition count is not initialized"
            );
        }
        return Math.floorMod(
                counter.getAndIncrement(),
                count
        );
    }

    public int getPartitionCount(){
        return partitionCount.get();
    }




}
