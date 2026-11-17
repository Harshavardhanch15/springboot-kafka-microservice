package com.example.orderservice.controller;

import com.example.basedomains.dto.Order;
import com.example.basedomains.dto.OrderEvent;
import com.example.orderservice.config.KafkaPartitionManager;
import com.example.orderservice.kafka.OrderProducer;
import com.example.orderservice.kafka.OrderProducerUsingRoundRobin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class OrderController {

    private final OrderProducer orderProducer;
    private final OrderProducerUsingRoundRobin producerUsingRoundRobin;

    public OrderController(OrderProducer orderProducer, OrderProducerUsingRoundRobin producerUsingRoundRobin) {
        this.orderProducer = orderProducer;
        this.producerUsingRoundRobin = producerUsingRoundRobin;
    }

    @PostMapping("/orders")
    public String placeOrder(@RequestBody Order order){
        order.setOrderId(UUID.randomUUID().toString());
        OrderEvent event=new OrderEvent();
        event.setStatus("PENDING");
        event.setMessage("order status is in pending state");
        event.setOrder(order);
        orderProducer.sendMessage(event);
        return "Order Placed successfully";

    }

    @PostMapping("/round-robin/orders")
    public String placeSequentialOrder(@RequestBody Order order){
        order.setOrderId(UUID.randomUUID().toString());
        OrderEvent event=new OrderEvent();
        event.setStatus("COMPLETED");
        event.setMessage("order is completed");
        event.setOrder(order);
        producerUsingRoundRobin.sendMessage(event);
        return "Order is successfully delivered";
    }
}
