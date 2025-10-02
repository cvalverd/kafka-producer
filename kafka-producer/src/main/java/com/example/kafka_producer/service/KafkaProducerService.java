package com.example.kafka_producer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "educacion";

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        try {
            var result = kafkaTemplate.send(TOPIC, message).get(); // <— BLOQUEA
            System.out.println("ENVIADO OK -> topic=" + result.getRecordMetadata().topic() +
                    ", partition=" + result.getRecordMetadata().partition() +
                    ", offset=" + result.getRecordMetadata().offset());
        } catch (Exception e) {
            System.err.println("ERROR AL ENVIAR: " + e.getMessage());
            e.printStackTrace();
        } finally {
            kafkaTemplate.flush();
        }
    }
}
