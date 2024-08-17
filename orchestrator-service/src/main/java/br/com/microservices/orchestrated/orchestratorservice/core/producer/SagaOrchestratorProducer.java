package br.com.microservices.orchestrated.orchestratorservice.core.producer;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@AllArgsConstructor
@Component
public class SagaOrchestratorProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendEvent(String payload, String topic){
        try{
            kafkaTemplate.send(topic, payload);
            log.info("Sending event to topic {} with data {}", topic, payload);
        }
        catch (Exception ex){
            log.error("Error trying to send data to topic {} with data {}", topic, payload, ex);
        }
    }
}
