package pl.jakubdrozdz.confluentkafka;

import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Configuration
public class KafkaConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<Integer, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        configProps.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ENABLE_IDEMPOTENCE_CONFIG, true);
        configProps.put(ACKS_CONFIG, "all");
        configProps.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        configProps.put(RETRIES_CONFIG, 5);
        configProps.put(BATCH_SIZE_CONFIG, 16384);
        configProps.put(BUFFER_MEMORY_CONFIG, 33554432);

        DefaultKafkaProducerFactory<Integer, String> factory = new DefaultKafkaProducerFactory<>(configProps);
        // Enable transactions
        factory.setTransactionIdPrefix("tx-");
        return factory;
    }

    @Bean
    public KafkaTemplate<Integer, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public KafkaTransactionManager<Integer, String> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(producerFactory());
    }
}
