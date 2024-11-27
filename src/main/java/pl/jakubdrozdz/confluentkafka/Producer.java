package pl.jakubdrozdz.confluentkafka;

import com.github.javafaker.Faker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.stream.Stream;

@RequiredArgsConstructor
@Component
@Slf4j
public class Producer {
    private final KafkaTemplate<Integer, String> template;

    @EventListener(ApplicationStartedEvent.class)
    public void generate() {
        Faker faker = Faker.instance();
        final Flux<Long> interval = Flux.interval(Duration.ofMillis(1_000));
        final Flux<String> quotes = Flux.fromStream(Stream.generate(() -> faker.hobbit().quote()));

        // Use executeInTransaction for reactive streams
        Flux.zip(interval, quotes)
                .map(it -> template.executeInTransaction(operations ->
                        operations.send("hobbit", faker.random().nextInt(42), it.getT2())))
                .blockLast();
    }
}
