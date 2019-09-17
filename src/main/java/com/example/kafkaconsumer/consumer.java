package com.example.kafkaconsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus.Series;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

@Component
public class consumer {

  private static final Logger log = LoggerFactory.getLogger(consumer.class.getName());

  private final RestTemplate restTemplate = new RestTemplate();

  private final String name = "sample-consumer";

  @PostConstruct
  void post() {
    final WebClient webClient = WebClient.create();
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId-31");
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, name);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    Scheduler scheduler = Schedulers.newElastic("consumer", 60, true);

    final String topic = "test";

    KafkaReceiver.create(ReceiverOptions.create(props).subscription(Collections.singletonList(topic))
        .commitInterval(Duration.ZERO)
        .addAssignListener(p -> {
          log.info("Group partitions assigned {}", p);
        })
        .addRevokeListener(p -> log.info("Group  partitions revoked {}", p))
    )
        .receive()
        .groupBy(m -> m.receiverOffset().topicPartition())
        .flatMap(partitionFlux -> partitionFlux.publishOn(scheduler)
            .flatMap(r ->
                {
                  return webClient.get()
                      .uri(UriComponentsBuilder.fromUriString("http://localhost:8080" + "/test").path("/{message}").buildAndExpand(r.value().toString()).toUri())
                      .retrieve()
                      .onStatus(httpStatus -> httpStatus.series() != Series.SUCCESSFUL, clientResponse -> {
                        return Mono.error(new RuntimeException("error "));
                      })
                      .bodyToMono(String.class)
                      .map(
                          rs -> {
                            r.receiverOffset().acknowledge();
                            return r.receiverOffset();
                          }
                      );
                }
            )
        )
        .retryBackoff(5, Duration.ofSeconds(10), Duration.ofMinutes(5), 0.2, scheduler)
        .sample(Duration.ofMillis(5000))
        .concatMap(offset -> {
          log.info("commiting {}", offset.topicPartition());
          return offset.commit();
        })
        .onErrorContinue((throwable, object) ->
            log.info("Event Dropped. Proceeding to next: {} - {} ", throwable, object)
        ).blockLast();
  }
}
