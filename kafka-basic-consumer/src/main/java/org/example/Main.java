package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class Main {
  public static void main(String[] args) throws InterruptedException {
    log.info("LOG TEST");

    // kafka 설정
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");


    // consumer config
    properties.setProperty("key.deserializer", StringDeserializer.class.getName());
    properties.setProperty("value.deserializer", StringDeserializer.class.getName());
    properties.setProperty("group.id", "test-group");

    //none - offset이 없을 경우 error
    //earliest - offset이 없을 경우 가장 처음부터
    //latest - offset이 없을 경우 가장 마지막부터
    properties.setProperty("auto.offset.reset", "earliest"); // earliest, latest, none

    // consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    // topic 구독
    consumer.subscribe(Arrays.asList("partitions_topic"));

    while(true) {
      log.info("polling...");
      // poll
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

      // record 출력
      for(ConsumerRecord<String, String> record : records) {
        log.info("Topic: " + record.topic());
        log.info("key: " + record.key() + " | Partition: " + record.partition());
        log.info("Value: " + record.value());
        log.info("Timestamp: " + record.timestamp());
      }

      Thread.sleep(1000);

    }

  }
}