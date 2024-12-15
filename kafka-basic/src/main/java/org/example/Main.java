package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class Main {
  public static void main(String[] args) {
    log.info("LOG TEST");

    // kafka 및 producer 설정
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");

    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());

    // 설정을 이용해 producer 생성
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    // producer Record 생성
    ProducerRecord<String, String> record = new ProducerRecord<>("java_topic", "hello world");

    // producer Record 전송
    producer.send(record);

    // flush and close producer
    producer.flush();
    producer.close();

  }
}