package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class Main {
  public static void main(String[] args) throws InterruptedException {
    log.info("LOG TEST");

    // kafka 및 producer 설정
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");

    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());
    properties.setProperty("acks", "1");

    // 설정을 이용해 producer 생성
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    // 항상 동일 key는 동일 partition으로 전송 확인하기
    for(int j=0; j<2; j++) {
      for(int i=0; i<10; i++) {
        final String topicName = "partitions_topic";
        final String key = "id_" + i;
        final String value = "hello world" + i;
        // producer Record 전송 및 콜백
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

        // producer 전송 및 callback
        producer.send(record, new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
              log.info("Topic: " + metadata.topic());
              log.info("key: " + key + " | Partition: " + metadata.partition());
              log.info("Timestamp: " + metadata.timestamp());
            } else {
              log.error("Fail");
              log.error(exception.getMessage());
            }
          }
        });
      }
    }


    // flush and close producer
    producer.flush();
    producer.close();


  }

}