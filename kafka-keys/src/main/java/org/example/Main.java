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

    properties.setProperty("batch.size", "400");
//    properties.setProperty("partitioner.class", StickyPartitionCache.class.getName());
//    properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
    // 설정을 이용해 producer 생성
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    // producer Record 생성

    for(int i=0; i<30; i++) {
      // producer Record 전송 및 콜백
      ProducerRecord<String, String> record = new ProducerRecord<>("partitions_topic", "hello world" + i);

      producer.send(record, new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          if(exception == null) {
            log.info("Success");
            log.info(metadata.toString());
            log.info("Topic: " + metadata.topic());
            log.info("Partition: " + metadata.partition());
            log.info("Offset: " + metadata.offset());
            log.info("Timestamp: " + metadata.timestamp());

          } else {
            log.error("Fail");
            log.error(exception.getMessage());
          }
        }});
      Thread.sleep(500);
    }


    // flush and close producer
    producer.flush();
    producer.close();


  }

}