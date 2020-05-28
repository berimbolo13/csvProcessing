package com.shulikov.assessment;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Framework {

    public static String TOPIC_NAME = "dqf-temperature";
    public static String PRODUCER_PROPERTIES = "producer.properties";
    public static String SENSOR_DATA = "sensor_data.csv";

    public static void main(String[] args) throws IOException {

        TopicManager topicManager = new TopicManager(TOPIC_NAME);
        topicManager.create();

        Properties producerProperties = new Properties();
        producerProperties.load(new FileInputStream(PRODUCER_PROPERTIES));

        try (Producer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            DatasetReader.read(SENSOR_DATA)
                    .filter("unit = 'C'")
                    .map((MapFunction<Row, SensorDataEvent>)
                            SensorDataEvent::of, Encoders.bean(SensorDataEvent.class))
                    .collectAsList()
                    .forEach(event -> topicManager.sentMessage(producer, event));
        }
    }
}
