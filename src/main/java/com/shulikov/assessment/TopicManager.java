package com.shulikov.assessment;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class TopicManager {

    private static final Logger log = LogManager.getLogger(TopicManager.class);

    private static final String TOPIC_PROPERTIES = "topic.properties";

    private final String topicName;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public TopicManager(String topicName) {
        this.topicName = topicName;
    }

    public void create() throws IOException {

        Properties props = new Properties();
        props.load(new FileInputStream(TOPIC_PROPERTIES));

        KafkaZkClient zkClient = KafkaZkClient.apply(
                props.getProperty("zkhost"),
                parseBoolean(props.getProperty("secure")),
                parseInt(props.getProperty("session.timeout")),
                parseInt(props.getProperty("connection.timeout")),
                parseInt(props.getProperty("maxInFlightRequests")),
                Time.SYSTEM,
                props.getProperty("metric.group"),
                props.getProperty("metric.type"));
        AdminZkClient adminZkClient = new AdminZkClient(zkClient);

        if (!zkClient.topicExists(topicName)) {
            adminZkClient.createTopic(
                    topicName,
                    parseInt(props.getProperty("partitions")),
                    parseInt(props.getProperty("replication")),
                    new Properties(), RackAwareMode.Disabled$.MODULE$);
            log.info("Topic with name " + topicName + " was created");
        }
    }

    public void sentMessage(Producer<String, String> producer, SensorDataEvent event) {
        try {
            producer.send(new ProducerRecord<>(
                    topicName,
                    event.getTimestamp(),
                    objectMapper.writeValueAsString(event)));
        } catch (IOException e) {
            log.error("The event with timestamp " + event.getTimestamp() + " wasn't sent");
        }
    }
}
