/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.jdbc.source.integration;

import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;

@Category(IntegrationTest.class)
public class MySinkConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(MySinkConnectorIT.class);

  private static final String CONNECTOR_NAME = "my-sink-connector";
  private static final long NUM_RECORDS_PRODUCED = 20;
  private static final int TASKS_MAX = 3;
  private static final List<String> KAFKA_TOPICS = Arrays.asList("kafka1", "kafka2");

  @Before
  public void setup() throws IOException {
    //TODO: Start proxy or external system

    startConnect();
  }

  @After
  public void close() {
    stopConnect();

    //TODO: Stop mock or external system
  }

  //TODO: uncomment next line to run test
  @Test
  public void testSink() throws Throwable {
    //TODO: find proxy endpoint

    // create topics in Kafka
    KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));

    // setup up props for the sink connector
    Map<String, String> props = new HashMap<>();
    props.put(SinkConnectorConfig.TOPICS_CONFIG, String.join(",", KAFKA_TOPICS));
    props.put(CONNECTOR_CLASS_CONFIG, "MySinkConnector");
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
    // converters
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());
    // license properties
    //TODO: put connector-specific properties

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int minimumNumTasks = Math.min(KAFKA_TOPICS.size(), TASKS_MAX);
    waitForConnectorToStart(CONNECTOR_NAME, minimumNumTasks);

    // TODO: setup proxy/external system to listen to any messages published

    // Send records to Kafka
    for (int i = 0; i < NUM_RECORDS_PRODUCED; i++) {
      String kafkaTopic = KAFKA_TOPICS.get(i % KAFKA_TOPICS.size());
      String kafkaKey = "simple-key-" + i;
      String kafkaValue = "simple-message-" + i;
      log.debug("Sending message {} with topic {} to Kafka broker {}", kafkaTopic, kafkaValue);
      connect.kafka().produce(kafkaTopic, kafkaKey, kafkaValue);
    }

    // wait for tasks to spin up and write records to proxy/external system
    waitForCondition(
        () -> {
          int numFound = 0;
          // TODO: count the number of records in the external system
          return numFound >= NUM_RECORDS_PRODUCED;
        },
        CONSUME_MAX_DURATION_MS,
        "Message consumption duration exceeded without all expected messages seen yet in server");

    // Verify the record were written to the proxy/external system
  }
}
