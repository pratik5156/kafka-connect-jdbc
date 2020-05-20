/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package io.confluent.connect.jdbc.source.integration;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;

@Category(IntegrationTest.class)
public class MySourceConnectorIT extends BaseConnectorIT {

  private static final Logger log = LoggerFactory.getLogger(MySourceConnectorIT.class);

  private static final String CONNECTOR_NAME = "my-source-connector";
  private static final int NUM_RECORDS_PRODUCED = 20;
  private static final int TASKS_MAX = 1;
  private static final String KAFKA_TOPIC = "destination";

  @Before
  public void setup() throws IOException {
    startConnect();
  }

  @After
  public void close() {
    stopConnect();
  }

  //TODO: uncomment next line to run test
  @Test
  public void testSource() throws Exception {
    // TODO: Set up a proxy or use an endpoint

    // create topic in Kafka
    connect.kafka().createTopic(KAFKA_TOPIC);

    // setup up props for the source connector
    Map<String, String> props = new HashMap<>();
    props.put(CONNECTOR_CLASS_CONFIG, "MySourceConnector");
    props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
    // converters
    props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG, ByteArrayConverter.class.getName());
    //TODO: put connector-specific properties

    // start a source connector
    connect.configureConnector(CONNECTOR_NAME, props);

    // wait for tasks to spin up
    int expectedNumTasks = TASKS_MAX; // or set to actual number
    waitForConnectorToStart(CONNECTOR_NAME, expectedNumTasks);

    // Write records in proxy/external system, and the connector should read them and write to Kafka

    // consume all records from the source topic or fail, to ensure that they were correctly
    // produced.
    log.info("Waiting for records in destination topic ...");
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
        NUM_RECORDS_PRODUCED,
        CONSUME_MAX_DURATION_MS,
        KAFKA_TOPIC
    );

    //TODO: Verify that the consumed records match what we wrote, and no more
  }
}
