package de.am;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import static org.junit.Assert.*;

/**
 * Created by andreas.maier on 04/07/16.
 */
public class CassandraKafkaTopologyIT {

    private static final String TOPIC_IN = "input";
    private static final String TOPIC_OUT = "output";
    private static final String BROKER_ID = "0";
    private static final String ZKHOST = "127.0.0.1";
    private static final String KAFKAHOST = "127.0.0.1";
    private static final String KAFKAPORT = "9092";

    private static final String CASSANDRA_HOSTS = "127.0.0.1";
    private static final String CASSANDRA_PORT = "29043";
    private static final String CASSANDRA_KEYSPACE = "test";

    private static String zkConnect;
    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static KafkaServer kafkaServer;
    private static KafkaProducer<Integer, byte[]> producer;
    private static KafkaConsumer<String, String> consumer;

    private static final String[] inputs = new String[]{
            "test data 1",
            "test data 2",
            "test data 3",
            "test data 4",
            "test data 5",
               };

    private static final String[] outputs = new String[]{
            "MATCH",
            "MATCH",
            "MISMATCH",
            "MATCH",
            "MATCH"
    };

    @ClassRule
    public static final CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("test.cql", CASSANDRA_KEYSPACE));

    @BeforeClass
    public static void setUp() throws Exception {
        // setup Zookeeper
        zkServer = new EmbeddedZookeeper();
        zkConnect = ZKHOST + ":" +zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", BROKER_ID);
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("advertised.host.name", KAFKAHOST);
        brokerProps.setProperty("listeners", "PLAINTEXT://" + KAFKAHOST +":" + KAFKAPORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        // create topic
        AdminUtils.createTopic(zkUtils, TOPIC_IN, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        AdminUtils.createTopic(zkUtils, TOPIC_OUT, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

        List<KafkaServer> servers = new ArrayList<>();
        servers.add(kafkaServer);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), TOPIC_IN, 0, 5000);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), TOPIC_OUT, 0, 5000);

        // setup producer
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", KAFKAHOST + ":" + KAFKAPORT);
        producerProps.setProperty("key.serializer","org.apache.kafka.common.serialization.IntegerSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(producerProps);

        // setup consumer
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", KAFKAHOST + ":" + KAFKAPORT);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");  // to make sure the consumer starts from the beginning of the topic
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TOPIC_OUT));
    }


    @AfterClass
    public static void tearDown() throws Exception {

        kafkaServer.shutdown();
        kafkaServer.awaitShutdown();
        zkClient.close();
        //zkServer.shutdown();
    }


    @Test
    public void testMain() throws Exception {

        final String brokerConnection = kafkaServer.config().advertisedHostName() + ":" + kafkaServer.config().advertisedPort();

        // send messages (these should be ignored, because they have been sent before the topology started)
        List<ProducerRecord> messages = new ArrayList<>();
        for(String input : inputs) {
            ProducerRecord<Integer, byte[]> data = new ProducerRecord<>(TOPIC_IN, input.getBytes(StandardCharsets.UTF_8));
            producer.send(data);
        }
        Thread.sleep(10000);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                // run topology
                CassandraKafkaTopology.main(new String[]{zkConnect, brokerConnection, CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE, TOPIC_IN, TOPIC_OUT});
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // wait for topology to be loaded
        Thread.sleep(40000);

        // send messages again (these messages should be processed)
        for(String input : inputs) {
            ProducerRecord<Integer, byte[]> data = new ProducerRecord<>(TOPIC_IN, input.getBytes(StandardCharsets.UTF_8));
            producer.send(data);
        }
        producer.close();

        // start consumer
        ConsumerRecords<String, String> records = consumer.poll(10000);
        assertEquals(5, records.count());

        Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();

        for(String output : outputs) {
            if(recordIterator.hasNext()) {
                String msg = recordIterator.next().value();
                System.out.println("Kafka consumer received message: " + msg);
                assertEquals(output, msg);
            } else {
                fail();
            }
        }

        // wait until the topology and the storm cluster have been stopped
        Thread.sleep(20000);
        executor.shutdown();
    }
}