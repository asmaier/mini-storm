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
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
 * Created by andreas.maier on 27.11.15.
 */
public class Kafka2KafkaTopologyIT {

    private static final String ZKHOST = "127.0.0.1";
    private static final String KAFKAHOST = "127.0.0.1";
    private static final String KAFKAPORT = "9092";
    private static final String TOPIC_IN = "input";
    private static final String TOPIC_OUT = "output";
    private static final String BROKER_ID = "0";

    private static String zkConnect;
    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static KafkaServer kafkaServer;
    private static KafkaProducer<Integer, byte[]> producer;
    private static KafkaConsumer<String, String> consumer;


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

        List<KafkaServer> servers = new ArrayList<KafkaServer>();
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
        zkServer.shutdown();

    }


    @Test
    public void testMain() throws Exception {

        final String brokerConnection = kafkaServer.config().advertisedHostName() + ":" + kafkaServer.config().advertisedPort();

        // send message (it should be ignored, because it has been sent before the topology started)
        ProducerRecord<Integer, byte[]> data = new ProducerRecord<>(TOPIC_IN, "test-message-ignore".getBytes(StandardCharsets.UTF_8));
        producer.send(data);
        Thread.sleep(10000);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // run topology
                    Kafka2KafkaTopology.main(new String[]{zkConnect, brokerConnection});
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // wait for topology to be loaded
        Thread.sleep(35000);

        // send message
        ProducerRecord<Integer, byte[]> data2 = new ProducerRecord<>(TOPIC_IN, "test-message".getBytes(StandardCharsets.UTF_8));
        producer.send(data2);
        producer.close();

        // start consumer
        ConsumerRecords<String, String> records = consumer.poll(10000);
        assertEquals(1, records.count());

        Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
        ConsumerRecord<String, String> record = recordIterator.next();
        String msg = record.value();
        System.out.println("Kafka consumer received message: " + msg);
        assertEquals("test-message", msg);

        // wait until the topology and the storm cluster have been stopped
        Thread.sleep(20000);
        executor.shutdown();
    }
}
