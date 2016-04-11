package de.am;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import static org.junit.Assert.*;

/**
 * Created by andreas.maier on 27.11.15.
 */
public class Kafka2KafkaTopologyIT {

    private static final String TOPIC_IN = "input";
    private static final String TOPIC_OUT = "output";
    private static final int BROKER_ID = 0;

    private static String zkConnect;
    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static KafkaServer kafkaServer;
    private static Producer producer;
    private static ConsumerConnector consumer;


    @BeforeClass
    public static void setUp() throws Exception {

        // setup Zookeeper
        zkConnect = TestZKUtils.zookeeperConnect();
        zkServer = new EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        // setup Broker
        int port = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(BROKER_ID, port, true);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        // create topic
        String [] argumentsIn = new String[]{"--topic", TOPIC_IN, "--partitions", "1","--replication-factor", "1"};
        TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(argumentsIn));

        String [] argumentsOut = new String[]{"--topic", TOPIC_OUT, "--partitions", "1","--replication-factor", "1"};
        TopicCommand.createTopic(zkClient, new TopicCommand.TopicCommandOptions(argumentsOut));

        List<KafkaServer> servers = new ArrayList<KafkaServer>();
        servers.add(kafkaServer);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), TOPIC_IN, 0, 5000);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), TOPIC_OUT, 0, 5000);

        // setup producer
        Properties properties = TestUtils.getProducerConfig("localhost:" + port);
        ProducerConfig producerConfig = new ProducerConfig(properties);
        producer = new Producer(producerConfig);

        // setup simple consumer (waiting 10 seconds for a message to arrive)
        Properties consumerProperties = TestUtils.createConsumerProperties(zkServer.connectString(), "group0", "consumer0", 30000);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
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
        KeyedMessage<Integer, byte[]> data1 = new KeyedMessage(TOPIC_IN, "test-message-ignore".getBytes(StandardCharsets.UTF_8));
        List<KeyedMessage> messages1 = new ArrayList<>();
        messages1.add(data1);
        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages1));

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
        Thread.sleep(45000);

        // send message
        KeyedMessage<Integer, byte[]> data2 = new KeyedMessage(TOPIC_IN, "test-message".getBytes(StandardCharsets.UTF_8));
        List<KeyedMessage> messages2 = new ArrayList<>();
        messages2.add(data2);
        producer.send(scala.collection.JavaConversions.asScalaBuffer(messages2));

        producer.close();

        // start consumer
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC_OUT, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC_OUT).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        if(iterator.hasNext()) {
            String msg = new String(iterator.next().message(), StandardCharsets.UTF_8);
            System.out.println("Kafka consumer received message: " + msg);
            assertEquals("test-message", msg);
        } else {
            fail();
        }

        consumer.shutdown();
        executor.shutdown();
    }
}