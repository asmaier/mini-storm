package de.am;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import kafka.admin.TopicCommand;
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

    }


    @AfterClass
    public static void tearDown() throws Exception {

        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();

    }


    @Test
    public void testMain() throws Exception {

        String brokerConnection = kafkaServer.config().advertisedHostName() + ":" + kafkaServer.config().advertisedPort();

        Kafka2KafkaTopology.main(new String[]{zkConnect, brokerConnection});

    }
}