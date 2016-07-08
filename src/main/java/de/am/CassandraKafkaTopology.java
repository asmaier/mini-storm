package de.am;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;

/**
 * Created by andreas.maier on 04/07/16.
 */
public class CassandraKafkaTopology {

    // if you change this ID the Kafka Spout will not know, where it stopped reading from the topic and fall back to
    // the setting of spoutConfig.startOffsetTime, which by default will reread all messages from the beginning of the topic.
    private static final String KAFKA_CONSUMER_ID = "CassandraKafkaTopology";

    // The Kafka Bolt needs a key to each message. That is why we need this Bolt.
    public static class CassandraLookupBolt extends BaseRichBolt {

        private static final String CQL_LOOKUP = "SELECT * FROM lookup WHERE message = ?";

        OutputCollector collector;

        Cluster cluster;
        Session session;
        PreparedStatement preparedStatement;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;

            Map cassandraConfig = (Map) conf.get("cassandra.properties");

            String[] nodes = ((String) cassandraConfig.get("cassandra.nodes")).split(",");
            int port = Integer.parseInt((String) cassandraConfig.get("cassandra.port"));
            String keyspace = (String) cassandraConfig.get("cassandra.keyspace");

            this.cluster = Cluster.builder()
                    .addContactPoints(nodes)
                    .withPort(port)
                    .withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE))
                    .build();

            this.session = cluster.connect(keyspace);
            this.preparedStatement = this.session.prepare(CQL_LOOKUP);
        }

        @Override
        public void execute(Tuple tuple) {

            System.out.println("Topology received kafka message: " + tuple.getString(0));

            BoundStatement statement = preparedStatement.bind(tuple.getString(0));

            ResultSet resultSet = this.session.execute(statement);
            String value =  resultSet.all().size() == 0  ? "MISMATCH" : "MATCH";

            collector.emit(tuple, new Values(UUID.randomUUID().toString(), value));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FieldNameBasedTupleToKafkaMapper.BOLT_KEY, FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE));
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        String zkConnString = args[0];
        String kafkaBroker = args[1];

        String cassandraNodes = args[2];
        String cassandraPort = args[3];
        String cassandraKeyspace = args[4];

        String topicIn = args[5];
        String topicOut = args[6];

        BrokerHosts hosts = new ZkHosts(zkConnString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicIn, "/" + topicIn, KAFKA_CONSUMER_ID);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        // Unfortunately the default for startOffsetTime is OffsetRequest.EarliestTime(), so we need to change this here,
        // to make sure we don't read the whole topic the first time we are starting the topology.
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        //set producer properties.
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBroker);
        props.put("acks", "1");
        props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt bolt = new KafkaBolt<>()
                .withTopicSelector(new DefaultTopicSelector(topicOut))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>())
                .withProducerProperties(props);

        Config conf = new Config();
        Properties propsCassandra = new Properties();
        propsCassandra.put("cassandra.nodes", cassandraNodes);
        propsCassandra.put("cassandra.port", cassandraPort);
        propsCassandra.put("cassandra.keyspace", cassandraKeyspace);
        conf.put("cassandra.properties", propsCassandra);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout);
        builder.setBolt("cassandraBolt", new CassandraLookupBolt()).shuffleGrouping("kafkaSpout");
        builder.setBolt("kafkaBolt", bolt).shuffleGrouping("cassandraBolt");

        if (args.length > 7) {
            StormSubmitter.submitTopologyWithProgressBar(args[7], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            conf.setDebug(true);
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(50000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
