package de.am;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hbase.bolt.HBaseLookupBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by andreas.maier on 04/07/16.
 */
public class HBaseKafkaTopology {

    // if you change this ID the Kafka Spout will not know, where it stopped reading from the topic and fall back to
    // the setting of spoutConfig.startOffsetTime, which by default will reread all messages from the beginning of the topic.
    private static final String KAFKA_CONSUMER_ID = "HBaseKafkaTopology";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        String zkConnString = args[0];
        String kafkaBroker = args[1];
        String hbaseRoot = args[2];
        String hbaseZkConnString = args[3];
        String hbaseZkHost = hbaseZkConnString.split(":")[0];
        String hbaseZkPort = hbaseZkConnString.split(":")[1];
        String topicIn = args[4];
        String topicOut = args[5];

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

        HBaseMapper hBaseMapper = new HBaseMapper() {
            private final Logger logger = LoggerFactory.getLogger(HBaseMapper.class);

            @Override
            public byte[] rowKey(Tuple tuple) {
                String key = tuple.getString(0);
                logger.info("HBaseMapper received key from kafka: " + key);
                return key.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public ColumnList columns(Tuple tuple) {
                return null;
            }
        };

        HBaseValueMapper hBaseValueMapper = new HBaseValueMapper() {

            private final Logger logger = LoggerFactory.getLogger(HBaseValueMapper.class);

            @Override
            public List<Values> toValues(ITuple iTuple, Result result) {

                logger.info("Result from HBase:" + result.toString());

                List<Values> values = new ArrayList<>();
                String value =  result.isEmpty() ? "MISMATCH" : "MATCH";
                values.add(new Values(iTuple.getValue(0), value));

                return values;
            }


            @Override
            public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
                outputFieldsDeclarer.declare(new Fields(FieldNameBasedTupleToKafkaMapper.BOLT_KEY, FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE));
            }
        };

        Config conf = new Config();
        // see https://stackoverflow.com/questions/29664742/storm-hbase-configuration-not-found
        Properties propsHBase = new Properties();
        // Because of this crazy change http://hortonworks.com/community/forums/topic/change-for-zookeeper-znode-parent/
        // we need to set the znode.parent here.
        // If this value is not set correctly the HBaseBolt will silently fail and you will debug for days
        // until you find the problem!
        propsHBase.put("zookeeper.znode.parent", "/hbase-unsecure");
        propsHBase.put("hbase.rootdir", hbaseRoot);
        propsHBase.put("hbase.zookeeper.quorum", hbaseZkHost );
        propsHBase.put("hbase.zookeeper.property.clientPort", hbaseZkPort);
        conf.put("hbase.properties", propsHBase);

        HBaseLookupBolt hBaseLookupBolt = new HBaseLookupBolt("lookup", hBaseMapper, hBaseValueMapper)
                .withConfigKey("hbase.properties");

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout);
        builder.setBolt("hBaseBolt", hBaseLookupBolt).shuffleGrouping("kafkaSpout");
        builder.setBolt("kafkaBolt", bolt).shuffleGrouping("hBaseBolt");

        if (args.length > 6) {
            StormSubmitter.submitTopologyWithProgressBar(args[6], conf, builder.createTopology());
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
