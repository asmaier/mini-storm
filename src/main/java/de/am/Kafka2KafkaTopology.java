package de.am;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
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

/**
 * Created by andreas.maier on 27.11.15.
 */
public class Kafka2KafkaTopology {

    // if you change this ID the Kafka Spout will not know, where it stopped reading from the topic and fall back to
    // the setting of spoutConfig.startOffsetTime, which by default will reread all messages from the beginning of the topic.
    private static final String KAFKA_CONSUMER_ID = "Kafka2KafkaTopology";

    private static final String TOPIC_IN = "input";
    private static final String TOPIC_OUT = "output";

    // The Kafka Bolt needs a key to each message. That is why we need this Bolt.
    public static class GenerateKafkaKeyBolt extends BaseRichBolt {
        OutputCollector collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {

            System.out.println("Topology received kafka message: " + tuple.getString(0));

            collector.emit(tuple, new Values(UUID.randomUUID().toString(), tuple.getString(0)));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields(FieldNameBasedTupleToKafkaMapper.BOLT_KEY, FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE));
        }
    }

    public static void main(String[] args) throws Exception {

        String zkConnString = args[0];
        String kafkaBroker = args[1];

        BrokerHosts hosts = new ZkHosts(zkConnString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, TOPIC_IN, "/" + TOPIC_IN, KAFKA_CONSUMER_ID);
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

        KafkaBolt bolt = new KafkaBolt()
                .withTopicSelector(new DefaultTopicSelector(TOPIC_OUT))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper())
                .withProducerProperties(props);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout);
        builder.setBolt("kafkaKey", new GenerateKafkaKeyBolt()).shuffleGrouping("kafkaSpout");
        builder.setBolt("kafkaBolt", bolt).shuffleGrouping("kafkaKey");

        Config conf = new Config();

        if (args.length > 2) {
            StormSubmitter.submitTopologyWithProgressBar(args[1], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            conf.setDebug(true);
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(40000);
            cluster.killTopology("test");
            Utils.sleep(10000);
            cluster.shutdown();
        }
    }

}
