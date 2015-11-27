package de.am;

import java.util.Properties;
import java.util.UUID;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

/**
 * Created by andreas.maier on 27.11.15.
 */
public class Kafka2KafkaTopology {

    private static final String TOPIC_IN = "input";
    private static final String TOPIC_OUT = "output";

    public static void main(String[] args) throws Exception {

        String zkConnString = args[0];
        String kafkaBroker = args[1];

        BrokerHosts hosts = new ZkHosts(zkConnString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, TOPIC_IN, "/" + TOPIC_IN, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        KafkaBolt bolt = new KafkaBolt()
                .withTopicSelector(new DefaultTopicSelector(TOPIC_OUT))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

        Config conf = new Config();
        //set producer properties.
        Properties props = new Properties();
        props.put("metadata.broker.list", kafkaBroker);
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout);
        builder.setBolt("kafkaBolt", bolt).shuffleGrouping("kafkaSpout");

        if (args.length > 2) {
            StormSubmitter.submitTopologyWithProgressBar(args[1], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

}
