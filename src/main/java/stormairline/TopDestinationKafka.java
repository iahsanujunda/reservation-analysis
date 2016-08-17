package stormairline;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import stormairline.topology.SlidingWindowFrequentBolt;
import stormairline.topology.SlidingWindowGeneralBolt;
import stormairline.topology.TransformDestSchedBolt;
import stormairline.topology.RankerBolt;

public class TopDestinationKafka
{
  private static int TUPLE_TIMEOUT = 60;
  private static int EMIT_RATE = 10;

  public static void main(String[] args) throws Exception {

    // Kafka Preparation
    String zkConnString = "localhost:2181";
    String topic = "reservation-topic";
    BrokerHosts hosts = new ZkHosts(zkConnString);

    SpoutConfig spoutConfig =
        new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
    spoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
    spoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    

    // Declare topology and streaming
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("kafka-spout", new KafkaSpout(spoutConfig));

    builder.setBolt("transform-destinations", new TransformDestSchedBolt(), 3)
        .shuffleGrouping("kafka-spout");
    builder.setBolt(
        "window-general",
        new SlidingWindowGeneralBolt().withWindow(new Duration(40,
            TimeUnit.SECONDS), new Duration(EMIT_RATE, TimeUnit.SECONDS)), 3)
        .fieldsGrouping("transform-destinations",
            new Fields("destinationschedule"));
    builder.setBolt(
        "window-frequent",
        new SlidingWindowFrequentBolt().withWindow(new Duration(40,
            TimeUnit.SECONDS), new Duration(EMIT_RATE, TimeUnit.SECONDS)), 3)
        .fieldsGrouping("transform-destinations",
            new Fields("destinationschedule"));
    builder.setBolt(
        "combiner-general",
        new RankerBolt().withTumblingWindow(new Duration(EMIT_RATE,
            TimeUnit.SECONDS)), 1)
.globalGrouping("window-general");
//    builder.setBolt(
//        "combiner-frequent",
//        new CombinerBolt().withTumblingWindow(new Duration(EMIT_RATE,
//            TimeUnit.SECONDS)), 1).globalGrouping("window-frequent");


    // Declare run configuration
    Map conf = new HashMap();
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, TUPLE_TIMEOUT);
    conf.put(Config.TOPOLOGY_DEBUG, true);

    if (args != null && args.length > 0) {

      // submit to remote cluster
      conf.put(Config.TOPOLOGY_WORKERS, 3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
          builder.createTopology());
    } else {

      // Submit to local cluster
      LocalCluster localCluster = new LocalCluster();
      localCluster.submitTopology("reservation-analysis", conf,
          builder.createTopology());

      Utils.sleep(600000);
      localCluster.killTopology("reservation-analysis");
      localCluster.shutdown();
    }
  }
}
