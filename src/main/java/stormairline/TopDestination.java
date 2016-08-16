package stormairline;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import stormairline.topology.SlidingWindowFrequentBolt;
import stormairline.topology.SlidingWindowGeneralBolt;
import stormairline.topology.TransformDestSchedBolt;
import stormairline.topology.TupleGeneratorSpout;
import stormairline.topology.CombinerBolt;

public class TopDestination
{
  private static int TUPLE_TIMEOUT = 60;
  private static int EMIT_RATE = 10;
  private static int WINDOW_LENGTH = 8000;
  private static int SLIDING_INTERVAL = 400;

  public static void main(String[] args) throws Exception {

    // Declare topology and streaming
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("generate-tuples", new TupleGeneratorSpout(), 2);

    builder.setBolt("transform-destinations", new TransformDestSchedBolt(), 3)
        .shuffleGrouping("generate-tuples");
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


    // Data Testing Purpose; use tuple count as window limit, instead of
    // time duration
    // builder.setBolt(
    // "window-general",
    // new SlidingWindowGeneralBolt().withWindow(new Count(WINDOW_LENGTH),
    // new Count(SLIDING_INTERVAL)), 3).fieldsGrouping(
    // "transform-destinations", new Fields("destinationschedule"));
    // builder.setBolt(
    // "window-frequent",
    // new SlidingWindowFrequentBolt().withWindow(new Count(WINDOW_LENGTH),
    // new Count(SLIDING_INTERVAL)), 3).fieldsGrouping(
    // "transform-destinations", new Fields("destinationschedule"));

    // Combiner
    builder.setBolt(
        "combiner-general",
        new CombinerBolt()
            .withTumblingWindow(new Duration(9, TimeUnit.SECONDS)), 1)
        .globalGrouping("window-general");
    builder.setBolt(
        "combiner-frequent",
        new CombinerBolt()
            .withTumblingWindow(new Duration(9, TimeUnit.SECONDS)), 1)
        .globalGrouping("window-frequent");


    // Declare run configuration
    Map conf = new HashMap();
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, TUPLE_TIMEOUT);
    conf.put(Config.TOPOLOGY_DEBUG, false);

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
