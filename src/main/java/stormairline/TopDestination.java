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
import stormairline.topology.SlidingWindowAllBolt;
import stormairline.topology.TransformDestSchedBolt;
import stormairline.topology.TupleGeneratorSpout;
import stormairline.topology.RankerBolt;

public class TopDestination
{
  private static int TUPLE_TIMEOUT = 60;
  private static int EMIT_RATE = 10;
  private static int WINDOW_LENGTH = 60000;
  private static int SLIDING_INTERVAL = 10000;

  public static void main(String[] args) throws Exception {

    // Declare topology and streaming
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("generate-tuples", new TupleGeneratorSpout(), 1)
        .setNumTasks(3);

    builder.setBolt("transform-destinations", new TransformDestSchedBolt(), 1)
        .setNumTasks(1).shuffleGrouping("generate-tuples");
    builder.setBolt(
        "window-all",
        new SlidingWindowAllBolt().withWindow(
            new Duration(40,
                TimeUnit.SECONDS), new Duration(EMIT_RATE, TimeUnit.SECONDS)),
            1)
        .setNumTasks(1)
        .fieldsGrouping("transform-destinations",
            new Fields("destinationschedule"));
    builder.setBolt(
        "window-frequent",
        new SlidingWindowFrequentBolt().withWindow(new Duration(40,
                TimeUnit.SECONDS), new Duration(EMIT_RATE, TimeUnit.SECONDS)),
            1)
        .setNumTasks(1)
        .fieldsGrouping("transform-destinations",
            new Fields("destinationschedule"));


    // Data Testing Purpose; use tuple count as window limit, instead of
    // time duration
    // builder.setBolt(
    // "window-all",
    // new SlidingWindowAllBolt().withWindow(new Count(WINDOW_LENGTH),
    // new Count(SLIDING_INTERVAL)), 1).fieldsGrouping(
    // "transform-destinations", new Fields("destinationschedule"));
    // builder.setBolt(
    // "window-frequent",
    // new SlidingWindowFrequentBolt().withWindow(new Count(WINDOW_LENGTH),
    // new Count(SLIDING_INTERVAL)), 1).fieldsGrouping(
    // "transform-destinations", new Fields("destinationschedule"));

    // Combine and Rank
    builder.setBolt("ranker-all",
        new RankerBolt().withTumblingWindow(new Duration(9, TimeUnit.SECONDS)),
 1).setNumTasks(1)
        .globalGrouping("window-all");
    builder
        .setBolt(
            "ranker-frequent",
            new RankerBolt().withTumblingWindow(new Duration(9,
                TimeUnit.SECONDS)), 1).setNumTasks(1)
        .globalGrouping("window-frequent");


    // Declare run configuration
    Map conf = new HashMap();
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, TUPLE_TIMEOUT);
    conf.put(Config.TOPOLOGY_DEBUG, false);

    if (args != null && args.length > 0) {

      // submit to remote cluster
      conf.put(Config.TOPOLOGY_WORKERS, 1);

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
