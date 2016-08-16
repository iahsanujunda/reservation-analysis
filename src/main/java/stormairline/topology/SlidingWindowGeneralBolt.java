package stormairline.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

public class SlidingWindowGeneralBolt extends BaseWindowedBolt {
  private static final Logger LOG = Logger
      .getLogger(SlidingWindowGeneralBolt.class);

  private OutputCollector collector;
  private Map<String, Integer> counts;
  private String window;

  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    this.collector = collector;
    counts = new HashMap<String, Integer>();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("destinationschedule", "count"));
  }

  @Override
  public void execute(TupleWindow inputWindow) {

    Integer count;
    List<Tuple> newTuple = inputWindow.getNew();
    List<Tuple> oldTuple = inputWindow.getExpired();

    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Date date = new Date();

    window = dateFormat.format(date);

    for (Tuple tuple : newTuple) {
      String destsched = tuple.getStringByField("destinationschedule");

      count = counts.get(destsched);
      if (count == null)
        count = 0;
      count++;

      counts.put(destsched, count);
    }

    for (Tuple tuple : oldTuple) {
      String destsched = tuple.getStringByField("destinationschedule");

      count = counts.get(destsched);
      if (count == null)
        count = 0;
      count--;

      counts.put(destsched, count);

    }

    for (String destsched : counts.keySet()) {
      count = counts.get(destsched);
      collector.emit(new Values(destsched, count));
    }

    // printWindow();
    // printDestschedCount();
    // LOG.info("Events in current window: " + inputWindow.get().size());
    // LOG.info("End of window\n\n\n\n\n\n");

  }

  private void printDestschedCount() {
    for (String destsched : counts.keySet()) {
      LOG.info(String.format("%s has count of %s", destsched,
          counts.get(destsched)));
    }
  }

  private void printWindow() {
    LOG.info("window: " + window);
  }

}
