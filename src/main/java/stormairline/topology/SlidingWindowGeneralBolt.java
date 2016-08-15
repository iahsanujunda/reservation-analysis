package stormairline.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

public class SlidingWindowGeneralBolt extends BaseWindowedBolt {
  private OutputCollector collector;
  private Map<String, Integer> counts;

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
      collector.emit(new Values(destsched, count));
      // collector.ack(tuple);
    }

    // System.out.println("============================\n");
    // printWindow();
    // printDestschedCount();
    // System.out.println("============================\n");
    // System.out.println("Events in current window: " +
    // inputWindow.get().size());
    // System.out.println("============================\n\n\n");

  }
  //
  // private void printDestschedCount() {
  // for (String destsched : counts.keySet()) {
  // System.out.println(String.format("%s has count of %s", destsched,
  // counts.get(destsched)));
  // }
  // }
  //
  // private void printWindow() {
  // DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  // Date date = new Date();
  // System.out.println("window : " + dateFormat.format(date));
  // }

}
