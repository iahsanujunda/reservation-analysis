package stormairline.topology;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

public class RankerBolt extends BaseWindowedBolt {
  private static final Logger LOG = Logger
.getLogger(RankerBolt.class);

  private OutputCollector collector;
  private Map<String, Integer> map;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("destsched", "count", "window"));
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(TupleWindow inputWindow) {
    map = new HashMap<String, Integer>();
    List<Tuple> currentTuple = inputWindow.get();
    String window = new String();

    // put the tuple from within current window into an sorted hashmap
    for (Tuple tuple : currentTuple) {
      String destsched = tuple.getStringByField("destinationschedule");
      Integer count = tuple.getIntegerByField("count");

      map.put(destsched, count);
      // collector.ack(tuple);
    }

    // Sort the hashmap
    Map<String, Integer> sortedMap = sortByValue(map);

    // To print out all destinations
    for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
      LOG.info(entry.getKey() + " has count of " + entry.getValue());
    }

    // To print out only top N destinations
    // Iterator it = sortedMap.entrySet().iterator();
    //
    // for (int N = 0; N < 21; N++) {
    // Map.Entry pair = (Map.Entry) it.next();
    // LOG.info(pair.getKey() + " has count of " + pair.getValue());
    // it.remove();
    // }



    LOG.info("End of window\n\n\n\n\n\n");

  }

  // Method to sort the destination count in descending order
  public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(
      Map<K, V> map) {
    List<Map.Entry<K, V>> list =
        new LinkedList<Map.Entry<K, V>>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
      public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
        return (o2.getValue()).compareTo(o1.getValue());
      }
    });

    Map<K, V> result = new LinkedHashMap<K, V>();
    for (Map.Entry<K, V> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

}
