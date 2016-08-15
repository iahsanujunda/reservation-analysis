package stormairline.topology;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

public class CombinerBolt extends BaseWindowedBolt {
  private Map<String, Integer> map;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("destsched", "count"));
  }

  @Override
  public void execute(TupleWindow inputWindow) {
    map = new HashMap<String, Integer>();
    List<Tuple> currentTuple = inputWindow.get();

    for (Tuple tuple : currentTuple) {
      String destsched = tuple.getStringByField("destinationschedule");
      Integer count = tuple.getIntegerByField("count");

      map.put(destsched, count);
    }

    System.out.println("============================");
    printWindow();
    System.out.println("============================");

    Iterator it = map.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry) it.next();
      System.out.println(pair.getKey() + " has count of " + pair.getValue());
      it.remove();
    }

    System.out.println("============================\n\n");

  }

  private void printWindow() {
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    Date date = new Date();
    System.out.println("window : " + dateFormat.format(date));
  }

}
