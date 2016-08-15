package stormairline.topology;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;


import java.util.HashMap;
import java.util.Map;

public class CountFlightnum extends BaseBasicBolt {
  private Map<String, Integer> counts;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("cityname", "count"));
  }

  @Override
  public void prepare(Map StormConf, TopologyContext context) {
    counts = new HashMap<String, Integer>();
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {

    String cityname = tuple.getStringByField("cityname");

    Integer count = counts.get(cityname);
    if (count == null)
      count = 0;
    count++;
    counts.put(cityname, count);

    printFlightCount();
  }

  private void printFlightCount() {
    for (String cityname : counts.keySet()) {
      System.out.println(String.format("%s has count of %s", cityname,
          counts.get(cityname)));
    }
  }
}
