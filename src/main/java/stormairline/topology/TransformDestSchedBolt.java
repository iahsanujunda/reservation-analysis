package stormairline.topology;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import stormairline.utils.DestinationHashmap;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TransformDestSchedBolt extends BaseBasicBolt {

  // load a predefined hashmap containing the list of airport and cities
  DestinationHashmap dhm;

  // declare output field of this bolt
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("time",
        "destinationschedule", "freqflyer"));
  }

  // initialize environment
  @Override
  public void prepare(Map StormConf, TopologyContext context) {
    dhm = new DestinationHashmap();
  }

  // execution of tuple stream
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    HashMap<String, String> hm = dhm.getDestmap();

    // initialize tuples into variables
    String time = tuple.getStringByField("time");
    Integer schedule = tuple.getIntegerByField("schedule");
    String destination = tuple.getStringByField("destination");
    Integer freqflyer = tuple.getIntegerByField("freqflyer");

    // extract month from flight schedules, january becomes JAN, february
    // becomes FEB, so on
    String scheduleAsString = String.valueOf(schedule);
    Date date = null;

    try {
      date = new SimpleDateFormat("yyyyMMdd").parse(scheduleAsString);
    } catch (ParseException e) {
      e.printStackTrace();
    }

    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int monthAsInt = cal.get(Calendar.MONTH);
    String month = getMonthForInt(monthAsInt);
    String monthThreeDigit = month.substring(0, 3);

    // testing purpose: emit format airportcode_month instead of city_month
    // String destsched = destination.concat("_").concat(monthThreeDigit);

    // Loop through the content of destinationHashmap to look up city name from
    // airport code
    outloop: for (Map.Entry<String, String> entry : hm.entrySet()) {
      String airport = entry.getKey();
      
      if (destination.contains(airport)) {
        String cityname = entry.getValue();

        String destsched = cityname.concat("_").concat(monthThreeDigit);
        collector.emit(new Values(time, destsched, freqflyer));

        break outloop;
      }
    }
  }

  // method to extract month from integer format
  private String getMonthForInt(int m) {
    String month = "invalid";
    DateFormatSymbols dfs = new DateFormatSymbols();
    String[] months = dfs.getMonths();
    if (m >= 0 && m <= 11) {
      month = months[m];
    }
    return month;
  }
}
