package stormairline.topology;

import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.apache.commons.io.IOUtils;

import stormairline.utils.DatasetList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import java.nio.charset.Charset;

public class TupleGeneratorSpout extends BaseRichSpout {

  // Instantiate variables
  private ArrayList<String> reservations = new ArrayList<String>();
  private int nextEmitIndex;
  private SpoutOutputCollector outputCollector;
  private DatasetList dshm;

  // Declare all the output fields of this spout
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("time", "schedule", "flightnum", "destination",
        "freqflyer"));
  }

  // Do something before this spout starts generating tuples
  @Override
  public void open(Map map, TopologyContext context,
      SpoutOutputCollector collector) {
    this.outputCollector = collector;
    this.nextEmitIndex = 0;

    dshm = new DatasetList();

//    try {
//      // read line-by-line from a file called dataset.txt
//      reservations =
//          IOUtils.readLines(ClassLoader
//              .getSystemResourceAsStream("dataset.txt"), Charset
//              .defaultCharset().name());
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
  }

  // Iterate this procedure through tuple stream
  @Override
  public void nextTuple() {
    List<String> reservations = dshm.getDataset();

    // set delay time between each tuple process
    Utils.sleep(4);
    String reservation = reservations.get(nextEmitIndex);

    // Define which column of dataset becomes which field of tuple
    String[] parts = reservation.split(",");
    String time = parts[0];
    int schedule = Integer.parseInt(parts[1]);
    String flightnum = parts[2];
    String destination = parts[3];
    int freqflyer = Integer.parseInt(parts[4]);

    // put the fields together to the output collector
    outputCollector.emit(new Values(time, schedule, flightnum, destination,
        freqflyer));

    // increase the index of line to emit tuples from, unless the index exceed
    // the number of lines within raw dataset, in which case set the emitindex
    // back to 0 and let the spout sleep for 100ms.
    if (nextEmitIndex == reservations.size() - 1) {
      nextEmitIndex = 0;
      Utils.sleep(100);
    } else {
      nextEmitIndex = nextEmitIndex + 1;
    }
  }

}
