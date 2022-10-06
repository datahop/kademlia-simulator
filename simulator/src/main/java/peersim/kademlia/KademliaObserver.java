package peersim.kademlia;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.util.IncrementalStats;

/**
 * This class implements a simple observer of search time and hop average in finding a node in the
 * network
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KademliaObserver implements Control {

  /** Configuration strings to read */
  private static final String PAR_STEP = "step";

  /** keep statistics of the number of hops of every message delivered. */
  public static IncrementalStats hopStore = new IncrementalStats();

  /** keep statistics of the time every message delivered. */
  public static IncrementalStats timeStore = new IncrementalStats();

  /** keep statistic of number of message delivered */
  public static IncrementalStats msg_deliv = new IncrementalStats();

  /** keep statistic of number of find operation */
  public static IncrementalStats find_op = new IncrementalStats();

  /** Parameter of the protocol we want to observe */
  private static final String PAR_PROT = "protocol";

  /** Successfull find operations */
  public static IncrementalStats find_ok = new IncrementalStats();

  private static HashMap<String, Map<String, Object>> messages =
      new HashMap<String, Map<String, Object>>();

  /** Name of the folder where experiment logs are written */
  private static String logFolderName;

  /** The time granularity of reporting metrics */
  private static int observerStep;

  public KademliaObserver(String prefix) {
    observerStep = Configuration.getInt(prefix + "." + PAR_STEP);

    logFolderName = "./logs";
  }

  private static void writeMap(Map<String, Map<String, Object>> map, String filename) {
    try (FileWriter writer = new FileWriter(filename)) {
      Set<String> keySet = map.entrySet().iterator().next().getValue().keySet();
      String header = "";
      for (Object key : keySet) {
        header += key + ",";
      }
      // remove the last comma
      header = header.substring(0, header.length() - 1);
      header += "\n";
      writer.write(header);

      for (Map<String, Object> entry : messages.values()) {
        String line = "";
        for (Object key : keySet) {
          line += "," + entry.get(key).toString();
        }
        // remove the last comma
        line = line.substring(0, line.length() - 1);
        line += "\n";
        writer.write(line);
      }
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writeOut() {
    File directory = new File(logFolderName);
    if (!directory.exists()) {
      directory.mkdir();
    }
    if (!messages.isEmpty()) {
      writeMap(messages, logFolderName + "/" + "messages.csv");
    }
  }

  /**
   * print the statistical snapshot of the current situation
   *
   * @return boolean always false
   */
  public boolean execute() {
    // get the real network size
    int sz = Network.size();
    for (int i = 0; i < Network.size(); i++) if (!Network.get(i).isUp()) sz--;

    System.gc();
    String s =
        String.format(
            "[time=%d]:[N=%d current nodes UP] [D=%f msg deliv] [%f min h] [%f average h] [%f max h] [%d min l] [%d msec average l] [%d max l] [%d find msg sent]",
            CommonState.getTime(),
            sz,
            msg_deliv.getSum(),
            hopStore.getMin(),
            hopStore.getAverage(),
            hopStore.getMax(),
            (int) timeStore.getMin(),
            (int) timeStore.getAverage(),
            (int) timeStore.getMax(),
            (int) find_op.getSum());

    if (CommonState.getEndTime() <= (observerStep + CommonState.getTime())) {
      // Last execute cycle of the experiment
      writeOut();
    }

    System.err.println(s);

    return false;
  }

  public static void reportMsg(Message m, boolean sent) {
    // messages without source are control messages sent by the traffic control
    // we don't want to log them
    if (m.src == null) return;

    assert (!messages.keySet().contains(String.valueOf(m.id)));
    messages.put(String.valueOf(m.id), m.toMap(sent));
  }
}
