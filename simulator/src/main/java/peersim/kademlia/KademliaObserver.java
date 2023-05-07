package peersim.kademlia;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.kademlia.operations.Operation;
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

  /** Messages exchanged in the Kademlia network */
  private static HashMap<String, Map<String, Object>> messages =
      new HashMap<String, Map<String, Object>>();

  /** Log of the "FIND" operations of the Kademlia network */
  private static HashMap<String, Map<String, Object>> find_log =
      new HashMap<String, Map<String, Object>>();

  /** Name of the folder where experiment logs are written */
  private static String logFolderName;

  /** The time granularity of reporting metrics */
  private static int observerStep;

  /**
   * Constructor to initialize the observer.
   *
   * @param prefix the configuration prefix
   */
  public KademliaObserver(String prefix) {
    observerStep = Configuration.getInt(prefix + "." + PAR_STEP);

    logFolderName = "./logs";
  }

  /**
   * Writes a map of messages to a file.
   *
   * @param map the map to write
   * @param filename the name of the file to write to
   */
  private static void writeMap(Map<String, Map<String, Object>> map, String filename) {
    try (FileWriter writer = new FileWriter(filename)) {
      // Set<String> keySet = map.entrySet().iterator().next().getValue().keySet();
      // Define the expected order of keys in the header
      String[] keys = {"src", "dst", "id", "type", "status"};

      // Write the comma seperated keys as the header of the file
      String header = String.join(",", keys) + "\n";
      writer.write(header);

      // String header = "";
      // for (Object key : keySet) {
      //   header += key + ",";
      // }

      // // Remove the last comma
      // header = header.substring(0, header.length() - 1);
      // header += "\n";
      // writer.write(header);

      // Iterate through each message and writes its content to a file
      for (Map<String, Object> entry : messages.values()) {
        String line = "";
        for (Object key : keys) {
          line += entry.get(key).toString() + ",";
        }
        // Remove the last comma
        line = line.substring(0, line.length() - 1);
        line += "\n";
        writer.write(line);
      }
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Writes a map of find operations to a file.
   *
   * @param map the map to write
   * @param filename the name of the file to write to
   */
  private static void writeMapFind(Map<String, Map<String, Object>> map, String filename) {
    try (FileWriter writer = new FileWriter(filename)) {
      // Define the expected order of keys in the header
      String[] keys = {"src", "start", "stop", "messages", "hops", "id", "type"};

      // Get the key set of the first entry in the map and use it to create the header
      // Set<String> keySet = map.entrySet().iterator().next().getValue().keySet();

      // Write the comma seperated keys as the header of the file
      String header = String.join(",", keys) + "\n";
      writer.write(header);

      // String header = "";
      // for (Object key : keySet) {
      //   header += key + ",";
      // }

      // Remove the last comma and add a newline character
      // header = header.substring(0, header.length() - 1);
      // header += "\n";
      // writer.write(header);

      // Iterate through each find operation and write its data to the file
      for (Map<String, Object> entry : find_log.values()) {
        String line = "";
        for (Object key : keys) {
          line +=
              entry.get(key).toString()
                  + ","; // Append the string representation of the value to line
        }

        // Remove the last comma and add a newline character
        line = line.substring(0, line.length() - 1);
        line += "\n";
        writer.write(line);
      }
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /** Writes log data to files. */
  public static void writeOut() {
    // Create log directory if it does not exist
    File directory = new File(logFolderName);
    if (!directory.exists()) {
      directory.mkdir();
    }
    // Write messages log to file if not empty
    if (!messages.isEmpty()) {
      writeMap(messages, logFolderName + "/" + "messages.csv");
    }
    // Write find operations log to file if not empty
    if (!find_log.isEmpty()) {
      writeMapFind(find_log, logFolderName + "/" + "operation.csv");
      // System.out.println(
      //     "The average hope and latency " + hopStore.getAverage() + ", " +
      // timeStore.getAverage());
    }
  }

  /**
   * Print the statistical snapshot of the current situation.
   *
   * @return always false
   */
  public boolean execute() {
    // Get the real network size
    int sz = Network.size();
    for (int i = 0; i < Network.size(); i++) {
      if (!Network.get(i).isUp()) {
        sz--;
      }
    }

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

    // Check if this is the last execution cycle of the experiment
    if (CommonState.getEndTime() <= (observerStep + CommonState.getTime())) {
      // Write out the logs to disk/permanent storage
      writeOut();
      // System.err.println(s);
    }

    return false;
  }

  /**
   * Reports a message, adding it to the message log if it has a source.
   *
   * @param m The message to report
   * @param sent a boolean indicating whether the message was sent or received.
   */
  public static void reportMsg(Message m, boolean sent) {
    // Messages without a source are control messages sent by the traffic control,
    // so we don't want to log them.
    if (m.src == null) return;

    // Add the message to the message log, but first check if it hasn't already been added
    assert (!messages.keySet().contains(String.valueOf(m.id)));
    messages.put(String.valueOf(m.id), m.toMap(sent));
  }

  /**
   * Reports an operation, adding it to the find operation log.
   *
   * @param op The operation to report.
   */
  public static void reportOperation(Operation op) {
    // Operations without a source are control messages sent by the traffic control,
    // so we don't want to log them.
    /*if (fLog.src == null) {
      return;
    }
    find_log.put(String.valueOf(fLog.id), fLog.toMap());*/

    // Calculate the operation stop time and then add the opearation to the find operation log.
    op.setStopTime(CommonState.getTime() - op.getTimestamp());
    find_log.put(String.valueOf(op.getId()), op.toMap());
  }
}
