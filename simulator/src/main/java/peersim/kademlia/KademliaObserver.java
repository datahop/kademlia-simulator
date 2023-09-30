package peersim.kademlia;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.kademlia.das.Neighbour;
import peersim.kademlia.das.SearchTable;
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

  private static final String PAR_FOLDER = "logfolder";

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
  private static HashMap<Long, Map<String, Object>> messages =
      new HashMap<Long, Map<String, Object>>();

  /** Log of operations in the Kademlia network */
  private static HashMap<Long, Map<String, Object>> operations =
      new HashMap<Long, Map<String, Object>>();

  /** Log of operations in the Kademlia network */
  private static HashMap<Long, Map<String, Object>> peerDiscoveries =
      new HashMap<Long, Map<String, Object>>();

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

    logFolderName = Configuration.getString(prefix + "." + PAR_FOLDER, "./logs");

    System.out.println("Logfolder: " + logFolderName);
  }

  private static void writeLogs(Map<Long, Map<String, Object>> map, String filename) {
    try (FileWriter writer = new FileWriter(filename)) {
      Set<String> keySet = new HashSet<String>();
      for (Map<String, Object> m : map.values())
        if (m.keySet().size() > keySet.size()) keySet = m.keySet();

      // Set<String> keySet = map.entrySet().iterator().next().getValue().keySet();
      String header = "";
      for (Object key : keySet) {
        header += key + ",";
      }

      // Write the comma seperated keys as the header of the file
      header = header.substring(0, header.length() - 1);
      header += "\n";
      writer.write(header);
      // Iterate through each find operation and write its data to the file
      for (Map<String, Object> entry : map.values()) {
        String line = "";
        for (Object key : keySet) {
          if (entry.get(key) != null) line += entry.get(key).toString() + ",";
          else line += ",";
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
    map.clear();
  }

  /** Writes log data to files. */
  public static void writeOut() {
    // System.out.println("Writing out");

    File directory = new File(logFolderName);
    if (!directory.exists()) {
      directory.mkdir();
    }
    // Write messages log to file if not empty
    if (!messages.isEmpty()) {
      writeLogs(messages, logFolderName + "/" + "messages.csv");
    }
    if (!operations.isEmpty()) {
      writeLogs(operations, logFolderName + "/" + "operation.csv");
    }
    if (!peerDiscoveries.isEmpty()) {
      writeLogs(peerDiscoveries, logFolderName + "/" + "peerDiscoveries.csv");
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
    assert (!messages.keySet().contains(m.id));
    messages.put(m.id, m.toMap(sent));
  }

  /**
   * Reports an operation, adding it to the operation log.
   *
   * @param op The operation to report.
   */
  public static void reportOperation(Operation op) {
    // messages without source are control messages sent by the traffic control
    // Calculate the operation stop time and then add the opearation to the operation log.
    assert (!operations.keySet().contains(op.getId()));
    op.setStopTime(CommonState.getTime() - op.getTimestamp());
    operations.put(op.getId(), op.toMap());
  }

  public static void reportPeerDiscovery(Message m, SearchTable st) {

    if (m.src == null) return;
    // Add the message to the message log, but first check if it hasn't already been added
    assert (!peerDiscoveries.keySet().contains(m.id));
    Map<String, Object> result = new HashMap<String, Object>();
    Neighbour[] neighs = (Neighbour[]) m.value;

    int notKnown = 0;
    for (Neighbour n : neighs) {
      if (!st.isNeighbourKnown(n)) notKnown++;
    }
    result.put("time", CommonState.getTime());
    result.put("message_id", m.id);
    result.put("dst_id", m.dst.getId());
    result.put("src_id", m.src.getId());
    result.put("total_peers", st.getAllNeighboursCount());
    result.put("total_peers_alive", st.getAllAliveNeighboursCount());
    result.put("peers_in_message", neighs.length);
    result.put("peers_not_known", notKnown);

    peerDiscoveries.put(m.id, result);
  }
}
