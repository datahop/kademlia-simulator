package peersim.kademlia;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

  /** Protocol id */
  private int pid;

  /** Name of the folder where experiment logs are written */
  private static String logFolderName;

  /** Prefix to be printed in output */
  private String prefix;

  /** The time granularity of reporting metrics */
  private static int observerStep;
  /** Message writer object */
  private static FileWriter msgWriter;

  public KademliaObserver(String prefix) {
    this.prefix = prefix;
    pid = Configuration.getPid(prefix + "." + PAR_PROT);
    this.observerStep = Configuration.getInt(prefix + "." + PAR_STEP);

    this.logFolderName = "./logs";

    File directory = new File(this.logFolderName);
    if (!directory.exists()) {
      directory.mkdir();
    }
    try {
      msgWriter = new FileWriter(this.logFolderName + "/" + "messages.csv");
      msgWriter.write("id,type,src,dst,sent/received\n");
    } catch (IOException e) {
      e.printStackTrace();
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

    if (CommonState.getEndTime() <= (this.observerStep + CommonState.getTime())) {
      // Last execute cycle of the experiment
    }

    System.err.println(s);

    return false;
  }

  public static void reportMsg(Message m, boolean sent) {
    try {

      String result = "";
      if (m.src == null) return; // ignore init messages
      result += m.id + "," + m.messageTypetoString() + "," + m.src + "," + m.dest + ",";
      if (sent) {
        result += "sent\n";
      } else {
        result += "received\n";
      }
      msgWriter.write(result);
      msgWriter.flush();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
