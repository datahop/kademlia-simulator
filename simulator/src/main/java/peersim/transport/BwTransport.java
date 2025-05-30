package peersim.transport;

import peersim.config.Configuration;
import peersim.core.*;
import peersim.edsim.*;
import peersim.kademlia.Message;

public class BwTransport extends PairwiseFixedLatencyTransport {

  /** Store the time until which this node's uplink is busy sending data */
  private long[] uploadInterfaceBusyUntil;
  /** Size of the network. */
  private int size;
  /** Default upload bandwith of a validator in Mbits/sec */
  private static int[] bw;

  private static final String PAR_SIZE = "size";

  /** Reads configuration parameter. */
  public BwTransport(String prefix) {
    super(prefix);
    size = Configuration.getInt(prefix + "." + PAR_SIZE);
    bw = new int[size];
    uploadInterfaceBusyUntil = new long[size];
    // symmetric latency assumption so only need to allocate half of the matrix
    for (int i = 0; i < size; i++) {
      bw[i] = 0;
      uploadInterfaceBusyUntil[i] = 0;
    }
  }

  /**
   * Delivers the message with a pairwise (pre-)generated latency for the given pair of src, dest
   * nodes.
   */
  public void send(Node src, Node dest, Object msg, int pid) {
    // avoid calling nextLong if possible
    long latency = getLatency(src, dest);
    // If the interface is busy, incorporate the additional delay
    // also update the time when interface is available again
    int sender = ((int) src.getID()) % size;

    long timeNow = CommonState.getTime();
    long transmissionDelay = getTransmissionTime(msg, sender, dest);
    long delay;
    if (this.uploadInterfaceBusyUntil[sender] > timeNow * 1000) {
      this.uploadInterfaceBusyUntil[sender] += transmissionDelay;
      delay = latency + (uploadInterfaceBusyUntil[sender] / 1000 - timeNow);
    } else {
      this.uploadInterfaceBusyUntil[sender] = timeNow * 1000 + transmissionDelay;
      delay = transmissionDelay + latency;
    }
    if (delay < 0) {
      System.out.println(delay);
    }
    int size = 0;
    if (msg instanceof Message) size = ((Message) msg).getSize();

    /*System.out.println(
    "Node "
        + src.getID()
        + " now "
        + timeNow
        + " tx delay "
        + transmissionDelay
        + " busy til "
        + this.uploadInterfaceBusyUntil[sender] / 1000
        + " size "
        + size
        + " msg "
        + msg);*/
    EDSimulator.add(delay, msg, dest, pid);
  }

  public void setBw(Node src, int bw) {
    int sender = ((int) src.getID()) % size;
    BwTransport.bw[sender] = bw;
  }
  /**
   * Returns the assigned delay to the specific src and dest peers that was previously generated.
   */
  public long getTransmissionTime(Object msg, int sender, Node dest) {
    int size = 0;
    if (msg instanceof Message) size = ((Message) msg).getSize();
    if (size == 0) {
      return 0;
    }
    if (BwTransport.bw[sender] == 0) {
      return 0;
    }
    double msgSize = (double) size * 8 / 1000000;
    return (long) ((double) msgSize * 1000 * 1000 / BwTransport.bw[sender]);
  }
}
