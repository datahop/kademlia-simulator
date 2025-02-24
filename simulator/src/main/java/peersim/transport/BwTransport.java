package peersim.transport;

import peersim.config.Configuration;
import peersim.core.*;
import peersim.edsim.*;
import peersim.kademlia.Message;

public class BwTransport extends PairwiseFixedLatencyTransport {

  /** Store the time until which this node's uplink is busy sending data */
  private long uploadInterfaceBusyUntil;
  /** Size of the network. */
  private int size;
  /** Default upload bandwith of a validator in Mbits/sec */
  private static int[] bw;

  private static final String PAR_SIZE = "size";

  /** Reads configuration parameter. */
  public BwTransport(String prefix) {
    super(prefix);
    uploadInterfaceBusyUntil = 0;
    size = Configuration.getInt(prefix + "." + PAR_SIZE);
    bw = new int[size];
    // symmetric latency assumption so only need to allocate half of the matrix
    for (int i = 0; i < size; i++) {
      bw[i] = 0;
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
    long timeNow = CommonState.getTime();
    long transmissionTime = (long) getTransmissionTime(msg, src, dest);
    if (this.uploadInterfaceBusyUntil > timeNow) {
      transmissionTime += this.uploadInterfaceBusyUntil - timeNow;
      this.uploadInterfaceBusyUntil += transmissionTime;
    } else {
      this.uploadInterfaceBusyUntil = timeNow + transmissionTime;
    }
    EDSimulator.add(latency + transmissionTime, msg, dest, pid);
    // EDSimulator.add(latency, msg, dest, pid);
  }

  public void setBw(Node src, int bw) {
    int sender = ((int) src.getID()) % size;
    BwTransport.bw[sender] = bw;
  }
  /**
   * Returns the assigned delay to the specific src and dest peers that was previously generated.
   */
  public long getTransmissionTime(Object msg, Node src, Node dest) {
    int size = 0;
    if (msg instanceof Message) size = ((Message) msg).getSize();
    if (size == 0) {
      return 0;
    }
    int sender = ((int) src.getID()) % size;
    if (BwTransport.bw[sender] == 0) {
      return 0;
    }
    double msgSize = size * 8 / 1000000;
    return (long) ((double) msgSize * 1000 / BwTransport.bw[sender]);
  }
}
