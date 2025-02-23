package peersim.transport;

import peersim.config.*;
import peersim.core.*;
import peersim.edsim.*;
import peersim.kademlia.Message;

public class BwTransport extends PairwiseFixedLatencyTransport {

  /** Store the time until which this node's uplink is busy sending data */
  private long uploadInterfaceBusyUntil;

  /** Default upload bandwith of a validator in Mbits/sec */
  public int bw;

  /** Latencies between peers (symmetric). */
  private static long[][] pairwise_lat;

  /**
   * String name of the parameter used to configure the minimum latency.
   *
   * @config
   */
  private static final String PAR_SIZE = "size";

  /** Reads configuration parameter. */
  public BwTransport(String prefix) {
    super(prefix);
    uploadInterfaceBusyUntil = 0;
    bw = 0;
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
  }

  public void setBw(int bw) {
    this.bw = bw;
  }
  /**
   * Returns the assigned delay to the specific src and dest peers that was previously generated.
   */
  public double getTransmissionTime(Object msg, Node src, Node dest) {
    int size = 0;
    if (msg instanceof Message) size = ((Message) msg).getSize();
    if (size == 0) {
      return 0;
    }
    if (bw == 0) {
      return 0;
    }
    return 1000 * (double) size / bw;
  }
}
