package peersim.transport;

import peersim.config.*;
import peersim.core.*;
import peersim.edsim.*;

public final class PairwiseFixedLatencyTransport extends UniformRandomTransport {

  /** Size of the network. */
  private int size;

  /** Latencies between peers (symmetric). */
  private static long[][] pairwise_lat;

  /**
   * String name of the parameter used to configure the minimum latency.
   *
   * @config
   */
  private static final String PAR_SIZE = "size";

  /** Reads configuration parameter. */
  public PairwiseFixedLatencyTransport(String prefix) {
    super(prefix);
    // generate the delays or just the array
    size = Configuration.getInt(prefix + "." + PAR_SIZE);
    pairwise_lat = new long[size][];
    // symmetric latency assumption so only need to allocate half of the matrix
    for (int i = 0; i < size; i++) {
      pairwise_lat[i] = new long[i + 1];
    }
  }

  /**
   * Delivers the message with a pairwise (pre-)generated latency for the given pair of src, dest
   * nodes.
   */
  public void send(Node src, Node dest, Object msg, int pid) {
    // avoid calling nextLong if possible
    long delay = getLatency(src, dest);

    EDSimulator.add(delay, msg, dest, pid);
  }

  /**
   * Returns the assigned delay to the specific src and dest peers that was previously generated.
   */
  public long getLatency(Node src, Node dest) {
    int sender = ((int) src.getID()) % size;
    int receiver = ((int) dest.getID()) % size;
    if (sender < receiver) {
      int tmp = sender;
      sender = receiver;
      receiver = tmp;
    }
    if (pairwise_lat[sender][receiver] == 0) {
      // compute the latency on-demand
      pairwise_lat[sender][receiver] = (range == 1 ? min : min + CommonState.r.nextLong(range));
    }
    return pairwise_lat[sender][receiver];
  }
}
