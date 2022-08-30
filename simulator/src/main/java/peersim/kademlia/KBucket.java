package peersim.kademlia;

import java.math.BigInteger;
import java.util.TreeMap;
import peersim.core.CommonState;

/**
 * This class implements a kademlia k-bucket. Function for the management of the neighbours update
 * are also implemented
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KBucket implements Cloneable {

  // k-bucket array
  protected TreeMap<BigInteger, Long> neighbours = null;

  // empty costructor
  public KBucket() {
    neighbours = new TreeMap<BigInteger, Long>();
  }

  // add a neighbour to this k-bucket
  public boolean addNeighbour(BigInteger node) {
    long time = CommonState.getTime();
    if (neighbours.size() < KademliaCommonConfig.K) { // k-bucket isn't full
      neighbours.put(node, time); // add neighbour to the tail of the list
      return true;
    }
    return false;
  }

  // remove a neighbour from this k-bucket
  public void removeNeighbour(BigInteger node) {
    neighbours.remove(node);
  }

  public void checkAndReplaceLast() {
    if (neighbours.size() == 0 || CommonState.getTime() == 0)
      // Entry has moved, don't replace it.
      return;

    // System.out.println("Replace node "+neighbours.get(neighbours.size()-1)+" at
    // "+CommonState.getTime());
    /*
    Node node = Util.nodeIdtoNode(neighbours.get(neighbours.size() - 1));
    // System.out.println("Replace node "+neighbours.get(neighbours.size()-1)+" at
    // "+CommonState.getTime());
    KademliaProtocol remote = node.getKademliaProtocol();

    if (remote.routingTable != null) remote.routingTable.sendToFront(rTable.nodeId);

    // System.out.println("checkAndReplaceLast "+remote.getNode().getId()+" at
    // "+CommonState.getTime()+" at "+rTable.nodeId);

    if (node.getFailState() != Node.OK) {
      // Still the last entry.
      neighbours.remove(neighbours.size() - 1);
      if (replacements.size() > 0) {
        // Random rand = new Random();
        // BigInteger n = replacements.get(rand.nextInt(replacements.size()));
        BigInteger n = replacements.get(CommonState.r.nextInt(replacements.size()));
        neighbours.add(n);
        replacements.remove(n);
      }
    }
    */
  }

  public Object clone() {
    KBucket dolly = new KBucket();
    for (BigInteger node : neighbours.keySet()) {
      dolly.neighbours.put(new BigInteger(node.toByteArray()), 0l);
    }
    return dolly;
  }

  public String toString() {
    String res = "{\n";

    for (BigInteger node : neighbours.keySet()) {
      res += node + "\n";
    }

    return res + "}";
  }
}
