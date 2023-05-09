package peersim.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import peersim.core.CommonState;
import peersim.core.Node;

/**
 * This class implements a kademlia k-bucket. Function for the management of the neighbours update
 * are also implemented
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KBucket implements Cloneable {
  // Replacement bucket
  protected List<BigInteger> replacements;

  protected RoutingTable rTable;

  // k-bucket array
  protected List<BigInteger> neighbours = null;

  // k-bucket size, max replacement bucket size
  protected int k, maxReplacements;

  // empty constructor
  public KBucket() {
    neighbours = new ArrayList<BigInteger>();
    replacements = new ArrayList<BigInteger>();
  }

  public KBucket(RoutingTable rTable, int k, int maxReplacements) {
    neighbours = new ArrayList<BigInteger>();
    replacements = new ArrayList<BigInteger>();
    this.k = k;
    this.maxReplacements = maxReplacements;
    this.rTable = rTable;
  }

  public void checkAndReplaceLast() {
    if (neighbours.size() == 0 || CommonState.getTime() == 0)
      // Entry has moved, don't replace it.
      return;

    // System.out.println("Replace node "+neighbours.get(neighbours.size()-1)+" at
    // "+CommonState.getTime());

    KademliaProtocol kProtocol = new KademliaProtocol(null);
    Node node = kProtocol.nodeIdtoNode(neighbours.get(neighbours.size() - 1));
    // System.out.println("Replace node "+neighbours.get(neighbours.size()-1)+" at
    // "+CommonState.getTime());
    // KademliaProtocol remote = node.getKademliaProtocol();

    if (kProtocol.routingTable != null) kProtocol.routingTable.sendToFront(rTable.nodeId);

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
  }

  // add a neighbour to this k-bucket
  public boolean addNeighbour(BigInteger node) {
    if (neighbours.size() < k) { // k-bucket isn't full

      neighbours.add(node); // add neighbour to the tail of the list
      removeReplacement(node);

      return true;

    } else {

      addReplacement(node);

      return false;
    }
  }

  // remove a neighbour from this k-bucket
  public void removeNeighbour(BigInteger node) {
    neighbours.remove(node);
  }

  public void addReplacement(BigInteger node) {
    if (!replacements.contains(node)) {
      replacements.add(0, node);

      if (replacements.size() > maxReplacements) {
        // Replacements is full - remove the last item
        replacements.remove(replacements.size() - 1);
      }
    }
  }

  public void removeReplacement(BigInteger node) {
    if (replacements.size() > 0) {
      replacements.remove(node);
    }
  }

  public Object clone() {
    KBucket dolly = new KBucket();
    for (BigInteger node : neighbours) {
      dolly.neighbours.add(new BigInteger(node.toByteArray()));
    }
    return dolly;
  }

  public String toString() {
    String res = "{\n";

    for (BigInteger node : neighbours) {
      res += node + "\n";
    }

    return res + "}";
  }
}
