package peersim.kademlia.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.graphstream.graph.Graph;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.ui.view.Viewer;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.UniformRandomGenerator;
import peersim.kademlia.Util;

/**
 * This class represents a find operation and offer the methods needed to maintain and update the
 * closest set.<br>
 * It also maintains the number of parallel requsts that can has a maximum of ALPHA.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class FindOperation extends Operation {

  /** number of available find request message to send (it must be always less than ALPHA) */
  protected int available_requests;
  /**
   * This map contains the K closest nodes and corresponding boolean value that indicates if the
   * nodes has been already queried or not
   */
  protected HashMap<BigInteger, Boolean> closestSet;

  /**
   * defaul constructor
   *
   * @param srcNode Id of the node to find
   * @param destNode Id of the node to find
   * @param timestamp Id of the node to find
   */
  public FindOperation(BigInteger srcNode, BigInteger destNode, long timestamp) {
    super(srcNode, destNode, timestamp);
    // initialize closestSet
    closestSet = new HashMap<BigInteger, Boolean>();
    returned = new ArrayList<BigInteger>();
  }

  public int getAvailableRequests() {
    return this.available_requests;
  }

  public void increaseAvailableRequests() {
    this.available_requests++;
  }

  public void setAvailableRequests(int requests) {
    this.available_requests = requests;
  }

  public HashMap<BigInteger, Boolean> getClosest() {
    return this.closestSet;
  }

  /**
   * update closestSet with the new information received
   *
   * @param neighbours
   */
  public void elaborateResponse(BigInteger[] neighbours) {

    // add to closestSet
    for (BigInteger n : neighbours) {

      if (n != null) {
        if (!closestSet.containsKey(n)) {
          if (closestSet.size() < KademliaCommonConfig.K) { // add directly
            closestSet.put(n, false);
          } else { // find in the closest set if there are nodes whit less distance
            BigInteger newdist = Util.xorDistance(n, destNode);

            // find the node with max distance
            BigInteger maxdist = newdist;
            BigInteger nodemaxdist = n;
            for (BigInteger i : closestSet.keySet()) {
              BigInteger dist = Util.xorDistance(i, destNode);

              if (dist.compareTo(maxdist) > 0) {
                maxdist = dist;
                nodemaxdist = i;
              }
            }

            if (nodemaxdist.compareTo(n) != 0) {
              closestSet.remove(nodemaxdist);
              closestSet.put(n, false);
            }
          }
        }
      }
    }
    // update responseNumber
    available_requests++;
  }

  /**
   * get the first neighbour in closest set which has not been already queried
   *
   * @return the Id of the node or null if there aren't available node
   */
  public BigInteger getNeighbour() {
    // find closest neighbour ( the first not already queried)
    BigInteger res = null;
    for (BigInteger n : closestSet.keySet()) {
      if (n != null && closestSet.get(n) == false) {
        if (res == null) {
          res = n;
        } else if (Util.xorDistance(n, destNode).compareTo(Util.xorDistance(res, destNode)) < 0) {
          res = n;
        }
      }
    }

    // Has been found a valid neighbour
    if (res != null) {
      closestSet.remove(res);
      closestSet.put(res, true);
      // increaseUsed(res);
      available_requests--; // decrease available request
    }

    return res;
  }

  /**
   * get the neighbours in closest set which has not been already queried
   *
   * @return the closest nodes set up to K
   */
  public List<BigInteger> getNeighboursList() {
    return new ArrayList<BigInteger>(closestSet.keySet());
    // return new ArrayList<BigInteger>(closestSet.keySet()).subList(0, KademliaCommonConfig.K-1);
  }

  public void visualize() {
    Graph graph = new SingleGraph("Operation");
    UniformRandomGenerator urg =
        new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    BigInteger max = urg.getMaxID();
    int kademliaid = Configuration.getPid("init.2statebuilder" + ".protocol");

    for (int i = 0; i < Network.size(); i++) {
      Node node = Network.get(i);
      KademliaProtocol prot = (KademliaProtocol) (node.getProtocol(kademliaid));
      BigInteger id = prot.node.getId();
      double ratio = id.doubleValue() / max.doubleValue() * 360;
      double alpha = Math.toRadians(ratio);
      double x = Math.cos(alpha);
      double y = Math.sin(alpha);

      org.graphstream.graph.Node gnode = graph.addNode(id.toString());
      gnode.setAttribute("x", x);
      gnode.setAttribute("y", y);
      if (returned.contains(id)) {
        gnode.setAttribute(
            "ui.style", "fill-color: rgb(255,0,0); size: 50px, 50px;"); // text-size: 30pt");
        gnode.setAttribute("label", String.valueOf(returned.indexOf(id)));
      } else if (id.equals(srcNode)) {
        gnode.setAttribute("ui.style", "fill-color: rgb(0,0,255); size: 50px, 50px;");
      } else {
        gnode.setAttribute("ui.style", "fill-color: rgba(0,100,255, 50); size: 8px, 8px;");
      }
    }
    org.graphstream.graph.Node dst = graph.getNode(destNode.toString());
    if (dst == null) {
      dst = graph.addNode(destNode.toString());
      double ratio = destNode.doubleValue() / max.doubleValue() * 360;
      double alpha = Math.toRadians(ratio);
      double x = Math.cos(alpha);
      double y = Math.sin(alpha);
      dst.setAttribute("x", x);
      dst.setAttribute("y", y);
    }
    dst.setAttribute("ui.style", "fill-color: rgb(0,255,0); size: 50px, 50px;");

    System.setProperty("org.graphstream.ui", "swing");
    Viewer viewer = graph.display();
    viewer.disableAutoLayout();
  }

  public Map<String, Object> toMap() {
    Map<String, Object> result = new HashMap<String, Object>();

    result.put("id", this.operationId);
    result.put("src", this.srcNode);
    result.put("type", this.getClass().getSimpleName());
    result.put("messages", getMessagesString());
    result.put("start", this.timestamp);
    result.put("stop", this.stopTime);
    result.put("hops", this.nrHops);
    return result;
  }
}
