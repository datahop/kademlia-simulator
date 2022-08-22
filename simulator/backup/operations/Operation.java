package peersim.kademlia.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
public class Operation {

  /** unique sequence number generator of the operation */
  private static long OPERATION_ID_GENERATOR = 0;

  public boolean finished = false;
  /** represent univocally the find operation */
  public long operationId;

  /** Type of the initial message */
  public int type;

  /** Id of the node to find */
  public BigInteger destNode;

  public BigInteger srcNode;

  /** Body of the original find message */
  public Object body;

  /** number of available find request message to send (it must be always less than ALPHA) */
  public int available_requests;

  /** Start timestamp of the search operation */
  protected long timestamp = 0;

  /** Number of hops the message did */
  public int nrHops = 0;

  /**
   * This map contains the K closest nodes and corresponding boolean value that indicates if the
   * nodes has been already queried or not
   */
  protected HashMap<BigInteger, Boolean> closestSet;

  protected ArrayList<BigInteger> returned;

  protected ArrayList<BigInteger> used;

  /**
   * defaul constructor
   *
   * @param destNode Id of the node to find
   */
  public Operation(BigInteger srcNode, BigInteger dstNode, int type, long timestamp) {
    this.timestamp = timestamp;
    this.destNode = dstNode;
    this.type = type;
    this.srcNode = srcNode;

    // set a new find id
    operationId = OPERATION_ID_GENERATOR++;

    // System.out.println("new operation "+operationId);
    // System.out.println("Operation type "+type+" "+operationId);
    // set availabe request to ALPHA
    available_requests = KademliaCommonConfig.ALPHA;

    // initialize closestSet
    closestSet = new HashMap<BigInteger, Boolean>();

    returned = new ArrayList<BigInteger>();
    used = new ArrayList<BigInteger>();
  }

  /**
   * update closestSet with the new information received
   *
   * @param neighbours
   */
  public void elaborateResponse(BigInteger[] neighbours) {
    // update responseNumber
    available_requests++;

    int max =
        KademliaCommonConfig
            .K; // <KademliaCommonConfig.N?KademliaCommonConfig.K:KademliaCommonConfig.N;

    // add to closestSet
    /*		for (BigInteger n : neighbours) {
    		if (n != null) {
    			if (!closestSet.containsKey(n)) {
    				if (closestSet.size() < max) { // add directly
    					closestSet.put(n, false);
    				} else { // find in the closest set if there are nodes whit less distance
    					int newdist = Util.logDistance(n, destNode);

    					// find the node with max distance
    					int maxdist = newdist;
    					BigInteger nodemaxdist = n;
    					for (BigInteger i : closestSet.keySet()) {
    						int dist = Util.logDistance(i, destNode);

    						if (dist > maxdist) {
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
    */
    // add to closestSet
    for (BigInteger n : neighbours) {

      if (n != null) {
        if (!closestSet.containsKey(n)) {
          if (closestSet.size() < KademliaCommonConfig.K) { // add directly
            closestSet.put(n, false);
          } else { // find in the closest set if there are nodes whit less distance
            BigInteger newdist = Util.distance(n, destNode);

            // find the node with max distance
            BigInteger maxdist = newdist;
            BigInteger nodemaxdist = n;
            for (BigInteger i : closestSet.keySet()) {
              BigInteger dist = Util.distance(i, destNode);

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

    /*String s = "closestSet to " + destNode + "\n";
    for (BigInteger clos : closestSet.keySet()) {
    	 s+= clos + "-";
    }
    System.out.println(s);*/

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
          // System.out.println("Res = "+n);
          res = n;
        } else if (Util.logDistance(n, destNode) < Util.logDistance(res, destNode)) {
          res = n;
          // System.out.println("Res = "+n);
        }
      } else {
        // System.out.println("Res = "+n+" alreaady queried");

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

  public void increaseReturned(BigInteger res) {
    returned.add(res);
  }

  public void increaseUsed(BigInteger res) {
    used.add(res);
  }

  public int getReturnedCount() {
    return returned.size();
  }

  public int getUsedCount() {
    return used.size();
  }
}
