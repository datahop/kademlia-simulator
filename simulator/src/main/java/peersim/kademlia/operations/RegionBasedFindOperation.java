package peersim.kademlia.operations;

import java.math.BigInteger;
import java.util.HashMap;
import peersim.core.Network;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.Util;

public class RegionBasedFindOperation extends FindOperation {

  // minimum common prefix length
  public int minCPL;
  // Closest peers that are within the target region
  protected HashMap<BigInteger, Boolean> regionalSet;
  // Store the original destNode
  public BigInteger targetNode;

  /**
   * defaul constructor
   *
   * @param destNode Id of the node to find
   * @param k Number of honest nodes
   */
  public RegionBasedFindOperation(BigInteger srcNode, BigInteger destNode, int k, long timestamp) {
    super(srcNode, destNode, timestamp);
    this.minCPL = (int) Math.ceil(Math.log(Network.size() / (double) k) / Math.log(2)) - 1;
    regionalSet = new HashMap<BigInteger, Boolean>();
    this.targetNode = destNode;
  }

  /**
   * update closestSet with the new information received
   *
   * @param neighbours
   */
  @Override
  public void elaborateResponse(BigInteger[] neighbours) {

    // add to closestSet
    for (BigInteger n : neighbours) {

      if (n != null && !regionalSet.containsKey(n)) {

        if (Util.prefixLen(n, this.targetNode) >= this.minCPL) {
          this.regionalSet.put(n, false);
        }
      }
    }
    super.elaborateResponse(neighbours);
  }

  /**
   * get the first neighbour in closest set which has not been already queried
   *
   * @return the Id of the node or null if there aren't available node
   */
  public BigInteger getNeighbour() {
    // find closest neighbour ( the first not already queried)
    BigInteger neighbour = super.getNeighbour();
    int curr_minCPL = 0;
    if (neighbour == null && available_requests == KademliaCommonConfig.ALPHA) {
      // should we let the find operation terminate?
      curr_minCPL = Util.getMinCplWithSet(this.targetNode, this.closestSet.keySet());
      // we found all closest peers with common prefix length >= curr_minCPL
      if (curr_minCPL <= this.minCPL) {
        return null;
      } else {
        // Update the destNode
        this.destNode = Util.flipBit(this.targetNode, curr_minCPL);
        // Form a new closestSet using regionalSet
        this.closestSet = new HashMap<BigInteger, Boolean>();
        for (BigInteger n : regionalSet.keySet()) {
          if (closestSet.size() < KademliaCommonConfig.K) { // add directly
            closestSet.put(n, false);
          } else {
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

    if (neighbour != null && regionalSet.containsKey(neighbour)) {
      regionalSet.remove(neighbour);
      regionalSet.put(neighbour, true);
    }

    return neighbour;
  }
}
