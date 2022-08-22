package peersim.kademlia.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
// import java.util.Random;
import peersim.core.CommonState;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.KademliaObserver;
import peersim.kademlia.SearchTable;
import peersim.kademlia.Topic;

public class LookupTicketOperation extends LookupOperation {

  SearchTable sTable;
  int lastAskedBucket;
  private boolean completed;

  private List<BigInteger> neighboursList;

  public LookupTicketOperation(BigInteger srcNode, SearchTable sTable, Long timestamp, Topic t) {
    super(srcNode, timestamp, t);
    this.sTable = sTable;
    lastAskedBucket = KademliaCommonConfig.BITS;
    completed = false;
  }

  private ArrayList<BigInteger> getRandomBucketNeighbours() {
    ArrayList<BigInteger> neighbours = new ArrayList<BigInteger>();
    int tries = 0;
    while ((neighbours.size() == 0) && (tries < sTable.getnBuckets())) {
      int distance = KademliaCommonConfig.BITS - CommonState.r.nextInt(sTable.getnBuckets());
      tries++;
      Collections.addAll(neighbours, sTable.getNeighbours(distance));
    }
    return neighbours;
  }

  private ArrayList<BigInteger> getMinBucketNeighbours() {
    ArrayList<BigInteger> neighbours = new ArrayList<BigInteger>();
    for (int dist = sTable.getbucketMinDistance(); dist <= KademliaCommonConfig.BITS; dist++) {
      Collections.addAll(neighbours, sTable.getNeighbours(dist));
      if (neighbours.size() != 0) break;
    }
    return neighbours;
  }

  private ArrayList<BigInteger> getAllBucketNeighbours() {
    ArrayList<BigInteger> neighbours = new ArrayList<BigInteger>();
    int tries = 0;
    for (; tries < sTable.getnBuckets(); lastAskedBucket--, tries++) {
      if (neighbours.size() != 0) break;
      if (lastAskedBucket < (KademliaCommonConfig.BITS - sTable.getnBuckets()))
        lastAskedBucket = KademliaCommonConfig.BITS;

      Collections.addAll(neighbours, sTable.getNeighbours(lastAskedBucket));
    }
    return neighbours;
  }

  private ArrayList<BigInteger> getCompleteRandomWalkNeighbours() {
    ArrayList<BigInteger> neighbours = new ArrayList<BigInteger>();

    int tries = 0;

    for (; tries < sTable.getnBuckets(); lastAskedBucket--, tries++) {

      Collections.addAll(neighbours, sTable.getNeighbours(lastAskedBucket));
    }
    return neighbours;
  }

  public boolean completed() {
    if (KademliaCommonConfig.LOOKUP_BUCKET_ORDER != KademliaCommonConfig.COMPLETE_WALK) {
      int all = KademliaObserver.topicRegistrationCount(topic.getTopic());
      // int required = Math.min(all, KademliaCommonConfig.TOPIC_PEER_LIMIT);
      int required = KademliaCommonConfig.TOPIC_PEER_LIMIT;
      return discoveredCount() >= required;
    } else {
      return completed;
    }
  }

  public BigInteger getNeighbour() {
    BigInteger res = null;
    ArrayList<BigInteger> neighbours = null;

    switch (KademliaCommonConfig.LOOKUP_BUCKET_ORDER) {
      case KademliaCommonConfig.RANDOM_BUCKET_ORDER:
        neighbours = getRandomBucketNeighbours();
        break;
      case KademliaCommonConfig.CLOSEST_BUCKET_ORDER:
        neighbours = getMinBucketNeighbours();
        break;
      case KademliaCommonConfig.ALL_BUCKET_ORDER:
        neighbours = getAllBucketNeighbours();
        break;
      case KademliaCommonConfig.COMPLETE_WALK:
        if (neighboursList == null) neighboursList = getCompleteRandomWalkNeighbours();
        break;
    }

    if (KademliaCommonConfig.LOOKUP_BUCKET_ORDER != KademliaCommonConfig.COMPLETE_WALK) {
      if (neighbours.size() != 0) {
        res = neighbours.get(CommonState.r.nextInt(neighbours.size()));

        // We should never get the same neighbour twice
        assert !this.used.contains(res);
      }

      if (res != null) {
        sTable.removeNeighbour(res);
        available_requests--;
      } else {
        System.out.println("Returning null");
      }

      return res;
    } else {
      if (neighboursList.size() > 0) {
        res = neighboursList.get(0);
        neighboursList.remove(res);
        System.out.println("Getneithbour return " + res + " " + neighboursList.size());
        available_requests--;

        if (neighboursList.size() == 0) completed = true;
        return res;
      } else return null;
    }
  }
}
