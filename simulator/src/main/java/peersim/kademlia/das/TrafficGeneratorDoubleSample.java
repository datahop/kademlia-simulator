package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.Message;
import peersim.kademlia.UniformRandomGenerator;

/**
 * This control generates samples every 5 min that are stored in a single node (builder) and starts
 * random sampling from the rest of the nodes In parallel, random lookups are started to start
 * discovering nodes
 *
 * @author Sergi Rene
 * @version 1.0
 */

// ______________________________________________________________________________________________
public class TrafficGeneratorDoubleSample implements Control {

  // ______________________________________________________________________________________________
  /** MSPastry Protocol to act */
  private static final String PAR_KADPROT = "kadprotocol";

  private static final String PAR_DASPROT = "dasprotocol";

  /** MSPastry Protocol ID to act */
  private final int kadpid, daspid;

  /** Mapping function for samples */
  final String PAR_MAP_FN = "mapping_fn";

  /** Number of sample copies stored per node */
  final String PAR_NUM_COPIES = "sample_copy_per_node";

  final String PAR_BLK_DIM_SIZE = "block_dim_size";

  int mapfn;

  Block b;
  private long ID_GENERATOR = 0;

  private boolean first = true, second = false;

  private HashMap<BigInteger, Node> nodeMap;
  // ______________________________________________________________________________________________
  public TrafficGeneratorDoubleSample(String prefix) {
    kadpid = Configuration.getPid(prefix + "." + PAR_KADPROT);
    daspid = Configuration.getPid(prefix + "." + PAR_DASPROT);

    KademliaCommonConfigDas.MAPPING_FN = Configuration.getInt(prefix + "." + PAR_MAP_FN);
    KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER =
        Configuration.getInt(prefix + "." + PAR_NUM_COPIES);
    KademliaCommonConfigDas.BLOCK_DIM_SIZE =
        Configuration.getInt(
            prefix + "." + PAR_BLK_DIM_SIZE, KademliaCommonConfigDas.BLOCK_DIM_SIZE);
    nodeMap = new HashMap<>();
  }

  // ______________________________________________________________________________________________
  /**
   * generates a GET message for t1 key.
   *
   * @return Message
   */
  private Message generateNewBlockMessage(Block b) {

    Message m = Message.makeInitNewBlock(b);
    m.timestamp = CommonState.getTime();

    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * generates a GET message for t1 key.
   *
   * @return Message
   */
  private Message generateNewSampleMessage(BigInteger s) {

    Message m = Message.makeInitGetSample(s);
    m.timestamp = CommonState.getTime();

    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * generates a random find node message, by selecting randomly the destination.
   *
   * @return Message
   */
  private Message generateFindNodeMessage() {

    UniformRandomGenerator urg =
        new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    BigInteger id = urg.generate();

    Message m = Message.makeInitFindNode(id);
    m.timestamp = CommonState.getTime();

    return m;
  }

  private List<BigInteger> getNodesbySample(BigInteger sampleId, BigInteger radius) {

    BigInteger top = sampleId.add(radius);
    BigInteger bottom = sampleId.subtract(radius);

    // System.out.println("Top:" + top);
    // System.out.println("Bottom:" + bottom);
    TreeSet<BigInteger> nodesIndexed = new TreeSet<BigInteger>(nodeMap.keySet());
    // System.out.println("Nodes:" + nodesIndexed.size());

    Collection<BigInteger> subSet = nodesIndexed.subSet(bottom, true, top, true);

    // System.out.println("Subset:" + subSet.size());

    return new ArrayList<BigInteger>(subSet);
  }

  // ______________________________________________________________________________________________
  /**
   * every call of this control generates and send a random find node message
   *
   * @return boolean
   */
  public boolean execute() {
    if (first) {
      /*  for (int i = 0; i < Network.size(); i++) {
          Node n = Network.get(i);
          if (n.isUp()) {
            for (int j = 0; j < 3; j++) {
              int time = CommonState.r.nextInt(300000);
              Node start = Network.get(i);
              Message lookup = generateFindNodeMessage();
              EDSimulator.add(time, lookup, start, kadpid);
            }
          }
        }
      } else {
        for (int i = 0; i < Network.size(); i++) {
          Node n = Network.get(i);
          if (n.isUp()) {
            int time = CommonState.r.nextInt(300000);
            Node start = Network.get(i);
            Message lookup = generateFindNodeMessage();
            EDSimulator.add(time, lookup, start, kadpid);
          }
        }*/

      for (int i = 0; i < Network.size(); i++) {
        DASProtocol dasProt = ((DASProtocol) (Network.get(i).getProtocol(daspid)));
        nodeMap.put(dasProt.getKademliaId(), Network.get(i));
      }
      first = false;
      second = true;
      // } else if (second) {
    } else {
      Block b = new Block(KademliaCommonConfigDas.BLOCK_DIM_SIZE, ID_GENERATOR);
      int samplesWithinRegion = 0; // samples that are within at least one node's region
      int totalSamples = 0;
      while (b.hasNext()) {
        Sample s = b.next();
        boolean inRegion = false;
        // System.out.println("New sample " + s.getColumn() + " " + s.getRow());
        int count = 0;
        BigInteger radius =
            b.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);

        while (!inRegion) {
          for (BigInteger nodeId : getNodesbySample(s.getIdByRow(), radius)) {
            Node n = nodeMap.get(nodeId);
            DASProtocol dasProt = ((DASProtocol) (n.getProtocol(daspid)));
            if (n.isUp() && !dasProt.isBuilder()) {
              totalSamples++;
              // System.out.println("Assigned row " + s.getIdByRow());
              EDSimulator.add(0, generateNewSampleMessage(s.getIdByRow()), n, daspid);

              if (inRegion == false) {
                samplesWithinRegion++;
                inRegion = true;
              }
            }
          }
          for (BigInteger nodeId : getNodesbySample(s.getIdByColumn(), radius)) {
            Node n = nodeMap.get(nodeId);
            DASProtocol dasProt = ((DASProtocol) (n.getProtocol(daspid)));
            if (n.isUp() && !dasProt.isBuilder()) {
              totalSamples++;
              EDSimulator.add(0, generateNewSampleMessage(s.getIdByColumn()), n, daspid);

              if (inRegion == false) {
                samplesWithinRegion++;
                inRegion = true;
              }
            }
          }
          if (!inRegion) {
            radius = radius.multiply(BigInteger.valueOf(2));
            count++;
            // System.out.println("Sample assigned after " + count + " increase " + s.getIdByRow());
          }
        }
        /*if (!inRegion) {
          System.out.println("Error sample not assgined " + s.getId());
          BigInteger closestByRow = BigInteger.valueOf(0);
          BigInteger closestByColumn = BigInteger.valueOf(0);
          int minDistByRow = KademliaCommonConfig.BITS, minDistByColumn = KademliaCommonConfig.BITS;
          for (BigInteger node : nodeMap.keySet()) {
            int dist = Util.logDistance(node, s.getIdByRow());
            if (dist < minDistByRow) closestByRow = node;
            if (dist < minDistByColumn) closestByColumn = node;
          }
          if (closestByRow.compareTo(BigInteger.valueOf(0)) != 0)
            EDSimulator.add(
                0, generateNewSampleMessage(s.getIdByRow()), nodeMap.get(closestByRow), daspid);
          if (closestByColumn.compareTo(BigInteger.valueOf(0)) != 0)
            EDSimulator.add(
                0,
                generateNewSampleMessage(s.getIdByColumn()),
                nodeMap.get(closestByColumn),
                daspid);
          samplesWithinRegion++;
        }
        for (int i = 0; i < Network.size(); i++) {
          Node n = Network.get(i);
          DASProtocol dasProt = ((DASProtocol) (n.getProtocol(daspid)));
          // if (dasProt.isBuilder()) EDSimulator.add(0, generateNewBlockMessage(s), n, daspid);
          // else
          if (n.isUp()
              && (s.isInRegionByRow(dasProt.getKademliaId(), radius)
                  || s.isInRegionByColumn(dasProt.getKademliaId(), radius))) {
            totalSamples++;
            if (s.isInRegionByRow(dasProt.getKademliaId(), radius)) {
              EDSimulator.add(0, generateNewSampleMessage(s.getIdByRow()), n, daspid);
              // System.out.println(dasProt.getKademliaId() + " row:" + s.getRow());
            } else {
              EDSimulator.add(0, generateNewSampleMessage(s.getIdByColumn()), n, daspid);
              // System.out.println(dasProt.getKademliaId() + " column:+" + s.getColumn());
            }
            if (inRegion == false) {
              samplesWithinRegion++;
              inRegion = true;
            }
          }
        }*/
      }

      for (int i = 0; i < Network.size(); i++) {
        Node n = Network.get(i);
        b.initIterator();
        EDSimulator.add(0, generateNewBlockMessage(b), n, daspid);
      }

      System.out.println(
          ""
              + samplesWithinRegion
              + " samples out of "
              + b.getNumSamples()
              + " samples are within a node's region");

      System.out.println("" + totalSamples + " total samples distributed");
      if (samplesWithinRegion != b.getNumSamples()) {
        System.out.println(
            "Error: there are "
                + (b.getNumSamples() - samplesWithinRegion)
                + " samples that are not within a region of a peer ");
        // System.exit(1);
      }
      // }
      ID_GENERATOR++;
      second = false;
    }
    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
