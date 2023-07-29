package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
public class TrafficGeneratorSample implements Control {

  // ______________________________________________________________________________________________
  /** MSPastry Protocol to act */
  private static final String PAR_KADPROT = "kadprotocol";

  private static final String PAR_DASPROT = "dasprotocol";

  /** MSPastry Protocol ID to act */
  //  private final int kadpid, daspid;

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
  private HashSet<BigInteger> nodesMissing;
  // ______________________________________________________________________________________________
  public TrafficGeneratorSample(String prefix) {

    KademliaCommonConfigDas.MAPPING_FN = Configuration.getInt(prefix + "." + PAR_MAP_FN);
    KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER =
        Configuration.getInt(prefix + "." + PAR_NUM_COPIES);
    KademliaCommonConfigDas.BLOCK_DIM_SIZE =
        Configuration.getInt(
            prefix + "." + PAR_BLK_DIM_SIZE, KademliaCommonConfigDas.BLOCK_DIM_SIZE);
    nodeMap = new HashMap<>();
    nodesMissing = new HashSet<>();
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
    // BigInteger max = BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);

    /*if (sampleId.add(radius).compareTo(max) == 1) {
      top = max;
      System.out.println("Max value " + top);
    }*/

    BigInteger bottom;
    if (radius.compareTo(sampleId) == 1) bottom = BigInteger.valueOf(0);
    else bottom = sampleId.subtract(radius);

    System.out.println("Nodes by sample " + top + " " + bottom);
    TreeSet<BigInteger> nodesIndexed = new TreeSet<BigInteger>(nodeMap.keySet());
    Collection<BigInteger> subSet = nodesIndexed.subSet(bottom, true, top, true);

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

      for (int i = 0; i < Network.size(); i++) {
        DASProtocol dasProt =
            (DASProtocol) Network.get(i).getDASProtocol(); // (Network.get(i).getProtocol(daspid)));
        nodeMap.put(dasProt.getKademliaId(), Network.get(i));
        nodesMissing.add(dasProt.getKademliaId());
      }
      first = false;
      second = true;
    } else {
      /*Block b = new Block(KademliaCommonConfigDas.BLOCK_DIM_SIZE, ID_GENERATOR);
      int samplesWithinRegion = 0; // samples that are within at least one node's region
      int totalSamples = 0;
      while (b.hasNext()) {
        Sample s = b.next();
        boolean inRegion = false;

        BigInteger radius =
            b.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);

        while (!inRegion) {
          for (BigInteger nodeId : getNodesbySample(s.getIdByRow(), radius)) {
            Node n = nodeMap.get(nodeId);
            DASProtocol dasProt = ((DASProtocol) (n.getDASProtocol()));
            if (n.isUp() && !dasProt.isBuilder()) {
              totalSamples++;
              System.out.println("Assigned row " + s.getIdByRow() + " to:" + nodeId);
              EDSimulator.add(
                  0,
                  generateNewSampleMessage(s.getIdByRow()),
                  n,
                  n.getDASProtocol().getDASProtocolID());
              nodesMissing.remove(nodeId);
              if (inRegion == false) {
                samplesWithinRegion++;
                inRegion = true;
              }
            }
          }
          for (BigInteger nodeId : getNodesbySample(s.getIdByColumn(), radius)) {
            Node n = nodeMap.get(nodeId);
            DASProtocol dasProt = ((DASProtocol) n.getDASProtocol());
            if (n.isUp() && !dasProt.isBuilder()) {
              totalSamples++;
              EDSimulator.add(
                  0, generateNewSampleMessage(s.getIdByColumn()), n, dasProt.getDASProtocolID());
              System.out.println("Assigned column " + s.getIdByColumn() + " to:" + nodeId);
              nodesMissing.remove(nodeId);
              if (inRegion == false) {
                samplesWithinRegion++;
                inRegion = true;
              }
            }
          }
          if (!inRegion) {
            radius = radius.multiply(BigInteger.valueOf(2));
          }
        }
      }*/
      Block b = new Block(KademliaCommonConfigDas.BLOCK_DIM_SIZE, ID_GENERATOR);
      int samplesWithinRegion = 0; // samples that are within at least one node's region
      int totalSamples = 0;

      while (b.hasNext()) {
        Sample s = b.next();
        boolean inRegion = false;
        BigInteger radius =
            b.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
        System.out.println(
            "New sample "
                + s.getRow()
                + " "
                + s.getColumn()
                + " "
                + s.getIdByRow()
                + " "
                + s.getIdByColumn()
                + " "
                + radius);

        while (!inRegion) {
          for (int i = 0; i < Network.size(); i++) {
            Node n = Network.get(i);
            DASProtocol dasProt = ((DASProtocol) (n.getDASProtocol()));
            // if (dasProt.isBuilder()) EDSimulator.add(0, generateNewBlockMessage(s), n, daspid);
            // else
            if (n.isUp()
                && (s.isInRegionByRow(dasProt.getKademliaId(), radius)
                    || s.isInRegionByColumn(dasProt.getKademliaId(), radius))) {
              totalSamples++;
              EDSimulator.add(
                  1, generateNewSampleMessage(s.getIdByColumn()), n, dasProt.getDASProtocolID());
              nodesMissing.remove(dasProt.getKademliaId());
              if (inRegion == false) {
                samplesWithinRegion++;
                inRegion = true;
              }
            }
          }
          if (!inRegion) {
            System.out.println("Not in region!");
            radius = radius.multiply(BigInteger.valueOf(2));
          }
        }
      }

      for (int i = 0; i < Network.size(); i++) {
        Node n = Network.get(i);
        b.initIterator();
        EDSimulator.add(0, generateNewBlockMessage(b), n, n.getDASProtocol().getDASProtocolID());
      }
      System.out.println(
          ""
              + samplesWithinRegion
              + " samples out of "
              + b.getNumSamples()
              + " samples are within a node's region");

      System.out.println("" + totalSamples + " total samples distributed");
      for (BigInteger id : nodesMissing) {
        System.out.println("Node without samples assinged " + id);
      }
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
