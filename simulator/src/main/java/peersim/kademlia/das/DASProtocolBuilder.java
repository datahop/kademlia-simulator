package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.Message;
import peersim.kademlia.Util;

public class DASProtocolBuilder extends DASProtocol {

  protected static String prefix = null;

  public DASProtocolBuilder(String prefix) {
    super(prefix);
    DASProtocolBuilder.prefix = prefix;
    isBuilder = true;
    isValidator = false;
  }

  /*@Override
  protected void handleGetSample(Message m, int myPid) {
    logger.warning("Builder handle get sample - return nothing " + this);
  }*/

  @Override
  protected void handleSeedSample(Message m, int myPid) {
    System.err.println("Builder should not receive seed sample");
    System.exit(-1);
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
    logger.warning("Builder new block:" + currentBlock.getBlockId());
    /*+ " "
    + validatorsList.length
    + " "
    + nonValidatorsIndexed.size());*/

    int samplesWithinRegion = 0; // samples that are within at least one node's region
    int samplesValidators = 0;
    int samplesNonValidators = 0;

    while (currentBlock.hasNext()) {
      Sample s = currentBlock.next();
      kv.add(s.getId(), s);
      kv.add(s.getIdByColumn(), s);
      boolean inRegion = false;
      BigInteger radiusValidator =
          currentBlock.computeRegionRadius(
              KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER,
              searchTable.getValidatorsIndexed().size());
      BigInteger radiusNonValidator =
          currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);

      // List<BigInteger> idsValidatorsRows = SearchTable.getNodesBySample(s.getIdByRow());
      List<BigInteger> idsValidatorsRows =
          searchTable.getValidatorNodesbySample(s.getIdByRow(), radiusValidator);
      List<BigInteger> idsValidatorsColumns =
          searchTable.getValidatorNodesbySample(s.getIdByColumn(), radiusValidator);
      // SearchTable.getNodesBySample(s.getIdByColumn());

      List<BigInteger> idsNonValidatorsRows =
          searchTable.getNonValidatorNodesbySample(s.getIdByRow(), radiusNonValidator);
      List<BigInteger> idsNonValidatorsColumns =
          searchTable.getNonValidatorNodesbySample(s.getIdByColumn(), radiusNonValidator);

      List<BigInteger> idsValidators = new ArrayList<>();
      idsValidators.addAll(idsValidatorsRows);
      idsValidators.addAll(idsValidatorsColumns);

      List<BigInteger> idsNonValidators = new ArrayList<>();
      idsNonValidators.addAll(idsNonValidatorsRows);
      idsNonValidators.addAll(idsNonValidatorsColumns);

      /*  + " "
      + +idsNonValidators.size());*/

      for (BigInteger id : idsValidators) {

        logger.warning(
            "Sending sample to validator "
                + s.getIdByRow()
                + " "
                + s.getIdByColumn()
                + " to "
                + id);
        Node n = Util.nodeIdtoNode(id, kademliaId);
        DASProtocol dasProt = ((DASProtocol) (n.getDASProtocol()));
        if (dasProt.isBuilder()) continue;
        if (n.isUp()) {
          samplesValidators++;
          if (inRegion == false) {
            samplesWithinRegion++;
            inRegion = true;
          }
          EDSimulator.add(1, generateNewSampleMessage(s.getId()), n, dasProt.getDASProtocolID());
        }
      }

      for (BigInteger id : idsNonValidators) {
        logger.warning(
            "Sending sample to non-validator "
                + s.getIdByRow()
                + " "
                + s.getIdByColumn()
                + " to "
                + id);
        Node n = Util.nodeIdtoNode(id, kademliaId);
        DASProtocol dasProt = ((DASProtocol) (n.getDASProtocol()));
        if (dasProt.isBuilder()) continue;
        if (n.isUp()) {
          /*if (inRegion == false) {
            samplesWithinRegion++;
            inRegion = true;
          }*/
          samplesNonValidators++;
          EDSimulator.add(2, generateNewSampleMessage(s.getId()), n, dasProt.getDASProtocolID());
        }
      }
    }

    logger.warning(
        samplesWithinRegion
            + " samples out of "
            + currentBlock.getNumSamples()
            + " samples are within a node's region"
            + " "
            + samplesValidators
            + " "
            + samplesNonValidators);
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Error. Init block builder node - getting samples. do nothing " + this);
    // super.handleInitGetSample(m, myPid);
  }

  @Override
  protected void handleGetSampleResponse(Message m, int myPid) {
    logger.warning("Received sample builder node: do nothing");
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    DASProtocolBuilder dolly = new DASProtocolBuilder(DASProtocolBuilder.prefix);
    return dolly;
  }
}
