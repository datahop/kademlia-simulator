package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.List;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.Message;
import peersim.kademlia.Util;

// DAS Protocol process functions executed only by builder. It basically seeds validators every
// block.
public class DASProtocolBuilder extends DASProtocol {

  protected static String prefix = null;

  public DASProtocolBuilder(String prefix) {
    super(prefix);
    DASProtocolBuilder.prefix = prefix;
    isBuilder = true;
    isValidator = false;
  }

  @Override
  protected void handleGetSample(Message m, int myPid) {
    /** Ignore sample request * */
    logger.warning("Builder handle get sample - return nothing " + this);
  }

  @Override
  protected void handleSeedSample(Message m, int myPid) {
    System.err.println("Builder should not receive seed sample");
    System.exit(-1);
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
    logger.warning("Builder new block:" + currentBlock.getBlockId());

    int samplesWithinRegion = 0; // samples that are within at least one node's region
    int samplesValidators = 0;
    int samplesNonValidators = 0;

    BigInteger radiusNonValidator =
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);

    while (currentBlock.hasNext()) {
      boolean inRegion = false;
      Sample s = currentBlock.next();

      BigInteger radiusValidator =
          currentBlock.computeRegionRadius(
              KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER,
              searchTable.getValidatorsIndexed().size());

      while (!inRegion) {

        List<BigInteger> idsValidators =
            searchTable.getValidatorNodesbySample(s.getIdByRow(), radiusValidator);

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
            Sample[] samples = {s};
            Message msg = generateSeedSampleMessage(samples);
            msg.operationId = -1;
            msg.src = this.getKademliaProtocol().getKademliaNode();
            msg.dst = n.getKademliaProtocol().getKademliaNode();
            sendMessage(msg, id, dasProt.getDASProtocolID());
            samplesValidators++;
            if (inRegion == false) {
              samplesWithinRegion++;
              inRegion = true;
            }
          }
        }
        if (!inRegion) radiusValidator = radiusValidator.multiply(BigInteger.valueOf(2));
      }
      inRegion = false;
      while (!inRegion) {

        List<BigInteger> idsValidators =
            searchTable.getValidatorNodesbySample(s.getIdByColumn(), radiusValidator);

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
            Sample[] samples = {s};
            Message msg = generateSeedSampleMessage(samples);
            msg.operationId = -1;
            msg.src = this.getKademliaProtocol().getKademliaNode();
            msg.dst = n.getKademliaProtocol().getKademliaNode();
            sendMessage(msg, id, dasProt.getDASProtocolID());
            samplesValidators++;
            if (inRegion == false) {
              samplesWithinRegion++;
              inRegion = true;
            }
          }
        }
        if (!inRegion) radiusValidator = radiusValidator.multiply(BigInteger.valueOf(2));
      }

      List<BigInteger> idsNonValidators =
          searchTable.getNonValidatorNodesbySample(s.getIdByRow(), radiusNonValidator);
      idsNonValidators.addAll(
          searchTable.getNonValidatorNodesbySample(s.getIdByColumn(), radiusNonValidator));
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
          samplesNonValidators++;

          if (!dasProt.isValidator()) {
            EDSimulator.add(2, generateNewSampleMessage(s.getId()), n, dasProt.getDASProtocolID());
          }
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
