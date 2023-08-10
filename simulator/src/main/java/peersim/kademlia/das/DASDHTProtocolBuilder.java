package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.kademlia.Message;
import peersim.kademlia.Util;

public class DASDHTProtocolBuilder extends DASProtocol {

  protected static String prefix = null;

  public DASDHTProtocolBuilder(String prefix) {
    super(prefix);
    DASDHTProtocolBuilder.prefix = prefix;
  }

  public Object clone() {
    DASDHTProtocolBuilder dolly = new DASDHTProtocolBuilder(DASDHTProtocolBuilder.prefix);
    return dolly;
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
    logger.warning("DHT builder new block:" + currentBlock.getBlockId());

    int samplesWithinRegion = 0; // samples that are within at least one node's region
    int samplesValidators = 0;
    int samplesNonValidators = 0;
    while (currentBlock.hasNext()) {
      Sample s = currentBlock.next();
      boolean inRegion = false;
      BigInteger radiusValidator =
          currentBlock.computeRegionRadius(
              KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER, validatorsList.length);

      while (!inRegion) {
        List<BigInteger> idsValidatorsRows =
            searchTable.getValidatorNodesbySample(s.getIdByRow(), radiusValidator);

        List<BigInteger> idsValidators = new ArrayList<>();
        idsValidators.addAll(idsValidatorsRows);

        logger.warning(
            "New sample " + s.getRow() + " " + s.getColumn() + " " + idsValidators.size());
        /*  + " "
        + +idsNonValidators.size());*/

        for (BigInteger id : idsValidators) {
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

        if (!inRegion) {
          radiusValidator = radiusValidator.multiply(BigInteger.valueOf(2));
          // radiusNonValidator = radiusNonValidator.multiply(BigInteger.valueOf(2));
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
    logger.warning("Init block evil node - getting samples " + this);
    // super.handleInitGetSample(m, myPid);
  }

  @Override
  protected void handleGetSampleResponse(Message m, int myPid) {
    logger.warning("Received sample builder node: do nothing");
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

  // ______________________________________________________________________________________________
  /**
   * Generates a PUT message for t1 key and string message
   *
   * @return Message
   */
  private Message generatePutMessageSample(Sample s) {

    // Existing active destination node
    Message m = Message.makeInitPutValue(s.getId(), s);
    m.timestamp = CommonState.getTime();

    return m;
  }
}
