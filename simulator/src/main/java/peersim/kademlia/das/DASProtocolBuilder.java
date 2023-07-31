package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.Message;

public class DASProtocolBuilder extends DASProtocol {

  protected static String prefix = null;

  public DASProtocolBuilder(String prefix) {
    super(prefix);
    DASProtocolBuilder.prefix = prefix;
  }

  @Override
  protected void handleGetSample(Message m, int myPid) {
    /** Ignore sample request * */
    logger.warning("Handle get sample - return nothing " + this);
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
    logger.warning("Builder new block:" + currentBlock.getBlockId());
    while (currentBlock.hasNext()) {
      Sample s = currentBlock.next();
      kv.add(s.getIdByRow(), s);
      kv.add(s.getIdByColumn(), s);
    }
    currentBlock.initIterator();

    int samplesWithinRegion = 0; // samples that are within at least one node's region
    int totalSamples = 0;

    while (currentBlock.hasNext()) {
      Sample s = currentBlock.next();
      boolean inRegion = false;
      BigInteger radius =
          currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);
      logger.info(
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

      // while (!inRegion) {
      for (BigInteger id : validatorsList) {
        logger.info("Sample " + s.getIdByRow() + " " + s.getIdByColumn() + " " + radius + " " + id);
        Node n = this.getKademliaProtocol().nodeIdtoNode(id);
        DASProtocol dasProt = ((DASProtocol) (n.getDASProtocol()));

        if (n.isUp() && (s.isInRegionByRow(id, radius) || s.isInRegionByColumn(id, radius))) {
          totalSamples++;
          EDSimulator.add(0, generateSeedSampleMessage(s), n, dasProt.getDASProtocolID());
          if (inRegion == false) {
            samplesWithinRegion++;
            inRegion = true;
          }
        }
      }
      /*if (!inRegion) {
        System.out.println("Not in region!");
        radius = radius.multiply(BigInteger.valueOf(2));
      }*/
      // }
    }
    logger.warning(
        samplesWithinRegion
            + " samples out of "
            + currentBlock.getNumSamples()
            + " samples are within a node's region");
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block evil node - getting samples " + this);
    // super.handleInitGetSample(m, myPid);
  }

  @Override
  protected void handleGetSampleResponse(Message m, int myPid) {
    logger.warning("Received sample evil node: do nothing");
  }

  // ______________________________________________________________________________________________
  /**
   * generates a GET message for a specific sample.
   *
   * @return Message
   */
  protected Message generateSeedSampleMessage(Sample s) {

    Message m = new Message(Message.MSG_SEED_SAMPLE, s);
    m.timestamp = CommonState.getTime();

    return m;
  }
  /*public void processEvent(Node myNode, int myPid, Object event) {
    logger.warning("Process event " + myPid + " " + this);
  }*/
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
