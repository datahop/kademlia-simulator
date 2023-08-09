package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.core.CommonState;
import peersim.kademlia.Message;

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

  /**
   * Start a topic query opearation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
    logger.warning("Builder new block:" + currentBlock.getBlockId());

    currentBlock.initIterator();
    while (currentBlock.hasNext()) {
      Sample s = currentBlock.next();

      BigInteger[] kClosest = searchTable.findKClosestValidators(s.getId());
      logger.warning("New sample id" + s.getId() + " " + kClosest.length);
    }
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
