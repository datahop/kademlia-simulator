package peersim.kademlia.das;

import peersim.kademlia.Message;

public class DASDHTProtocolNonValidator extends DASProtocol {

  protected static String prefix = null;

  public DASDHTProtocolNonValidator(String prefix) {
    super(prefix);
    DASDHTProtocolNonValidator.prefix = prefix;
  }

  public Object clone() {
    DASDHTProtocolNonValidator dolly =
        new DASDHTProtocolNonValidator(DASDHTProtocolNonValidator.prefix);
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
    logger.warning("non-validator new block:" + currentBlock.getBlockId());
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block non-validato node - getting samples " + this);
    // super.handleInitGetSample(m, myPid);
  }

  @Override
  protected void handleGetSampleResponse(Message m, int myPid) {
    logger.warning("non-validato Received sample : do nothing");
  }

  @Override
  protected void handleGetSample(Message m, int myPid) {
    /** Ignore sample request * */
    logger.warning("non-validator handle get sample - return nothing " + this);
  }

  @Override
  protected void handleSeedSample(Message m, int myPid) {
    System.err.println("non-validator should not receive seed sample");
    System.exit(-1);
  }
}
