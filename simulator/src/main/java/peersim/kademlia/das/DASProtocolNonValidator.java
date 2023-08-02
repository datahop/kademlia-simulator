package peersim.kademlia.das;

import peersim.kademlia.Message;

public class DASProtocolNonValidator extends DASProtocol {

  protected static String prefix = null;

  public DASProtocolNonValidator(String prefix) {
    super(prefix);
    DASProtocolNonValidator.prefix = prefix;
    isValidator = false;
    isBuilder = false;
  }

  @Override
  protected void handleGetSample(Message m, int myPid) {
    /** Ignore sample request * */
    logger.warning("Handle get sample - return nothing " + this);
  }

  @Override
  protected void handleSeedSample(Message m, int myPid) {
    System.err.println("Non-validator should not receive seed sample");
    System.exit(-1);
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block non-validator node - getting samples " + this);
    // super.handleInitGetSample(m, myPid);
  }

  @Override
  protected void handleGetSampleResponse(Message m, int myPid) {
    logger.warning("Received sample non-validator node: do nothing");
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
    // logger.warning("Starting random sampling");
    // startRandomSampling();
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
    DASProtocolNonValidator dolly = new DASProtocolNonValidator(DASProtocolNonValidator.prefix);
    return dolly;
  }
}
