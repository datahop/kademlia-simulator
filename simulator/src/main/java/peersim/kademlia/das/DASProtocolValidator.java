package peersim.kademlia.das;

import peersim.kademlia.Message;

public class DASProtocolValidator extends DASProtocol {

  protected static String prefix = null;

  public DASProtocolValidator(String prefix) {
    super(prefix);
    DASProtocolValidator.prefix = prefix;
    isValidator = true;
    isBuilder = false;
  }

  @Override
  protected void handleGetSample(Message m, int myPid) {
    /** Ignore sample request * */
    logger.warning("Handle get sample - return nothing " + this);
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    logger.warning("Init block validator node - do nothing");
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
    DASProtocolValidator dolly = new DASProtocolValidator(DASProtocolValidator.prefix);
    return dolly;
  }
}
