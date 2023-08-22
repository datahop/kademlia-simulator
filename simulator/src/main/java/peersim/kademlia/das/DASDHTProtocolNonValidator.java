package peersim.kademlia.das;

import peersim.kademlia.Message;

public class DASDHTProtocolNonValidator extends DASDHTProtocolValidator {

  protected static String prefix = null;

  public DASDHTProtocolNonValidator(String prefix) {
    super(prefix);
    DASDHTProtocolNonValidator.prefix = prefix;
    isValidator = false;
    isBuilder = false;
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
    startRandomSampling();
  }
}
