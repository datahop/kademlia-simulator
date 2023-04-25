package peersim.kademlia.das;

import peersim.kademlia.Message;

public class EvilDASProtocol extends DASProtocol {

  protected static String prefix = null;

  public EvilDASProtocol(String prefix) {
    super(prefix);
    EvilDASProtocol.prefix = prefix;
  }

  @Override
  protected void handleGetSample(Message m, int myPid) {
    /** Ignore sample request * */
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    EvilDASProtocol dolly = new EvilDASProtocol(EvilDASProtocol.prefix);
    return dolly;
  }
}
