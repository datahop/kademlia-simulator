package peersim.kademlia.das;

import peersim.kademlia.KademliaProtocol;
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
  /**
   * sets the kademliaprotocol instance can be used to run kad operations
   *
   * @param prot KademliaProtocol
   */
  protected void setKademliaProtocol(KademliaProtocol prot) {
    this.kadProtocol = prot;
    this.logger = prot.getLogger();
  }
}
