package peersim.kademlia.das;

import peersim.kademlia.Message;

public class DASProtocolEvilValidator extends DASProtocolValidator {

  protected static String prefix = null;

  public DASProtocolEvilValidator(String prefix) {
    super(prefix);
    DASProtocolEvilValidator.prefix = prefix;
    isEvil = true;
    searchTable.setOnlyAddEvilNghbrs();
    isValidator = true;
    isBuilder = false;
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block evil node validator - getting samples " + this);
  }

  protected void handleGetSample(Message m, int myPid) {
    logger.warning("evil node validator - witholding samples " + m.body);
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    DASProtocolEvilValidator dolly = new DASProtocolEvilValidator(DASProtocolEvilValidator.prefix);
    return dolly;
  }
}
