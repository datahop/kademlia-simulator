package peersim.kademlia.das;

import peersim.core.CommonState;
import peersim.kademlia.Message;

public class DASProtocolEvilNonValidator extends DASProtocolNonValidator {

  protected static String prefix = null;

  public DASProtocolEvilNonValidator(String prefix) {
    super(prefix);
    DASProtocolEvilNonValidator.prefix = prefix;
    isEvil = true;
    searchTable.setOnlyAddEvilNghbrs();
    isValidator = false;
    isBuilder = false;
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block evil node validator - getting samples " + this);
  }

  protected void handleGetSample(Message m, int myPid) {
    logger.warning("evil node nonvalidator - witholding samples "+m.body);

  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    DASProtocolEvilNonValidator dolly =
        new DASProtocolEvilNonValidator(DASProtocolEvilNonValidator.prefix);
    return dolly;
  }
}
