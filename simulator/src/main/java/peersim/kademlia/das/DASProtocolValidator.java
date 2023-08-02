package peersim.kademlia.das;

import java.math.BigInteger;
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
  protected void handleSeedSample(Message m, int myPid) {
    logger.warning("seed sample receveived");
    if (m.body == null) return;

    Sample s = (Sample) m.body;
    logger.warning("Received sample:" + kv.occupancy() + " " + s.getRow() + " " + s.getColumn());
    // just to check whether we started actual sampling
    // we increase the counter when asking the builder and decrease when receiving the sample from
    // the builder
    // when reaches 0 - we start the actual sampling (potential conflicts with the init sampling
    // message)

    kv.add((BigInteger) s.getIdByRow(), s);
    kv.add((BigInteger) s.getIdByColumn(), s);
    // count # of samples for each row and column
    column[s.getColumn()]++;
    row[s.getRow()]++;
  }

  @Override
  protected void handleGetSample(Message m, int myPid) {
    /** Ignore sample request * */
    logger.warning("Handle get sample - return nothing " + this);
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block validator node - getting samples " + this);
    // super.handleInitGetSample(m, myPid);
  }

  @Override
  protected void handleGetSampleResponse(Message m, int myPid) {
    logger.warning("Received sample validator node: do nothing");
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
