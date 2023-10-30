package peersim.kademlia.das;

import java.util.List;
import peersim.core.Node;
import peersim.kademlia.Message;

public class DASProtocolEvilValidator extends DASProtocolValidator {

  protected static String prefix = null;

  public DASProtocolEvilValidator(String prefix) {
    super(prefix);
    DASProtocolEvilValidator.prefix = prefix;
    isEvil = true;
    isValidator = true;
    isBuilder = false;
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block evil node validator - getting samples " + this);
    // super.handleInitGetSample(m, myPid);
  }

  protected void handleGetSample(Message m, int myPid) {
    // kv is for storing the sample you have
    logger.info("KV size " + kv.occupancy() + " from:" + m.src.getId() + " " + m.id);
    // sample IDs that are requested in the message

    Message response = new Message(Message.MSG_GET_SAMPLE_RESPONSE, new Sample[] {});
    response.operationId = m.operationId;
    response.dst = m.src;
    response.src = this.kadProtocol.getKademliaNode();
    response.ackId = m.id; // set ACK number
    response.value = searchTable.getEvilNeighbours();

    sendMessage(response, m.src.getId(), myPid);
  }

  public void setEvilIds(List<Node> evilIds) {
    searchTable.setEvilIds(evilIds);
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
