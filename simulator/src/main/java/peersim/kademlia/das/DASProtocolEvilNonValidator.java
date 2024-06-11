package peersim.kademlia.das;

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
    // super.handleInitGetSample(m, myPid);
  }

  /*protected void handleGetSample(Message m, int myPid) {
    // kv is for storing the sample you have
    logger.info("KV size " + kv.occupancy() + " from:" + m.src.getId() + " " + m.id);
    // sample IDs that are requested in the message

    Message response = new Message(Message.MSG_GET_SAMPLE_RESPONSE, new Sample[] {});
    response.operationId = m.operationId;
    response.dst = m.src;
    response.src = this.kadProtocol.getKademliaNode();
    response.ackId = m.id; // set ACK number
    response.value = searchTable.getEvilNeighbours(KademliaCommonConfigDas.MAX_NODES_RETURNED);
    sendMessage(response, m.src.getId(), myPid);
  }*/
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
