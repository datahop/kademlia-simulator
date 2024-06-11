package peersim.kademlia.das;

import peersim.core.CommonState;
import peersim.kademlia.Message;

public class DASProtocolEvilValidator extends DASProtocolValidator {

  protected static String prefix = null;
  // TODO Read the attackTime from the config - for now set it to the end of the experiment
  long attackTime = CommonState.getEndTime();

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
    // super.handleInitGetSample(m, myPid);
  }

  protected void handleGetSample(Message m, int myPid) {
    // kv is for storing the sample you have
    logger.info("KV size " + kv.occupancy() + " from:" + m.src.getId() + " " + m.id);
    // sample IDs that are requested in the message

    if (CommonState.getTime() < attackTime) {
      super.handleGetSample(m, myPid);

    } else {
      return; // withhold by not responding
    }

    /*
    Message response = new Message(Message.MSG_GET_SAMPLE_RESPONSE, new Sample[] {});
    response.operationId = m.operationId;
    response.dst = m.src;
    response.src = this.kadProtocol.getKademliaNode();
    response.ackId = m.id; // set ACK number
    response.value = searchTable.getEvilNeighbours(KademliaCommonConfigDas.MAX_NODES_RETURNED);
    sendMessage(response, m.src.getId(), myPid);*/
  }

  /*
  public void setEvil(List<Node> evilIds) {
    searchTable.setEvil(evilIds);
  }*/

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
