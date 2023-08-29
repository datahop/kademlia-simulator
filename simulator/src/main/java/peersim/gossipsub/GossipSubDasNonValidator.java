package peersim.gossipsub;

import java.util.List;
import peersim.core.CommonState;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.operations.SamplingOperation;

public class GossipSubDasNonValidator extends GossipSubDas {

  public GossipSubDasNonValidator(String prefix) {
    super(prefix);
    // TODO Auto-generated constructor stub
  }

  /**
   * Replicate this object by returning an identical copy. It is called by the initializer and do
   * not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    GossipSubDasNonValidator dolly = new GossipSubDasNonValidator(GossipSubDasNonValidator.prefix);
    return dolly;
  }

  protected void startRandomSampling(int myPid) {}
  ;

  protected void handleMessage(Message m, int myPid) {

    Sample s = (Sample) m.value;

    logger.warning(
        "dasrows handleMessage received "
            + m.body
            + " "
            + s.getId()
            + " "
            + m.id
            + " "
            + m.src.getId());

    Sample[] samples = new Sample[] {s};

    String topic = (String) m.body;

    logger.warning(
        "Validator received message sample " + s.getRow() + " " + s.getColumn() + " " + topic);

    for (SamplingOperation sop : samplingOp.values()) {
      List<String> topics = samplingTopics.get(sop.getId());
      if (topics.contains(topic)) {
        sop.addMessage(m.id);
        sop.elaborateResponse(samples);
        sop.increaseHops();
        if (sop.completed()) {
          sop.setStopTime(CommonState.getTime() - sop.getTimestamp());
        }
        logger.warning(
            "Sop "
                + sop.getSamples().length
                + " "
                + topic
                + " "
                + sop.getHops()
                + " "
                + sop.getStopTime());
      }
    }
    super.handleMessage(m, myPid);
  }

  protected void handleInitNewBlock(Message m, int myPid) {
    currentBlock = (Block) m.body;
    logger.warning("non-validator Init block");
    for (SamplingOperation sop : samplingOp.values()) {
      logger.warning("Reporting operation " + sop.getId());
      GossipObserver.reportOperation(sop);
      samplingTopics.remove(sop.getId());
      /*if (sop instanceof RandomSamplingOperation) {
        List<String> topics = samplingTopics.get(sop.getId());
        for (String topic : topics) {
          EDSimulator.add(0, Message.makeLeaveMessage(topic), getNode(), myPid);
        }
      }*/
    }
    samplingOp.clear();
    if (!started) {
      started = true;
    } else startRandomSampling(myPid);
  }
}
