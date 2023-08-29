package peersim.gossipsub;

import java.util.ArrayList;
import java.util.List;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.operations.SamplingOperation;
import peersim.kademlia.das.operations.ValidatorSamplingOperation;

public class GossipSubDasValidator extends GossipSubDas {

  public GossipSubDasValidator(String prefix) {
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
    GossipSubDasValidator dolly = new GossipSubDasValidator(GossipSubDasValidator.prefix);
    return dolly;
  }

  protected void startValidatorSampling(int row, int column, String topic, int myPid) {
    logger.warning("Sampling operation started validator " + row + " " + column);

    ValidatorSamplingOperation op =
        new ValidatorSamplingOperation(
            this.getGossipNode().getId(),
            CommonState.getTime(),
            currentBlock,
            null,
            row,
            column,
            true,
            null);
    samplingOp.put(op.getId(), op);
    List<String> topics = new ArrayList<>();
    topics.add(topic);
    samplingTopics.put(op.getId(), topics);
  }
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
      if (topics != null) {
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
    }
    super.handleMessage(m, myPid);
  }

  protected void handleInitNewBlock(Message m, int myPid) {
    currentBlock = (Block) m.body;
    logger.warning("Validator Init block");
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
      logger.warning("Select rows/columns");
      started = true;
      row1 = CommonState.r.nextInt(currentBlock.getSize()) + 1;
      row2 = CommonState.r.nextInt(currentBlock.getSize()) + 1;
      col1 = CommonState.r.nextInt(currentBlock.getSize()) + 1;
      col2 = CommonState.r.nextInt(currentBlock.getSize()) + 1;
      EDSimulator.add(0, Message.makeInitJoinMessage("Row" + row1), getNode(), myPid);
      EDSimulator.add(0, Message.makeInitJoinMessage("Row" + row2), getNode(), myPid);
      EDSimulator.add(0, Message.makeInitJoinMessage("Column" + col1), getNode(), myPid);
      EDSimulator.add(0, Message.makeInitJoinMessage("Column" + col2), getNode(), myPid);
      for (int i = 1; i < Network.size(); i++) {
        Node n = Network.get(i);
        GossipSubProtocol prot = (GossipSubProtocol) n.getProtocol(myPid);
        prot.getTable().addPeer("Row" + row1, this.getGossipNode().getId());
        prot.getTable().addPeer("Row" + row2, this.getGossipNode().getId());
        prot.getTable().addPeer("Column" + col1, this.getGossipNode().getId());
        prot.getTable().addPeer("Column" + col2, this.getGossipNode().getId());
      }
    } else {

      // if (isValidator) {
      startValidatorSampling(row1, 0, "Row" + row1, myPid);
      startValidatorSampling(row2, 0, "Row" + row2, myPid);
      startValidatorSampling(0, col1, "Column" + col1, myPid);
      startValidatorSampling(0, col2, "Column" + col2, myPid);
      // }
      startRandomSampling();
    }
  }
}
