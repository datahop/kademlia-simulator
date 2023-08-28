package peersim.kademlia.das;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.gossipsub.GossipSubProtocol;
import peersim.gossipsub.Message;
import peersim.kademlia.SimpleEvent;
import peersim.kademlia.das.operations.RandomSamplingOperation;
import peersim.kademlia.das.operations.SamplingOperation;

public abstract class GossipSubDas extends GossipSubProtocol {

  protected LinkedHashMap<Long, SamplingOperation> samplingOp;

  protected HashMap<Long, List<String>> samplingTopics;

  protected Block currentBlock;

  protected boolean isValidator;

  public GossipSubDas(String prefix) {
    super(prefix);
    samplingOp = new LinkedHashMap<>();
    samplingTopics = new HashMap<>();
    // TODO Auto-generated constructor stub
    isValidator = false;
  }
  /**
   * Replicate this object by returning an identical copy. It is called by the initializer and do
   * not fill any particular field.
   *
   * @return Object
   */
  public abstract Object clone();

  protected void startValidatorSampling(int row, int column, String topic, int myPid) {
    logger.warning("Sampling operation started validator " + row + " " + column);

    /*ValidatorSamplingOperation op =
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
    EDSimulator.add(0, Message.makeInitJoinMessage(topic), getNode(), myPid);*/
  }

  protected void startRandomSampling(int myPid) {
    // logger.warning("Starting random sampling");
    /*RandomSamplingOperation op =
        new RandomSamplingOperation(
            this.getGossipNode().getId(),
            null,
            CommonState.getTime(),
            currentBlock,
            null,
            this.isValidator,
            null);
    // op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started random");

    Sample[] samples = op.getRandomSamples();
    List<String> topics = new ArrayList<>();
    for (Sample s : samples) {
      EDSimulator.add(0, Message.makeInitJoinMessage("Row" + s.getRow()), getNode(), myPid);
      EDSimulator.add(0, Message.makeInitJoinMessage("Column" + s.getColumn()), getNode(), myPid);
      topics.add("Row" + s.getRow());
      topics.add("Column" + s.getColumn());
    }
    samplingTopics.put(op.getId(), topics);*/
  }
  /**
   * Start a topic query opearation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected abstract void handleInitNewBlock(Message m, int myPid);

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

    logger.warning("Received message sample " + s.getRow() + " " + s.getColumn() + " " + topic);

    for (SamplingOperation sop : samplingOp.values()) {
      List<String> topics = samplingTopics.get(sop.getId());
      if (sop instanceof RandomSamplingOperation)
        logger.warning(
            "Random sampling operation sample received " + topic + " " + sop.samplesCount());
      if (topics.contains(topic)) {
        sop.addMessage(m.id);
        sop.elaborateResponse(samples);
        sop.increaseHops();
        if (sop.completed()) {
          sop.setStopTime(CommonState.getTime() - sop.getTimestamp());
        }
        logger.warning("Sop " + sop.getSamples().length + " " + topic + " " + sop.getHops());
      }
    }
    super.handleMessage(m, myPid);
  }

  @Override
  public void processEvent(Node node, int pid, Object event) {
    // Set the Kademlia ID as the current process ID - assuming Pid stands for process ID.
    this.gossipid = pid;
    Message m;

    // If the event is a message, report the message to the Kademlia observer.
    if (event instanceof Message) {
      m = (Message) event;
      // KademliaObserver.reportMsg(m, false);
    }

    // Handle the event based on its type.
    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_INIT_NEW_BLOCK:
        m = (Message) event;
        handleInitNewBlock(m, pid);
        break;
      case Message.MSG_MESSAGE:
        m = (Message) event;
        handleMessage(m, pid);
        break;
      default:
        super.processEvent(node, pid, event);
        break;
    }
  }

  public void setValidator(boolean isValidator) {
    this.isValidator = isValidator;
  }
}
