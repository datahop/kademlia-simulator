package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import peersim.core.CommonState;
import peersim.kademlia.KademliaObserver;
import peersim.kademlia.Message;
import peersim.kademlia.das.operations.RandomSamplingOperationGossip;
import peersim.kademlia.das.operations.SamplingOperation;
import peersim.kademlia.das.operations.ValidatorSamplingOperation;
import peersim.kademlia.gossipsub.GossipSubProtocol;

public class GossipDASValidator extends GossipDAS {

  protected boolean started;
  int row, column;
  protected HashMap<BigInteger, List<Message>> missingSamples;

  public GossipDASValidator(String prefix) {
    super(prefix);
    started = false;
    isValidator = true;
    isBuilder = false;
    row = column = 0;
    missingSamples = new HashMap<>();
  }

  @Override
  public Object clone() {
    GossipDASValidator dolly = new GossipDASValidator(GossipDASValidator.prefix);
    return dolly;
  }

  protected void handleInitNewBlock(Message m, int myPid) {
    currentBlock = (Block) m.body;
    logger.warning("Validator Init block");

    if (!started) {
      started = true;
      row = CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1;
      String topic = "Row" + row;
      gossipsub.Join(topic);
      GossipSubProtocol.getTable().addPeer(topic, gossipsub.getGossipNode().getId());

      column = CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1;
      topic = "Column" + column;
      gossipsub.Join(topic);
      GossipSubProtocol.getTable().addPeer(topic, gossipsub.getGossipNode().getId());

    } else {
      createValidatorSamplingOperation(row, 0, CommonState.getTime(), null);
      createValidatorSamplingOperation(0, column, CommonState.getTime(), null);
      startRandomSampling();
    }
    super.handleInitNewBlock(m, myPid);
  }

  private void createValidatorSamplingOperation(
      int row, int column, long timestamp, List<BigInteger> validatorList) {
    ValidatorSamplingOperation op =
        new ValidatorSamplingOperation(
            this.getNodeId(),
            timestamp,
            currentBlock,
            searchTable,
            row,
            column,
            this.isValidator,
            KademliaCommonConfigDas.validatorsSize,
            validatorList,
            null);
    long id = 0;
    if (row > 0) id = (long) row;
    else id = (long) column;
    samplingOp.put(id, op);
    logger.warning("Sampling operation started validator " + op.getId());
  }

  @Override
  public void messageReceived(Message m) {
    // TODO Auto-generated method stub
    // throw new UnsupportedOperationException("Unimplemented method 'messageReceived'");
    Sample[] samples = (Sample[]) m.value;
    String topic = (String) m.body;
    for (Sample s : samples) {
      reconstruct(s);
    }
    HashMap<Message, List<Sample>> toSend = findMissingSamples(samples);

    for (Message msg : toSend.keySet()) {
      if (msg.src.getId().compareTo(m.src.getId()) == 0) continue;
      Sample[] samplesToSend = toSend.get(msg).toArray(new Sample[0]);
      /*  if (isEvil) {
        samplesToSend = new Sample[] {samplesToSend[0]};
      }*/
      Message response = new Message(Message.MSG_GET_SAMPLE_RESPONSE, samplesToSend);
      response.operationId = msg.operationId;
      response.dst = msg.src;
      response.src = this.gossipsub.getGossipNode();
      response.ackId = msg.id; // set ACK number

      for (Sample s : samplesToSend)
        logger.info("Sending sample cached " + s.getId() + " to " + msg.src.getId() + " " + msg.id);
      sendMessage(response, msg.src.getId());
    }
    toSend.clear();

    long id;
    if (topic.contains("Column")) {
      id = Long.parseLong(topic.replace("Column", ""));
    } else {
      id = Long.parseLong(topic.replace("Row", ""));
    }

    // logger.info("Sample received row:" + s.getRow() + " column:" + s.getColumn());
    if (samplingOp.get(id) != null) {
      SamplingOperation op = samplingOp.get(id);
      op.elaborateResponse(samples);
      logger.warning("Operation found:" + op.getSamples().length);
      if (op.completed()) {
        KademliaObserver.reportOperation(op);
        logger.warning("Sampling operation completed " + op.getId());
      }
    }
  }

  /**
   * Starts the random sampling operation
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  protected void startRandomSampling() {
    logger.warning("Starting random sampling");
    RandomSamplingOperationGossip op =
        new RandomSamplingOperationGossip(
            this.getNodeId(),
            null,
            CommonState.getTime(),
            currentBlock,
            true,
            KademliaCommonConfigDas.validatorsSize,
            builderAddress);
    // op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started random");
    op.createNodes();
    doSampling(op);
  }

  protected void doSampling(SamplingOperation sop) {
    if (sop.completed()) {
      samplingOp.remove(sop.getId());
      KademliaObserver.reportOperation(sop);
      // logger.warning("Sampling operation finished " + sop.getId());
      logger.warning("Sampling operation finished random dosampling " + sop.getId());
    } else {
      if (sop.getPending() == 0) {
        logger.warning("Doing sampling again " + sop.getId());
        BigInteger[] nextNodes = sop.doSampling();
        for (BigInteger nextNode : nextNodes) {
          BigInteger[] reqSamples = sop.getSamples();
          Message msg = generateGetSampleMessage(reqSamples);
          msg.operationId = sop.getId();
          msg.src = this.gossipsub.getGossipNode();
          msg.dst =
              GossipSubProtocol.nodeIdtoNode(nextNode, this.gossipsub.getProtocolID())
                  .getGossipProtocol()
                  .getGossipNode();
          sop.addMessage(msg.id);
          sendMessage(msg, nextNode);
          sop.getMessages();
        }
      }
    }
  }

  private HashMap<Message, List<Sample>> findMissingSamples(Sample[] samples) {
    HashMap<Message, List<Sample>> toSend = new HashMap<>();
    List<BigInteger> toRemove = new ArrayList<>();
    for (BigInteger id : missingSamples.keySet()) {
      if (kv.contains(id)) {
        Sample s = currentBlock.getSample(id);
        // if (kv.get(id) != null) {
        //  Sample s = (Sample) kv.get(id);
        for (Message msg : missingSamples.get(id)) {
          if (toSend.get(msg) != null) {
            toSend.get(msg).add(s);
          } else {
            List<Sample> sToSend = new ArrayList<>();
            sToSend.add(s);
            toSend.put(msg, sToSend);
          }
        }
        toRemove.add(s.getId());
      }
    }
    for (BigInteger id : toRemove) {
      missingSamples.remove(id);
    }

    return toSend;
  }

  protected void handleGetSample(Message m, int myPid) {
    // kv is for storing the sample you have
    logger.info("KV size " + kv.size() + " from:" + m.src.getId() + " " + m.id);
    // sample IDs that are requested in the message
    List<BigInteger> samples = Arrays.asList((BigInteger[]) m.body);

    List<Sample> samplesToSend = new ArrayList<>();

    for (BigInteger id : samples) {
      logger.info("Requesting sample " + id + " from " + m.src.getId());
      if (kv.contains(id)) {

        samplesToSend.add(currentBlock.getSample(id));
        // if (isEvil && samplesToSend.size() > 0) break;
      } else {
        if (missingSamples.get(id) != null) missingSamples.get(id).add(m);
        else {
          List<Message> requests = new ArrayList<>();
          requests.add(m);
          missingSamples.put(id, requests);
        }
        // logger.warning("Sample request missing");
      }
    }
    if (!samplesToSend.isEmpty()) {
      Message response =
          new Message(Message.MSG_GET_SAMPLE_RESPONSE, samplesToSend.toArray(new Sample[0]));
      response.operationId = m.operationId;
      response.dst = m.src;
      response.src = this.gossipsub.getGossipNode();
      response.ackId = m.id; // set ACK number
      sendMessage(response, m.src.getId());
    }
  }

  protected void handleGetSampleResponse(Message m, int myPid) {

    if (m.body == null) return;

    Sample[] samples = (Sample[]) m.body;

    SamplingOperation op = (SamplingOperation) samplingOp.get(m.operationId);
    // We continue an existing operation
    if (op != null) {
      op.increaseHops();
      op.addMessage(m.id);

      // keeping track of received samples
      op.elaborateResponse(samples, m.src.getId());
      logger.warning(
          "Continue operation "
              + op.getId()
              + " "
              + op.getHops()
              + " "
              + ((SamplingOperation) op).samplesCount());

      if (!op.completed() && op.getHops() < KademliaCommonConfigDas.MAX_HOPS) {
        if (op instanceof RandomSamplingOperationGossip
            && (CommonState.getTime() - op.getTimestamp())
                > KademliaCommonConfigDas.RANDOM_SAMPLING_DEADLINE) {
          samplingOp.remove(m.operationId);
          logger.warning("Sampling operation finished");
          KademliaObserver.reportOperation(op);
        }
        doSampling(op);
      } else {
        logger.warning("Operation completed");
        samplingOp.remove(m.operationId);
        logger.warning("Sampling operation finished random completed " + op.getId());
        KademliaObserver.reportOperation(op);
      }
    }
  }

  protected Message generateGetSampleMessage(BigInteger[] sampleId) {

    Message m = new Message(Message.MSG_GET_SAMPLE, sampleId);
    m.timestamp = CommonState.getTime();

    return m;
  }
}
