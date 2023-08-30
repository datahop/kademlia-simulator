package peersim.gossipsub;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.SimpleEvent;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.KademliaCommonConfigDas;
import peersim.kademlia.das.MissingNode;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.operations.RandomSamplingOperation;
import peersim.kademlia.das.operations.SamplingOperation;
import peersim.kademlia.operations.Operation;
import peersim.transport.UnreliableTransport;

public abstract class GossipSubDas extends GossipSubProtocol implements MissingNode {

  protected LinkedHashMap<Long, SamplingOperation> samplingOp;

  protected HashMap<Long, List<String>> samplingTopics;

  protected Block currentBlock;

  protected BigInteger builderAddress;
  protected boolean isValidator;

  protected int row1;
  protected int row2;
  protected int col1;
  protected int col2;

  protected static HashMap<String, List<BigInteger>> nodeMap;
  protected boolean started;
  protected boolean isBuilder;
  private long uploadInterfaceBusyUntil;

  public GossipSubDas(String prefix) {
    super(prefix);
    samplingOp = new LinkedHashMap<>();
    samplingTopics = new HashMap<>();
    // TODO Auto-generated constructor stub
    isValidator = false;
    isBuilder = false;
    uploadInterfaceBusyUntil = 0;
  }
  /**
   * Replicate this object by returning an identical copy. It is called by the initializer and do
   * not fill any particular field.
   *
   * @return Object
   */
  public abstract Object clone();

  // protected abstract void startValidatorSampling(int row, int column, String topic, int myPid);

  // protected abstract void startRandomSampling(int myPid);
  /**
   * Start a topic query opearation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  /**
   * Start a topic query opearation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected abstract void handleInitNewBlock(Message m, int myPid);

  protected void handleMessage(Message m, int myPid) {
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
      case Message.MSG_GET_SAMPLE:
        m = (Message) event;
        handleGetSample(m, pid);
        break;
      case Message.MSG_GET_SAMPLE_RESPONSE:
        m = (Message) event;
        GossipObserver.reportMsg(m, false);
        handleGetSampleResponse(m, pid);
        break;
      default:
        super.processEvent(node, pid, event);
        break;
    }
  }

  public void setValidator(boolean isValidator) {
    this.isValidator = isValidator;
  }

  public boolean isValidator() {
    return isValidator;
  }

  public boolean isBuilder() {
    return isBuilder;
  }

  public int getDASProtocolID() {
    return gossipid;
  }

  /**
   * Sends a message using the current transport layer and starts the timeout timer if the message
   * is a request.
   *
   * @param m the message to send
   * @param destId the ID of the destination node
   * @param myPid the sender process ID (Todo: verify what myPid stand for!!!)
   */
  protected void sendMessage(Message m, BigInteger destId, int myPid) {

    // Assert that message source and destination nodes are not null
    assert m.src != null;
    assert m.dst != null;

    // Get source and destination nodes
    Node src = nodeIdtoNode(this.getGossipNode().getId(), gossipid);
    Node dest = nodeIdtoNode(destId, gossipid);

    // destpid = dest.getKademliaProtocol().getProtocolID();

    /*logger.warning(
    "Sending message "
        + m.getType()
        + " to "
        + destId
        + " "
        + ((GossipSubProtocol) dest.getProtocol(myPid)).getGossipNode().getId()
        + " from "
        + this.getGossipNode().getId()
        + " "
        + ((GossipSubProtocol) src.getProtocol(myPid)).getGossipNode().getId()
        + " "
        + m.getType());*/
    // Get the transport protocol
    m.nrHops++;
    transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);

    if (m.getType() != Message.MSG_GET_SAMPLE_RESPONSE && m.getType() != Message.MSG_MESSAGE) {
      // Send the message
      transport.send(src, dest, m, gossipid);
    } else {
      GossipObserver.reportMsg(m, true);
      Sample[] samples;
      if (m.getType() == Message.MSG_MESSAGE) samples = new Sample[] {(Sample) m.value};
      else {
        samples = (Sample[]) m.body;
      }
      double samplesSize = 0.0;
      if (samples != null) samplesSize = samples.length * KademliaCommonConfigDas.SAMPLE_SIZE;
      double msgSize = samplesSize;
      long propagationLatency = transport.getLatency(src, dest);
      // Add the transmission time of the message (upload)
      double transDelay = 0.0;
      if (this.isValidator) {
        transDelay = 1000 * msgSize / KademliaCommonConfigDas.VALIDATOR_UPLOAD_RATE;
      } else if (isBuilder()) {
        transDelay = 1000 * msgSize / KademliaCommonConfigDas.BUILDER_UPLOAD_RATE;
      } else {
        transDelay = 1000 * msgSize / KademliaCommonConfigDas.NON_VALIDATOR_UPLOAD_RATE;
      }
      // If the interface is busy, incorporate the additional delay
      // also update the time when interface is available again
      long timeNow = CommonState.getTime();
      long latency = propagationLatency;
      logger.info("Transmission propagationLatency " + latency);
      latency += (long) transDelay; // truncated value
      logger.info("Transmission total latency " + latency);
      if (this.uploadInterfaceBusyUntil > timeNow) {
        latency += this.uploadInterfaceBusyUntil - timeNow;
        this.uploadInterfaceBusyUntil += (long) transDelay; // truncated value

      } else {
        this.uploadInterfaceBusyUntil = timeNow + (long) transDelay; // truncated value
      }
      logger.info("Transmission " + latency + " " + transDelay);
      // add to sent msg
      // this.sentMsg.put(m.id, m.timestamp);
      EDSimulator.add(latency, m, dest, myPid);
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
            this.node.getId(),
            null,
            CommonState.getTime(),
            currentBlock,
            peers,
            this.isValidator,
            // validatorsList.length,
            this);
    // op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started random");
    /*while (!doSampling(op)) {
      op.increaseRadius(2);
    }*/
    op.createNodes(peers, builderAddress);
    doSampling(op);
  }

  protected boolean doSampling(SamplingOperation sop) {

    if (sop.completed()) {
      samplingOp.remove(sop.getId());
      GossipObserver.reportOperation(sop);
      // logger.warning("Sampling operation finished " + sop.getId());
      logger.warning("Sampling operation finished random dosampling " + sop.getId());
      return true;
    } else {
      boolean success = false;
      if (sop.getAvailableRequests() == 0) {
        while (!success) {
          logger.warning("Doing sampling again " + sop.getId());
          BigInteger[] nextNodes = sop.doSampling();
          for (BigInteger nextNode : nextNodes) {
            BigInteger[] reqSamples = sop.getSamples();
            logger.warning(
                "sending to node "
                    + nextNode
                    + " "
                    + reqSamples.length
                    + " "
                    + sop.getAvailableRequests()
                    + " "
                    + sop.getId());

            Message msg = generateGetSampleMessage(reqSamples);
            msg.operationId = sop.getId();
            msg.src = this.node;
            success = true;
            msg.dst =
                ((GossipSubProtocol) nodeIdtoNode(nextNode, gossipid).getProtocol(gossipid))
                    .getGossipNode();
            // msg.dst = Util.nodeIdtoNode(nextNode, gossipid).getGossipProtocol().getGossipNode();
            /*if (nextNode.compareTo(builderAddress) == 0) {
              logger.warning("Error sending to builder or 0 samples assigned");
              continue;
            }*/
            sop.addMessage(msg.id);
            // logger.warning("Send message " + dasID + " " + this);
            sendMessage(msg, nextNode, gossipid);
            sop.getMessages();
          }
          if (!success) {
            // sop.increaseRadius(2);
            success = ((RandomSamplingOperationGossip) sop).createNodes(peers, builderAddress);
            logger.warning("Retrying sampling");
            if (!success) {
              logger.warning(
                  "Sampling operation finished random failed " + sop.getId() + ". no more nodes");

              samplingOp.remove(sop.getId());
              GossipObserver.reportOperation(sop);
              success = true;
            }
          }
        }
      }
      return success;
    }
  }

  protected void handleGetSample(Message m, int myPid) {
    logger.warning("Get sample received");
    // kv is for storing the sample you have
    // logger.info("KV size " + kv.occupancy() + " from:" + m.src.getId() + " " + m.id);
    // sample IDs that are requested in the message
    List<BigInteger> samples = Arrays.asList((BigInteger[]) m.body);
    // samples to return
    List<Sample> s = new ArrayList<>();
    // if (!isValidator())
    //  logger.warning("Non-validator received " + samples.size() + " from " + m.src.getId());

    List<BigInteger> nodes = new ArrayList<>();
    for (BigInteger id : samples) {
      Sample sample = (Sample) mCache.get(id);
      if (sample != null) {
        s.add(sample);
      }
    }

    logger.warning("Get sample request responding with " + s.size() + " samples");

    Message response = new Message(Message.MSG_GET_SAMPLE_RESPONSE, s.toArray(new Sample[0]));
    response.operationId = m.operationId;
    response.dst = m.src;
    response.src = this.node;
    response.ackId = m.id; // set ACK number
    // response.value = returnedNodes;
    sendMessage(response, m.src.getId(), myPid);
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
        if (op instanceof RandomSamplingOperation
            && (CommonState.getTime() - op.getTimestamp())
                > KademliaCommonConfigDas.RANDOM_SAMPLING_DEADLINE) {
          samplingOp.remove(m.operationId);
          logger.warning("Sampling operation finished");
          GossipObserver.reportOperation(op);
        }
        doSampling(op);
      } else {
        logger.warning("Operation completed");
        samplingOp.remove(m.operationId);
        logger.warning("Sampling operation finished random completed " + op.getId());
        GossipObserver.reportOperation(op);
      }
    }
  }

  // ______________________________________________________________________________________________
  /**
   * generates a GET message for a specific sample.
   *
   * @return Message
   */
  protected Message generateGetSampleMessage(BigInteger[] sampleId) {

    Message m = new Message(Message.MSG_GET_SAMPLE, sampleId);
    m.timestamp = CommonState.getTime();

    return m;
  }

  public void setBuilderAddress(BigInteger addr) {
    this.builderAddress = addr;
  }

  @Override
  public void missing(BigInteger sample, Operation op) {

    logger.warning("Missing node for sample " + sample + " " + op.getId());
  }
}
