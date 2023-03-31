package peersim.kademlia.das;

/**
 * A Kademlia implementation for PeerSim extending the EDProtocol class.<br>
 * See the Kademlia bibliografy for more information about the protocol.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.logging.Logger;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.kademlia.KademliaEvents;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.KeyValueStore;
import peersim.kademlia.Message;
import peersim.kademlia.SimpleEvent;
import peersim.kademlia.das.operations.RandomSamplingOperation;
import peersim.kademlia.das.operations.SamplingOperation;
import peersim.kademlia.das.operations.ValidatorSamplingOperation;
import peersim.transport.UnreliableTransport;

public class DASProtocol implements Cloneable, EDProtocol, KademliaEvents {

  private static final String PAR_TRANSPORT = "transport";
  private static final String PAR_DASPROTOCOL = "dasprotocol";
  private static final String PAR_KADEMLIA = "kademlia";

  private static String prefix = null;
  private UnreliableTransport transport;
  private int tid;
  private int kademliaId;
  // private int kademliaId;

  private KademliaProtocol kadProtocol;
  /** allow to call the service initializer only once */
  private static boolean _ALREADY_INSTALLED = false;

  private Logger logger;

  private BigInteger builderAddress;

  private boolean isBuilder;

  private boolean isValidator;

  private KeyValueStore kv;

  private SearchTable searchTable;

  private Block currentBlock;

  private LinkedHashMap<Long, SamplingOperation> samplingOp;

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    DASProtocol dolly = new DASProtocol(DASProtocol.prefix);
    return dolly;
  }

  /**
   * Used only by the initializer when creating the prototype. Every other instance call CLONE to
   * create the new object.
   *
   * @param prefix String
   */
  public DASProtocol(String prefix) {

    DASProtocol.prefix = prefix;
    _init();
    tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
    // System.out.println("New DASProtocol");

    kademliaId = Configuration.getPid(prefix + "." + PAR_KADEMLIA);
    kv = new KeyValueStore();
    searchTable = new SearchTable();
    samplingOp = new LinkedHashMap<Long, SamplingOperation>();
  }

  /**
   * This procedure is called only once and allow to inizialize the internal state of protocol.
   * Every node shares the same configuration, so it is sufficient to call this routine once.
   */
  private void _init() {
    // execute once
    if (_ALREADY_INSTALLED) return;

    _ALREADY_INSTALLED = true;
  }

  /**
   * manage the peersim receiving of the events
   *
   * @param myNode Node
   * @param myPid int
   * @param event Object
   */
  public void processEvent(Node myNode, int myPid, Object event) {

    Message m;

    SimpleEvent s = (SimpleEvent) event;
    if (s instanceof Message) {
      m = (Message) event;
      m.dst = this.getKademliaProtocol().getKademliaNode();
    }

    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_INIT_NEW_BLOCK:
        m = (Message) event;
        handleInitNewBlock(m, myPid);
        break;
      case Message.MSG_INIT_GET_SAMPLE:
        m = (Message) event;
        handleInitGetSample(m, myPid);
        break;
      case Message.MSG_GET_SAMPLE:
        m = (Message) event;
        handleGetSample(m, myPid);
        break;
      case Message.MSG_GET_SAMPLE_RESPONSE:
        m = (Message) event;
        handleGetSampleResponse(m, myPid);
        break;
    }
  }

  /**
   * sets the kademliaprotocol instance can be used to run kad operations
   *
   * @param prot KademliaProtocol
   */
  public void setKademliaProtocol(KademliaProtocol prot) {
    this.kadProtocol = prot;
    this.logger = prot.getLogger();
  }

  /**
   * sets the kademliaprotocol instance can be used to run kad operations
   *
   * @return KademliaProtocol
   */
  public KademliaProtocol getKademliaProtocol() {
    return kadProtocol;
  }

  public boolean isBuilder() {
    return this.isBuilder;
  }

  public void setBuilder(boolean isBuilder) {
    this.isBuilder = isBuilder;
  }

  public boolean isValidator() {
    return this.isValidator;
  }

  public void setValidator(boolean isValidator) {
    this.isValidator = isValidator;
  }

  public void setBuilderAddress(BigInteger address) {
    this.builderAddress = address;
  }

  public BigInteger getBuilderAddress() {
    return this.builderAddress;
  }

  /**
   * Start a topic query opearation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  private void handleInitNewBlock(Message m, int myPid) {
    currentBlock = (Block) m.body;
    kv.erase();
    if (isBuilder()) {

      logger.info("Builder new block:" + currentBlock.getBlockId());
      while (currentBlock.hasNext()) {
        Sample s = currentBlock.next();
        kv.add(s.getId(), s);
      }
    } else /*if (isValidator()) {
             startRowsandColumnsFetch(m, myPid);
             startRandomSampling(m, myPid);
           } else */ {
      startRowsandColumnsFetch(m, myPid);
      // startRandomSampling(m, myPid);
    }
  }

  /**
   * Start a topic query opearation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  private void handleInitGetSample(Message m, int myPid) {
    BigInteger[] sampleId = new BigInteger[1];
    sampleId[0] = (BigInteger) m.body;

    if (isBuilder()) return;

    logger.info("Getting sample from builder " + sampleId);
    Message msg = generateGetSampleMessage(sampleId);
    msg.src = this.getKademliaProtocol().getKademliaNode();
    msg.dst =
        this.getKademliaProtocol()
            .nodeIdtoNode(builderAddress)
            .getKademliaProtocol()
            .getKademliaNode();
    sendMessage(msg, builderAddress, myPid);
  }

  private void handleGetSample(Message m, int myPid) {

    logger.info("KV size " + kv.occupancy());

    List<BigInteger> samples = Arrays.asList((BigInteger[]) m.body);
    List<Sample> s = new ArrayList<>();

    Collections.shuffle(samples);

    for (BigInteger id : samples) {
      Sample sample = (Sample) kv.get(id);
      if (sample != null) {
        s.add(sample);
        if (s.size() == KademliaCommonConfigDas.MAX_SAMPLES_RETURNED) break;
      }
      // else logger.warning("Sample not found");
    }

    logger.info("Get sample request responding with " + s.size() + " samples");

    Message response = new Message(Message.MSG_GET_SAMPLE_RESPONSE, s.toArray(new Sample[0]));
    response.operationId = m.operationId;
    response.dst = m.src;
    response.src = this.kadProtocol.getKademliaNode();
    response.ackId = m.id; // set ACK number
    sendMessage(response, m.src.getId(), myPid);
  }

  private void handleGetSampleResponse(Message m, int myPid) {

    if (m.body == null) return;

    Sample[] samples = (Sample[]) m.body;
    for (Sample s : samples) {
      logger.info("Received sample:" + s.getId());
      kv.add((BigInteger) s.getId(), s);
    }

    SamplingOperation op = (SamplingOperation) samplingOp.get(m.operationId);
    if (op != null) {
      op.elaborateResponse(samples);
      logger.warning(
          "Continue operation " + op.getId() + " " + op.getAvailableRequests() + " " + op.nrHops);
      // + " "
      // + op.getReceivedSamples().length);

      if (!op.completed()) {
        BigInteger[] nextNodes = op.continueSampling();
        for (BigInteger nextNode : nextNodes) {
          BigInteger[] reqSamples = op.getSamples(currentBlock, nextNode);
          Message msg = generateGetSampleMessage(reqSamples);
          msg.operationId = op.getId();
          msg.src = this.kadProtocol.getKademliaNode();

          msg.dst = kadProtocol.nodeIdtoNode(nextNode).getKademliaProtocol().getKademliaNode();
          if (nextNode.compareTo(builderAddress) == 0) {
            logger.info("Error sending to builder or 0 samples assigned");
            continue;
          }
          sendMessage(msg, nextNode, myPid);
          op.nrHops++;
        }
      } else {
        // logger.warning("Operation completed with " + op.getReceivedSamples().length + "
        // samples");
      }
    }
  }

  /**
   * send a message with current transport layer and starting the timeout timer (wich is an event)
   * if the message is a request
   *
   * @param m the message to send
   * @param destId the Id of the destination node
   * @param myPid the sender Pid
   */
  private void sendMessage(Message m, BigInteger destId, int myPid) {

    // int destpid;
    assert m.src != null;
    assert m.dst != null;

    Node src = this.kadProtocol.getNode();
    Node dest = this.kadProtocol.nodeIdtoNode(destId);

    // destpid = dest.getKademliaProtocol().getProtocolID();

    transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
    transport.send(src, dest, m, myPid);
  }

  // ______________________________________________________________________________________________
  /**
   * generates a GET message for a specific sample.
   *
   * @return Message
   */
  private Message generateGetSampleMessage(BigInteger[] sampleId) {

    Message m = new Message(Message.MSG_GET_SAMPLE, sampleId);
    m.timestamp = CommonState.getTime();

    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * Returns the dht id of the kademlia protocol
   *
   * @return Message
   */
  public BigInteger getKademliaId() {
    return this.getKademliaProtocol().getKademliaNode().getId();
  }

  /**
   * Callback of the kademlia protocol of the nodes found and contacted
   *
   * @param neihbours array with the ids of the nodes found
   */
  @Override
  public void nodesFound(BigInteger[] neighbours) {
    List<BigInteger> list = new ArrayList<>(Arrays.asList(neighbours));
    list.remove(builderAddress);
    searchTable.addNode(list.toArray(new BigInteger[0]));
  }

  /**
   * Starts the random sampling operation
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  private void startRandomSampling(Message m, int myPid) {

    RandomSamplingOperation op =
        new RandomSamplingOperation(this.getKademliaId(), null, searchTable, m.timestamp);
    samplingOp.put(op.getId(), op);
    BigInteger[] nodes = op.startSampling();

    for (BigInteger nextNode : nodes) {
      BigInteger[] samples = op.getSamples(currentBlock, nextNode);

      if (nextNode.compareTo(builderAddress) == 0 || samples.length == 0) {
        logger.warning("No samples to request");
        op.increaseAvailableRequests();
        continue;
      }
      Message msg = generateGetSampleMessage(samples);
      msg.operationId = op.getId();
      msg.src = kadProtocol.getKademliaNode();
      msg.dst = kadProtocol.nodeIdtoNode(nextNode).getKademliaProtocol().getKademliaNode();

      sendMessage(msg, nextNode, myPid);
      // logger.info("Sending sample request to: " + nextNode + " " + samples.length + " samples");
      op.nrHops++;
    }
  }

  /**
   * Starts getting rows and columns, only for validators
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  private void startRowsandColumnsFetch(Message m, int myPid) {
    logger.info("Starting rows and columns fetch");
    ValidatorSamplingOperation op =
        new ValidatorSamplingOperation(
            this.getKademliaId(), null, searchTable, m.timestamp, currentBlock);
    samplingOp.put(op.getId(), op);
    BigInteger[] nodes = op.startSampling();

    if (nodes == null || nodes.length == 0) {
      logger.warning("No nodes found");
      return;
    }

    for (BigInteger nextNode : nodes) {
      logger.warning("Next node " + nextNode);

      BigInteger[] samples = op.getSamples(currentBlock, nextNode);

      if (nextNode.compareTo(builderAddress) == 0 || samples.length == 0) {
        logger.warning("No samples to request");
        op.increaseAvailableRequests();
        continue;
      }
      Message msg = generateGetSampleMessage(samples);
      msg.operationId = op.getId();
      msg.src = kadProtocol.getKademliaNode();
      msg.dst = kadProtocol.nodeIdtoNode(nextNode).getKademliaProtocol().getKademliaNode();

      sendMessage(msg, nextNode, myPid);
      logger.warning("Sending sample request to: " + nextNode + " " + samples.length + " samples");
      op.nrHops++;
    }
  }
}
