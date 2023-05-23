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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Logger;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.kademlia.KademliaEvents;
import peersim.kademlia.KademliaObserver;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.KeyValueStore;
import peersim.kademlia.Message;
import peersim.kademlia.SimpleEvent;
import peersim.kademlia.Timeout;
import peersim.kademlia.Util;
import peersim.kademlia.das.operations.RandomSamplingOperation;
import peersim.kademlia.das.operations.SamplingOperation;
import peersim.kademlia.das.operations.ValidatorSamplingOperation;
import peersim.kademlia.operations.FindOperation;
import peersim.kademlia.operations.Operation;
import peersim.transport.UnreliableTransport;

public class DASProtocol implements Cloneable, EDProtocol, KademliaEvents, MissingNode {

  private static final String PAR_TRANSPORT = "transport";
  // private static final String PAR_DASPROTOCOL = "dasprotocol";
  private static final String PAR_KADEMLIA = "kademlia";

  private static String prefix = null;
  private UnreliableTransport transport;
  private int tid;
  private int kademliaId;

  private KademliaProtocol kadProtocol;
  /** allow to call the service initializer only once */
  private static boolean _ALREADY_INSTALLED = false;

  protected Logger logger;

  private BigInteger builderAddress;

  private boolean isBuilder;

  private boolean isValidator;

  private KeyValueStore kv;

  private Block currentBlock;

  private LinkedHashMap<Long, SamplingOperation> samplingOp;

  private LinkedHashMap<Operation, SamplingOperation> kadOps;

  private boolean samplingStarted;

  private SearchTable searchTable;

  private int[] row, column;

  private int samplesRequested;

  private BigInteger[] validatorsList;

  private HashSet<BigInteger> queried;

  protected int dasID;

  /** trace message sent for timeout purpose */
  private TreeMap<Long, Long> sentMsg;

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
    // samplesRequested = 0;
    kademliaId = Configuration.getPid(prefix + "." + PAR_KADEMLIA);
    kv = new KeyValueStore();
    samplingOp = new LinkedHashMap<Long, SamplingOperation>();
    kadOps = new LinkedHashMap<Operation, SamplingOperation>();
    samplingStarted = false;
    isValidator = false;
    searchTable = new SearchTable(currentBlock);
    row = new int[KademliaCommonConfigDas.BLOCK_DIM_SIZE + 1];
    column = new int[KademliaCommonConfigDas.BLOCK_DIM_SIZE + 1];
    samplesRequested = 0;
    queried = new HashSet<BigInteger>();

    sentMsg = new TreeMap<Long, Long>();
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
      KademliaObserver.reportMsg(m, false);
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
        // logger.warning("Send message removed " + m.ackId);
        sentMsg.remove(m.ackId);
        handleGetSampleResponse(m, myPid);
        break;

      case Timeout.TIMEOUT: // timeout
        Timeout t = (Timeout) event;
        if (sentMsg.containsKey(t.msgID)) { // the response msg isn't arrived
          // remove form sentMsg
          logger.warning("Timeouuuuut! " + t.msgID);
          sentMsg.remove(t.msgID);
          // this.searchTable.removeNode(t.node);
          SamplingOperation sop = samplingOp.get(t.opID);
          if (sop != null) {
            if (!sop.completed()) {
              logger.warning("Samping operation found");
              int req = sop.getAvailableRequests() + 1;
              sop.setAvailableRequests(req);
              doSampling(sop);
            }
          }
        }
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
    logger.warning("Set builder " + isBuilder + " " + this.kademliaId);
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
  protected void handleInitNewBlock(Message m, int myPid) {
    currentBlock = (Block) m.body;
    kv.erase();
    samplesRequested = 0;
    row = new int[KademliaCommonConfigDas.BLOCK_DIM_SIZE + 1];
    column = new int[KademliaCommonConfigDas.BLOCK_DIM_SIZE + 1];
    if (isBuilder()) {

      logger.warning("Builder new block:" + currentBlock.getBlockId());
      while (currentBlock.hasNext()) {
        Sample s = currentBlock.next();
        kv.add(s.getIdByRow(), s);
        kv.add(s.getIdByColumn(), s);
      }
    } else {
      logger.warning("Non Builder new block:" + currentBlock.getBlockId());

      samplingStarted = false;
      searchTable.setBlock(currentBlock);
      if (validatorsList != null) searchTable.addNodes(validatorsList);
      for (int i = 0; i < row.length; i++) {
        row[i] = 0;
      }
      for (int i = 0; i < column.length; i++) {
        column[i] = 0;
      }
      for (int i = 0; i < 3; i++) {
        Message lookup = Util.generateFindNodeMessage();
        this.kadProtocol.handleInit(lookup, kademliaId);
      }
      Message lookup = Util.generateFindNodeMessage(this.getKademliaId());
      this.kadProtocol.handleInit(lookup, kademliaId);

      for (SamplingOperation sop : samplingOp.values()) {
        KademliaObserver.reportOperation(sop);
        if (sop instanceof ValidatorSamplingOperation)
          logger.warning("Sampling operation finished validator " + sop.getId());
        else logger.warning("Sampling operation finished random " + sop.getId());
      }
      samplingOp.clear();
      kadOps.clear();
      queried.clear();
    }
  }

  /**
   * Start a topic query operation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected void handleInitGetSample(Message m, int myPid) {
    BigInteger[] sampleId = new BigInteger[1];
    sampleId[0] = ((BigInteger) m.body);

    if (isBuilder()) return;

    logger.warning(
        "Getting sample from builder "
            + sampleId[0]
            + " from:"
            + builderAddress
            + " "
            + samplesRequested);
    Message msg = generateGetSampleMessage(sampleId);
    msg.operationId = -1;
    msg.src = this.getKademliaProtocol().getKademliaNode();
    msg.dst =
        this.getKademliaProtocol()
            .nodeIdtoNode(builderAddress)
            .getKademliaProtocol()
            .getKademliaNode();
    sendMessage(msg, builderAddress, myPid);
    samplesRequested++;
  }

  protected void handleGetSample(Message m, int myPid) {

    logger.warning("KV size " + kv.occupancy() + " from:" + m.src.getId() + " " + m.id);

    List<BigInteger> samples = Arrays.asList((BigInteger[]) m.body);
    List<Sample> s = new ArrayList<>();

    List<BigInteger> nodes = new ArrayList<>();
    for (BigInteger id : samples) {
      Sample sample = (Sample) kv.get(id);
      if (sample != null) {
        s.add(sample);
      }
      nodes.addAll(
          Arrays.asList(
              this.getKademliaProtocol()
                  .getRoutingTable()
                  .getNeighbours(Util.logDistance(id, this.getKademliaId()))));
    }

    Collections.shuffle(s);
    Sample[] returnedSamples;
    if (s.size() > KademliaCommonConfigDas.MAX_SAMPLES_RETURNED)
      returnedSamples =
          new HashSet<Sample>(s.subList(0, KademliaCommonConfigDas.MAX_SAMPLES_RETURNED))
              .toArray(new Sample[0]);
    else returnedSamples = s.toArray(new Sample[0]);

    Collections.shuffle(nodes);
    BigInteger[] returnedNodes;
    if (nodes.size() > KademliaCommonConfigDas.MAX_NODES_RETURNED)
      returnedNodes =
          new HashSet<BigInteger>(nodes.subList(0, KademliaCommonConfigDas.MAX_NODES_RETURNED))
              .toArray(new BigInteger[0]);
    else returnedNodes = nodes.toArray(new BigInteger[0]);

    logger.info("Get sample request responding with " + s.size() + " samples");

    Message response = new Message(Message.MSG_GET_SAMPLE_RESPONSE, returnedSamples);
    response.operationId = m.operationId;
    response.dst = m.src;
    response.src = this.kadProtocol.getKademliaNode();
    response.ackId = m.id; // set ACK number
    response.value = returnedNodes;
    sendMessage(response, m.src.getId(), myPid);
  }

  protected void handleGetSampleResponse(Message m, int myPid) {

    if (m.body == null) return;

    Sample[] samples = (Sample[]) m.body;
    searchTable.addNodes((BigInteger[]) m.value);
    for (Sample s : samples) {
      logger.warning(
          "Received sample:"
              + samples.length
              + " "
              + kv.occupancy()
              + " "
              + s.getRow()
              + " "
              + s.getColumn()
              + " "
              // + row[s.getRow()]
              // + " "
              // + column[s.getColumn()]
              // + " "
              + samplesRequested);
      if (!samplingStarted) samplesRequested--;

      kv.add((BigInteger) s.getIdByRow(), s);
      kv.add((BigInteger) s.getIdByColumn(), s);
      column[s.getColumn()]++;
      row[s.getRow()]++;
    }

    SamplingOperation op = (SamplingOperation) samplingOp.get(m.operationId);
    if (op != null) {
      op.elaborateResponse(samples);
      logger.warning(
          "Continue operation "
              + op.getId()
              + " "
              + op.getAvailableRequests()
              + " "
              + op.nrHops
              + " "
              + searchTable.nodesIndexed().size()
              + " "
              + ((SamplingOperation) op).samplesCount());

      if (!op.completed() && op.nrHops < KademliaCommonConfigDas.MAX_HOPS) {
        BigInteger[] nextNodes = op.doSampling();

        for (BigInteger nextNode : nextNodes) {
          logger.warning("sending to node " + nextNode);
          BigInteger[] reqSamples = op.getSamples();
          Message msg = generateGetSampleMessage(reqSamples);
          msg.operationId = op.getId();
          msg.src = this.kadProtocol.getKademliaNode();

          msg.dst = kadProtocol.nodeIdtoNode(nextNode).getKademliaProtocol().getKademliaNode();
          /*if (nextNode.compareTo(builderAddress) == 0) {
            logger.warning("Error sending to builder or 0 samples assigned");
            continue;
          }*/
          op.AddMessage(msg.id);
          sendMessage(msg, nextNode, myPid);
          op.nrHops++;
        }
        if (nextNodes.length == 0) {
          logger.warning(
              "No left nodes to ask "
                  + op.getAvailableRequests()
                  + " "
                  + kadOps.size()
                  + " "
                  + op.getSamples().length);
          if (op.getAvailableRequests() == KademliaCommonConfigDas.ALPHA) {
            for (BigInteger sample : op.getSamples()) logger.warning("Missing sample " + sample);
            op.increaseRadius(2);
            doSampling(op);
            /*samplingOp.remove(m.operationId);
            logger.warning("Sampling operation finished");
            KademliaObserver.reportOperation(op);*/

          }
        }
      } else {
        logger.warning("Operation completed");
        samplingOp.remove(m.operationId);
        if (op instanceof ValidatorSamplingOperation)
          logger.warning("Sampling operation finished validator completed " + op.getId());
        else logger.warning("Sampling operation finished random completed " + op.getId());
        KademliaObserver.reportOperation(op);
      }
    } else if (!samplingStarted && samplesRequested == 0) {
      if (isValidator()) {
        logger.warning("Starting validator (rows and columns) sampling");
        startRowsandColumnsSampling(m, myPid);
        startRandomSampling(m, myPid);
        samplingStarted = true;

      } else {
        logger.warning("Starting non-validator random sampling");
        startRandomSampling(m, myPid);
        samplingStarted = true;
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

    transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
    transport.send(src, dest, m, myPid);

    if (m.getType() == Message.MSG_GET_SAMPLE) { // is a request
      Timeout t = new Timeout(destId, m.id, m.operationId);
      long latency = transport.getLatency(src, dest);
      logger.warning("Send message added " + m.id + " " + latency);

      // add to sent msg
      this.sentMsg.put(m.id, m.timestamp);
      EDSimulator.add(4 * latency, t, src, myPid); // set delay = 2*RTT
    }
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

  private int rowWithHighestNumSamples() {
    int max = 1;
    for (int i = 0; i < row.length; i++) if (row[max] < row[i]) max = i;

    return max;
  }

  private int columnWithHighestNumSamples() {
    int max = 1;
    for (int i = 0; i < column.length; i++) if (column[max] < column[i]) max = i;

    return max;
  }

  public void addKnownValidator(BigInteger[] ids) {
    validatorsList = ids;
  }

  /**
   * Starts the random sampling operation
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  private void startRandomSampling(Message m, int myPid) {

    logger.warning("Starting random sampling");
    RandomSamplingOperation op =
        new RandomSamplingOperation(
            this.getKademliaId(),
            null,
            m.timestamp,
            currentBlock,
            searchTable,
            this.isValidator,
            this);
    op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started random");
    op.setAvailableRequests(KademliaCommonConfigDas.ALPHA);
    doSampling(op);
  }

  /**
   * Starts getting rows and columns, only for validators
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  private void startRowsandColumnsSampling(Message m, int myPid) {
    logger.warning(
        "Starting rows and columns fetch "
            + rowWithHighestNumSamples()
            + " "
            + row[rowWithHighestNumSamples()]
            + " "
            + columnWithHighestNumSamples()
            + " "
            + column[columnWithHighestNumSamples()]);

    // start 2 row 2 column Validator operation (1 row/column with the highest number of samples
    // already downloaded and another random)
    createValidatorSamplingOperation(rowWithHighestNumSamples(), 0, m.timestamp);
    createValidatorSamplingOperation(0, rowWithHighestNumSamples(), m.timestamp);
    createValidatorSamplingOperation(
        CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, 0, m.timestamp);
    createValidatorSamplingOperation(
        0, CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, m.timestamp);
  }

  private boolean doSampling(SamplingOperation sop) {

    if (sop.completed()) {
      samplingOp.remove(sop.getId());
      KademliaObserver.reportOperation(sop);
      // logger.warning("Sampling operation finished " + sop.getId());
      if (sop instanceof ValidatorSamplingOperation)
        logger.warning("Sampling operation finished validator dosampling " + sop.getId());
      else logger.warning("Sampling operation finished random dosampling " + sop.getId());
      return true;
    } else {
      boolean success = false;
      logger.warning("Dosampling " + sop.getAvailableRequests());
      BigInteger[] nextNodes = sop.doSampling();
      for (BigInteger nextNode : nextNodes) {
        BigInteger[] reqSamples = sop.getSamples();
        logger.warning(
            "sending to node "
                + nextNode
                + " "
                + reqSamples.length
                + " "
                + sop.getAvailableRequests());

        Message msg = generateGetSampleMessage(reqSamples);
        msg.operationId = sop.getId();
        msg.src = this.kadProtocol.getKademliaNode();
        success = true;
        msg.dst = kadProtocol.nodeIdtoNode(nextNode).getKademliaProtocol().getKademliaNode();
        /*if (nextNode.compareTo(builderAddress) == 0) {
          logger.warning("Error sending to builder or 0 samples assigned");
          continue;
        }*/
        sop.AddMessage(msg.id);
        // logger.warning("Send message " + dasID + " " + this);
        sendMessage(msg, nextNode, dasID);
        sop.nrHops++;
      }
      return success;
    }
  }

  private void createValidatorSamplingOperation(int row, int column, long timestamp) {
    ValidatorSamplingOperation op =
        new ValidatorSamplingOperation(
            this.getKademliaId(),
            timestamp,
            currentBlock,
            searchTable,
            row,
            column,
            this.isValidator,
            this);
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started validator " + op.getId());

    op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    op.setAvailableRequests(KademliaCommonConfigDas.ALPHA);
    doSampling(op);
    /*BigInteger sampleId =
        currentBlock
            .getSamplesIdsByRow(row)[
            CommonState.r.nextInt(
                currentBlock.getSamplesIdsByRow(row).length)];
    logger.warning("Sending lookup " + sampleId);
    Message lookup = Util.generateFindNodeMessage(sampleId);
    Operation lop = this.kadProtocol.handleInit(lookup, kademliaId);
    // logger.warning("Sent lookup " + lop);
    kadOps.put(lop, op);
    queried.add(sampleId);*/
  }

  public void setDASProtocolID(int dasId) {
    this.dasID = dasId;
  }

  public int getDASProtocolID() {
    return this.dasID;
  }

  @Override
  public void operationComplete(Operation op) {
    if (op instanceof FindOperation) {
      logger.warning(
          "Findoperation complete with result " + op.isFinished() + " " + kadOps.size() + " " + op);
      FindOperation fop = (FindOperation) op;
      List<BigInteger> list = fop.getNeighboursList();
      list.remove(builderAddress);
      searchTable.addNodes(list.toArray(new BigInteger[0]));
      logger.warning(
          "Search table operation complete"
              // + searchTable.samplesIndexed().size()
              // + " "
              + searchTable.nodesIndexed().size()
              + " "
              //       + kadOps.get(op).nrHops
              //       + " "
              + list.size());

      if (kadOps.get(op) == null) return;

      if (!kadOps.get(op).completed()) {
        doSampling(kadOps.get(op));
      }

      logger.warning(
          "Sampling operation found "
              + kadOps.size()
              + " "
              + kadOps.get(op).getAvailableRequests());
      kadOps.remove(op);
    }
  }

  /**
   * Callback of the kademlia protocol of the nodes found and contacted
   *
   * @param neihbours array with the ids of the nodes found
   */
  @Override
  public void nodesFound(Operation op, BigInteger[] neighbours) {
    List<BigInteger> list = new ArrayList<>(Arrays.asList(neighbours));
    list.remove(builderAddress);
    if (neighbours.length == 0) {
      logger.warning("No neighbours found");
      return;
    }
    searchTable.addNodes(list.toArray(new BigInteger[0]));
    logger.warning(
        "Search table nodes found "
            // + searchTable.samplesIndexed().size()
            // + " "
            + searchTable.nodesIndexed().size()
            + " "
            + neighbours.length);

    if (kadOps.get(op) != null) {
      if (!kadOps.get(op).completed()) {
        logger.info("Samping operation found");
        doSampling(kadOps.get(op));
      }
    }
  }

  @Override
  public void missing(BigInteger sample, Operation op) {

    logger.warning("Missing nodes for sample " + sample + " " + kadOps.size());
    /*if (!queried.contains(sample) && kadOps.size() < 3) {
      Message lookup = Util.generateFindNodeMessage(sample);
      Operation lop = this.kadProtocol.handleInit(lookup, kademliaId);
      kadOps.put(lop, (SamplingOperation) op);
      queried.add(sample);
      logger.warning("Sent lookup operation " + op);
    } else {
          logger.warning("All queried " + kadOps.size());
        }
      if (((SamplingOperation) op).getAvailableRequests() >= KademliaCommonConfig.ALPHA
          && kadOps.size() == 0) {
        samplingOp.remove(op.getId());
        logger.warning("Sampling operation finished");
        KademliaObserver.reportOperation(op);
      }*/
  }
}
