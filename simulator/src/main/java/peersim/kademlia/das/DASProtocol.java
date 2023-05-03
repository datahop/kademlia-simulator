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
import java.util.logging.Logger;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.KademliaEvents;
import peersim.kademlia.KademliaObserver;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.KeyValueStore;
import peersim.kademlia.Message;
import peersim.kademlia.SimpleEvent;
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

  private Logger logger;

  private BigInteger builderAddress;

  private boolean isBuilder;

  private boolean isValidator;

  private KeyValueStore kv;

  private Block currentBlock;

  private LinkedHashMap<Long, SamplingOperation> samplingOp;

  private LinkedHashMap<Operation, SamplingOperation> kadOps;

  private boolean samplingStarted;

  private int pid;

  private SearchTable searchTable;

  private int[] row, column;

  private int samplesRequested;

  private BigInteger[] validatorsList;

  private HashSet<BigInteger> queried;
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

    kademliaId = Configuration.getPid(prefix + "." + PAR_KADEMLIA);
    kv = new KeyValueStore();
    samplingOp = new LinkedHashMap<Long, SamplingOperation>();
    kadOps = new LinkedHashMap<Operation, SamplingOperation>();
    samplingStarted = false;
    isValidator = false;
    searchTable = new SearchTable(currentBlock);
    row = new int[512];
    column = new int[512];
    samplesRequested = 0;
    queried = new HashSet<BigInteger>();
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
    pid = myPid;
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
    logger.warning("Set builder " + isBuilder);
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
    samplesRequested = 0;
    row = new int[512];
    column = new int[512];
    if (isBuilder()) {

      logger.warning("Builder new block:" + currentBlock.getBlockId());
      while (currentBlock.hasNext()) {
        Sample s = currentBlock.next();
        kv.add(s.getIdByRow(), s);
        kv.add(s.getIdByColumn(), s);
      }
    } else {
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
          logger.warning("Sampling operation finished validator" + sop.getId());
        else logger.warning("Sampling operation finished random" + sop.getId());
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
  private void handleInitGetSample(Message m, int myPid) {
    BigInteger[] sampleId = new BigInteger[1];
    sampleId[0] = ((BigInteger) m.body);

    if (isBuilder()) return;

    logger.info("Getting sample from builder " + sampleId[0]);
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

  private void handleGetSample(Message m, int myPid) {

    logger.info("KV size " + kv.occupancy());

    List<BigInteger> samples = Arrays.asList((BigInteger[]) m.body);
    List<Sample> s = new ArrayList<>();

    for (BigInteger id : samples) {
      Sample sample = (Sample) kv.get(id);
      if (sample != null) {
        s.add(sample);
      }
    }

    Collections.shuffle(s);
    Sample[] returnedSamples;
    if (s.size() > KademliaCommonConfigDas.MAX_SAMPLES_RETURNED)
      returnedSamples =
          new HashSet<Sample>(s.subList(0, KademliaCommonConfigDas.MAX_SAMPLES_RETURNED))
              .toArray(new Sample[0]);
    else returnedSamples = s.toArray(new Sample[0]);

    logger.info("Get sample request responding with " + s.size() + " samples");

    Message response = new Message(Message.MSG_GET_SAMPLE_RESPONSE, returnedSamples);
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
      kv.add((BigInteger) s.getIdByRow(), s);
      kv.add((BigInteger) s.getIdByColumn(), s);
      column[s.getColumn() - 1]++;
      row[s.getRow() - 1]++;
      logger.warning(
          "Sample received "
              + s.getRow()
              + " "
              + s.getColumn()
              + " "
              + row[s.getRow() - 1]
              + " "
              + column[s.getColumn() - 1]);
    }
    logger.warning("Received sample:" + samples.length + " " + kv.occupancy());

    if (samples.length == 1) samplesRequested--;
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
          BigInteger[] reqSamples = op.getSamples(nextNode);
          Message msg = generateGetSampleMessage(reqSamples);
          msg.operationId = op.getId();
          msg.src = this.kadProtocol.getKademliaNode();

          msg.dst = kadProtocol.nodeIdtoNode(nextNode).getKademliaProtocol().getKademliaNode();
          if (nextNode.compareTo(builderAddress) == 0) {
            logger.warning("Error sending to builder or 0 samples assigned");
            continue;
          }
          op.AddMessage(msg.id);
          sendMessage(msg, nextNode, myPid);
          op.nrHops++;
        }
        if (nextNodes.length == 0) {
          logger.warning("No left nodes to ask " + op.getAvailableRequests() + " " + kadOps.size());
          /*if (op.getAvailableRequests() == KademliaCommonConfig.ALPHA) {
            samplingOp.remove(m.operationId);
            logger.warning("Sampling operation finished");
            KademliaObserver.reportOperation(op);
          }*/
        }
      } else {
        logger.warning("Operation completed");
        samplingOp.remove(m.operationId);
        if (op instanceof ValidatorSamplingOperation)
          logger.warning("Sampling operation finished validator completed" + op.getId());
        else logger.warning("Sampling operation finished random completed" + op.getId());
        KademliaObserver.reportOperation(op);
      }
    } else if (!samplingStarted && samplesRequested == 0) {
      if (isValidator()) {
        logger.warning("Starting validator (rows and columns) sampling");
        startRowsandColumnsSampling(m, myPid);
        // startRandomSampling(m, myPid);
        samplingStarted = true;

      } else {
        logger.warning("Starting non-validator random sampling");
        // startRandomSampling(m, myPid);
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

  private int maxRow() {
    int max = 1;
    for (int i = 0; i < row.length; i++) if (row[max] < row[i]) max = i;

    return max;
  }

  private int maxColumn() {
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
    op.elaborateResponse(kv.getAll().toArray(new Sample[0]));

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
            + maxRow()
            + " "
            + row[maxRow()]
            + " "
            + maxColumn()
            + " "
            + column[maxColumn()]);

    ValidatorSamplingOperation op =
        new ValidatorSamplingOperation(
            this.getKademliaId(),
            m.timestamp,
            currentBlock,
            searchTable,
            maxRow() + 1,
            0,
            this.isValidator,
            this);
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started validator");

    op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    doSampling(op);
    BigInteger sampleId =
        currentBlock
            .getSamplesIdsByRow(maxRow() + 1)[
            CommonState.r.nextInt(currentBlock.getSamplesIdsByRow(maxRow() + 1).length)];
    logger.warning("Sending lookup " + sampleId);
    Message lookup = Util.generateFindNodeMessage(sampleId);
    Operation lop = this.kadProtocol.handleInit(lookup, kademliaId);
    logger.warning("Sent lookup " + lop);
    kadOps.put(lop, op);
    queried.add(sampleId);
    op =
        new ValidatorSamplingOperation(
            this.getKademliaId(),
            m.timestamp,
            currentBlock,
            searchTable,
            0,
            maxColumn() + 1,
            this.isValidator,
            this);
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started validator");

    op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    doSampling(op);
    sampleId =
        currentBlock
            .getSamplesIdsByRow(maxColumn() + 1)[
            CommonState.r.nextInt(currentBlock.getSamplesIdsByRow(maxColumn() + 1).length)];
    logger.warning("Sending lookup " + sampleId);
    lookup = Util.generateFindNodeMessage(sampleId);
    lop = this.kadProtocol.handleInit(lookup, kademliaId);
    logger.warning("Sent lookup " + lop);
    kadOps.put(lop, op);
    queried.add(sampleId);
  }

  private boolean doSampling(SamplingOperation sop) {

    if (sop.completed()) {
      samplingOp.remove(sop.getId());
      KademliaObserver.reportOperation(sop);
      // logger.warning("Sampling operation finished " + sop.getId());
      if (sop instanceof ValidatorSamplingOperation)
        logger.warning("Sampling operation finished validator dosampling" + sop.getId());
      else logger.warning("Sampling operation finished random dosampling" + sop.getId());
      return true;
    } else {
      boolean success = false;
      BigInteger[] nextNodes = sop.doSampling();
      for (BigInteger nextNode : nextNodes) {
        BigInteger[] reqSamples = sop.getSamples(nextNode);
        logger.warning("sending to node " + nextNode + " " + reqSamples.length);

        Message msg = generateGetSampleMessage(reqSamples);
        msg.operationId = sop.getId();
        msg.src = this.kadProtocol.getKademliaNode();
        success = true;
        msg.dst = kadProtocol.nodeIdtoNode(nextNode).getKademliaProtocol().getKademliaNode();
        if (nextNode.compareTo(builderAddress) == 0) {
          logger.warning("Error sending to builder or 0 samples assigned");
          continue;
        }
        sop.AddMessage(msg.id);
        sendMessage(msg, nextNode, pid);
        sop.nrHops++;
      }
      return success;
    }
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
      SamplingOperation sop = kadOps.get(op);
      logger.warning(
          "Sampling operation found " + kadOps.size() + " " + sop.getAvailableRequests());
      kadOps.remove(op);

      if (sop.getAvailableRequests() >= KademliaCommonConfig.ALPHA
          && kadOps.size() == 0
          && samplingOp.get(sop.getId()) != null) {
        samplingOp.remove(op.getId());
        if (sop instanceof ValidatorSamplingOperation)
          logger.warning("Sampling operation finished validator operationcompleted" + sop.getId());
        else logger.warning("Sampling operation finished random operationcompleted" + sop.getId());
        KademliaObserver.reportOperation(sop);
      }
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
    if (!queried.contains(sample) && kadOps.size() < 3) {
      Message lookup = Util.generateFindNodeMessage(sample);
      Operation lop = this.kadProtocol.handleInit(lookup, kademliaId);
      kadOps.put(lop, (SamplingOperation) op);
      queried.add(sample);
      logger.warning("Sent lookup operation " + op);
    } /*else {
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
