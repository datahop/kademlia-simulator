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
import java.util.TreeSet;
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
import peersim.kademlia.gossipsub.GossipSubProtocol;
import peersim.kademlia.operations.FindOperation;
import peersim.kademlia.operations.Operation;
import peersim.transport.UnreliableTransport;

public abstract class DASProtocol extends GossipSubProtocol
    implements Cloneable, EDProtocol, KademliaEvents, MissingNode {

  protected static final String PAR_TRANSPORT = "transport";
  // private static final String PAR_DASPROTOCOL = "dasprotocol";
  protected static final String PAR_KADEMLIA = "kademlia";
  protected static final String PAR_ALPHA = "alpha";
  protected static final String PAR_PARCEL = "parcelSize";
  private static String prefix = null;
  private UnreliableTransport transport;
  /** Store the time until which this node's uplink is busy sending data */
  private long uploadInterfaceBusyUntil;

  private int tid;
  protected int kademliaId;

  protected KademliaProtocol kadProtocol;
  /** allow to call the service initializer only once */
  protected static boolean _ALREADY_INSTALLED = false;

  protected Logger logger;

  protected BigInteger builderAddress;

  protected boolean isBuilder;

  protected boolean isValidator;

  protected KeyValueStore kv;

  protected Block currentBlock;

  protected LinkedHashMap<Long, SamplingOperation> samplingOp;

  protected LinkedHashMap<Operation, SamplingOperation> kadOps;

  protected boolean samplingStarted;

  protected SearchTable searchTable;

  protected int[] row, column;

  // protected int samplesRequested;

  // protected BigInteger[] validatorsList;

  protected HashSet<BigInteger> queried;

  protected int dasID;

  /** trace message sent for timeout purpose */
  protected TreeMap<Long, Long> sentMsg;

  protected long time;

  protected TreeSet<BigInteger> nonValidatorsIndexed; // , samplesIndexed;

  protected boolean isEvil;

  protected boolean missing;
  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public abstract Object clone();

  /**
   * Used only by the initializer when creating the prototype. Every other instance call CLONE to
   * create the new object.
   *
   * @param prefix String
   */
  public DASProtocol(String prefix) {
    super(prefix);

    DASProtocol.prefix = prefix;
    isEvil = false;
    _init();
    tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
    // samplesRequested = 0;
    kademliaId = Configuration.getPid(prefix + "." + PAR_KADEMLIA);

    KademliaCommonConfigDas.ALPHA =
        Configuration.getInt(prefix + "." + PAR_ALPHA, KademliaCommonConfigDas.ALPHA);

    KademliaCommonConfigDas.PARCEL_SIZE =
        Configuration.getInt(prefix + "." + PAR_PARCEL, KademliaCommonConfigDas.PARCEL_SIZE);
    kv = new KeyValueStore();
    samplingOp = new LinkedHashMap<Long, SamplingOperation>();
    kadOps = new LinkedHashMap<Operation, SamplingOperation>();
    samplingStarted = false;

    queried = new HashSet<BigInteger>();
    uploadInterfaceBusyUntil = 0;

    sentMsg = new TreeMap<Long, Long>();

    searchTable = new SearchTable();
    nonValidatorsIndexed = new TreeSet<>();
    isBuilder = false;
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
      // m.dst = this.kadProtocol.getKademliaNode();
      KademliaObserver.reportMsg(m, false);
      if (m.src != null)
        searchTable.seenNeighbour(m.src.getId(), Util.nodeIdtoNode(m.src.getId(), kademliaId));
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
      case Message.MSG_SEED_SAMPLE:
        m = (Message) event;
        handleSeedSample(m, myPid);
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

              doSampling(sop);
            }
          }
        }
        break;
      default:
        super.processEvent(myNode, myPid, event);
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
    /*searchTable = new SearchTable(currentBlock, this.getKademliaId());*/
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

  public boolean isValidator() {
    return this.isValidator;
  }

  public boolean isEvil() {
    return this.isEvil;
  }

  public void setBuilderAddress(BigInteger address) {
    this.builderAddress = address;
    searchTable.setBuilderAddress(address);
    logger.info("Set builderAddress " + address);
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
    missing = false;
    time = CommonState.getTime();
    currentBlock = (Block) m.body;
    kv.erase();
    // samplesRequested = 0;
    row = new int[currentBlock.getSize()];
    column = new int[currentBlock.getSize()];

    // clearing any pending operation from previous block
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

  protected void handleSeedSample(Message m, int myPid) {
    System.err.println("This should not happen");
    System.exit(-1);
  }

  /**
   * Start a topic query operation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected abstract void handleInitGetSample(Message m, int myPid);

  protected void handleGetSample(Message m, int myPid) {
    // kv is for storing the sample you have
    logger.info("KV size " + kv.occupancy() + " from:" + m.src.getId() + " " + m.id);
    // sample IDs that are requested in the message
    List<BigInteger> samples = Arrays.asList((BigInteger[]) m.body);
    // samples to return
    List<Sample> s = new ArrayList<>();

    // if (!isValidator())
    //  logger.warning("Non-validator received " + samples.size() + " from " + m.src.getId());

    for (BigInteger id : samples) {
      Sample sample = (Sample) kv.get(id);
      if (sample != null) {
        s.add(sample);
      }
    }
    // return a random subset of samples (if more than MAX_SAMPLES_RETURNED)
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
    if (m.value instanceof BigInteger[]) {
      BigInteger[] smpls = (BigInteger[]) m.value;
      List<Neighbour> neigh = new ArrayList<>();
      for (int i = 0; i < smpls.length; i++) {
        Neighbour[] neighs =
            searchTable.getNeighbours(
                smpls[i],
                currentBlock.computeRegionRadius(
                    KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER));
        for (Neighbour n : neighs) {
          neigh.add(n);
          if (neigh.size() >= KademliaCommonConfigDas.MAX_NODES_RETURNED) break;
        }
        if (neigh.size() >= KademliaCommonConfigDas.MAX_NODES_RETURNED) break;
      }
      response.value = neigh.toArray(new Neighbour[0]);
      // logger.warning("targeted sample request " + neigh.size());
    } else {
      response.value = searchTable.getNeighbours();
    }
    sendMessage(response, m.src.getId(), myPid);
  }

  private void reconstruct(Sample s) {
    column[s.getColumn() - 1]++;
    row[s.getRow() - 1]++;
    if (column[s.getColumn() - 1] >= column.length / 2
        && column[s.getColumn() - 1] != column.length) {
      Sample[] samples = currentBlock.getSamplesByColumn(s.getColumn());
      for (Sample sam : samples) {
        kv.add((BigInteger) sam.getIdByRow(), sam);
        kv.add((BigInteger) sam.getIdByColumn(), sam);
      }
      column[s.getColumn() - 1] = currentBlock.getSize();
    }
    if (row[s.getRow() - 1] >= row.length / 2 && row[s.getRow() - 1] != row.length) {
      Sample[] samples = currentBlock.getSamplesByRow(s.getRow());
      for (Sample sam : samples) {
        kv.add((BigInteger) sam.getIdByRow(), sam);
        kv.add((BigInteger) sam.getIdByColumn(), sam);
      }
      row[s.getRow() - 1] = currentBlock.getSize();
    }
  }

  protected void handleGetSampleResponse(Message m, int myPid) {

    if (m.body == null) return;

    Sample[] samples = (Sample[]) m.body;
    // searchTable.addNodes((BigInteger[]) m.value);

    KademliaObserver.reportPeerDiscovery(m, searchTable);
    for (Neighbour neigh : (Neighbour[]) m.value) {
      if (neigh.getId().compareTo(builderAddress) != 0) searchTable.addNeighbour(neigh);
    }
    for (Sample s : samples) {

      kv.add((BigInteger) s.getIdByRow(), s);
      kv.add((BigInteger) s.getIdByColumn(), s);
      // count # of samples for each row and column and reconstruct if more than half received
      reconstruct(s);
    }

    SamplingOperation op = (SamplingOperation) samplingOp.get(m.operationId);
    // We continue an existing operation

    logger.info(
        "Nodes discovered "
            + ((Neighbour[]) m.value).length
            + " "
            + searchTable.getAllNeighboursCount()
            + " "
            + searchTable.getValidatorsNeighboursCount()
            + " "
            + op);
    if (op != null) {
      // keeping track of received samples
      op.elaborateResponse(samples, m.src.getId());
      logger.warning(
          "Continue operation "
              + op.getId()
              + " "
              + op.getHops()
              + " "
              + searchTable.nodesIndexed().size()
              + " "
              + ((SamplingOperation) op).samplesCount());

      if (!op.completed() && op.getHops() < KademliaCommonConfigDas.MAX_HOPS) {
        if (op instanceof ValidatorSamplingOperation
                && (CommonState.getTime() - op.getTimestamp())
                    > KademliaCommonConfigDas.VALIDATOR_DEADLINE
            || op instanceof RandomSamplingOperation
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
        if (op instanceof ValidatorSamplingOperation)
          logger.warning("Sampling operation finished validator completed " + op.getId());
        else logger.warning("Sampling operation finished random completed " + op.getId());
        KademliaObserver.reportOperation(op);
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
  protected void sendMessage(Message m, BigInteger destId, int myPid) {

    // int destpid;
    assert m.src != null;
    assert m.dst != null;

    Node src = this.kadProtocol.getNode();
    Node dest = Util.nodeIdtoNode(destId, kademliaId);
    transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);

    if (m.getType() != Message.MSG_GET_SAMPLE_RESPONSE && m.getType() != Message.MSG_SEED_SAMPLE) {
      transport.send(src, dest, m, myPid);
    } else {
      // Send message taking into account the transmission delay and the availability of upload
      // interface
      // Timeout t = new Timeout(destId, m.id, m.operationId);
      Sample[] samples = (Sample[]) m.body;
      Neighbour[] nghbrs = (Neighbour[]) m.value;
      double samplesSize = 0.0;
      if (samples != null) samplesSize = samples.length * KademliaCommonConfigDas.SAMPLE_SIZE;
      double nghbrsSize = 0.0;
      if (nghbrs != null) nghbrsSize = nghbrs.length * KademliaCommonConfigDas.NODE_RECORD_SIZE;
      double msgSize = samplesSize + nghbrsSize;
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
      this.sentMsg.put(m.id, m.timestamp);
      EDSimulator.add(latency, m, dest, myPid);
    }
    // logger.warning("Sending message to " + m.dst.getId());

    // Setup timeout
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
  protected Message generateGetSampleMessage(BigInteger[] sampleId) {

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

  protected int rowWithHighestNumSamples() {
    int max = 1;
    for (int i = 0; i < row.length; i++) if (row[max] < row[i]) max = i;

    return max;
  }

  protected int columnWithHighestNumSamples() {
    int max = 1;
    for (int i = 0; i < column.length; i++) if (column[max] < column[i]) max = i;

    return max;
  }

  public void addKnownValidator(BigInteger[] ids) {
    logger.info("Adding validator list " + ids.length);
    // validatorsList = ids;
    // if (validatorsList != null && isBuilder()) searchTable.addValidatorNodes(validatorsList);
    if (isBuilder()) searchTable.addValidatorNodes(ids);
  }

  public void setNonValidators(List<BigInteger> nonValidators) {
    for (BigInteger id : nonValidators) {
      nonValidatorsIndexed.add(id);
    }
    searchTable.addNodes(nonValidators.toArray(new BigInteger[0]));
  }
  /**
   * Starts the random sampling operation
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  protected void startRandomSampling() {

    logger.warning("Starting random sampling");
    RandomSamplingOperation op =
        new RandomSamplingOperation(
            this.getKademliaId(),
            null,
            time,
            currentBlock,
            searchTable,
            this.isValidator,
            KademliaCommonConfigDas.validatorsSize,
            this);
    op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started random");
    /*while (!doSampling(op)) {
      op.increaseRadius(2);
    }*/
    doSampling(op);
  }

  protected boolean doSampling(SamplingOperation sop) {

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
      if (sop.getAvailableRequests() == 0) {
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
          msg.src = this.kadProtocol.getKademliaNode();
          if (missing) msg.value = reqSamples;
          success = true;
          msg.dst = Util.nodeIdtoNode(nextNode, kademliaId).getKademliaProtocol().getKademliaNode();
          /*if (nextNode.compareTo(builderAddress) == 0) {
            logger.warning("Error sending to builder or 0 samples assigned");
            continue;
          }*/
          sop.addMessage(msg.id);
          // logger.warning("Send message " + dasID + " " + this);
          sendMessage(msg, nextNode, dasID);
          sop.getMessages();
        }
        if (!success) {
          if (sop instanceof ValidatorSamplingOperation)
            logger.warning("Sampling operation finished validator failed " + sop.getId());
          else {
            logger.warning("Sampling operation finished random failed " + sop.getId());
            for (BigInteger id : sop.getSamples()) {
              logger.warning("Missing sample " + id + " for op " + sop.getId());
            }
          }

          samplingOp.remove(sop.getId());
          KademliaObserver.reportOperation(sop);
        }
      }
      return success;
    }
  }

  public void setDASProtocolID(int dasId) {
    this.dasID = dasId;
  }

  public int getDASProtocolID() {
    return this.dasID;
  }

  @Override
  public void putValueReceived(Object o) {}

  @Override
  public void operationComplete(Operation op) {
    if (op instanceof FindOperation) {
      logger.warning(
          "Findoperation complete with result " + op.isFinished() + " " + kadOps.size() + " " + op);
      FindOperation fop = (FindOperation) op;
      List<BigInteger> list = fop.getNeighboursList();
      list.remove(builderAddress);
      // searchTable.addNodes(list.toArray(new BigInteger[0]));
      for (BigInteger id : list) {
        Node n = Util.nodeIdtoNode(id, kademliaId);
        searchTable.addNeighbour(new Neighbour(id, n, n.getDASProtocol().isEvil()));
      }
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
    // searchTable.addNodes(list.toArray(new BigInteger[0]));
    for (BigInteger id : list) {
      Node n = Util.nodeIdtoNode(id, kademliaId);
      searchTable.addNeighbour(new Neighbour(id, n, n.getDASProtocol().isEvil()));
    }
    logger.info(
        "Search table nodes found "
            // + searchTable.samplesIndexed().size()
            // + " "
            + searchTable.nodesIndexed().size()
            + " "
            + neighbours.length);

    if (kadOps.get(op) != null) {
      if (!kadOps.get(op).completed()) {
        logger.info("Sampling operation found");
        doSampling(kadOps.get(op));
      }
    }
  }

  @Override
  public void missing(BigInteger sample, Operation op) {

    logger.warning("Missing nodes for sample " + sample + " " + kadOps.size());
    missing = true;
    /*List<BigInteger> ids =
        searchTable.getValidatorNodesbySample(
            sample,
            currentBlock.computeRegionRadius(
                KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER,
                KademliaCommonConfigDas.validatorsSize));
    for (BigInteger id : ids) {
      logger.warning("Found id " + id + " for sample " + sample);
    }*/
  }

  // ______________________________________________________________________________________________
  /**
   * generates a GET message for a specific sample.
   *
   * @return Message
   */
  protected Message generateSeedSampleMessage(Sample[] s) {

    Message m = new Message(Message.MSG_SEED_SAMPLE, s);
    m.timestamp = CommonState.getTime();

    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * generates a GET message for t1 key.
   *
   * @return Message
   */
  protected Message generateNewSampleMessage(BigInteger s) {

    Message m = Message.makeInitGetSample(s);
    m.timestamp = CommonState.getTime();

    return m;
  }

  public void refreshSearchTable() {
    // logger.warning("RefreshSearchTable");
    searchTable.refresh();
  }

  protected void handleMessage(Message m, int myPid) {

    logger.warning("handleMessage");
    super.handleMessage(m, myPid);
  }
}
