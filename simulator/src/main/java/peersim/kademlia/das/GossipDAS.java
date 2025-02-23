package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.kademlia.KademliaObserver;
import peersim.kademlia.Message;
import peersim.kademlia.SimpleEvent;
import peersim.kademlia.Timeout;
import peersim.kademlia.das.operations.SamplingOperation;
import peersim.kademlia.gossipsub.GossipEvent;
import peersim.kademlia.gossipsub.GossipSubProtocol;
import peersim.transport.BwTransport;

public abstract class GossipDAS implements Cloneable, EDProtocol, GossipEvent {

  protected static String prefix = null;
  protected GossipSubProtocol gossipsub;
  /** allow to call the service initializer only once */
  protected static boolean _ALREADY_INSTALLED = false;

  protected int[] row, column;
  protected HashSet<BigInteger> kv;
  protected BigInteger builderAddress;

  protected Logger logger;
  private boolean msgReport;
  protected static final String PAR_TRANSPORT = "transport";
  protected static final String PAR_GOSSIP = "gossipsub";
  protected static final String PAR_MSG = "reportMsg";
  protected TreeMap<Long, Message> sentMsg;
  protected Block currentBlock;
  protected SearchTable searchTable;
  protected LinkedHashMap<Long, SamplingOperation> samplingOp;
  private BwTransport transport;
  private int tid;
  private long uploadInterfaceBusyUntil;
  protected int protocolId;
  protected boolean isBuilder, isValidator;

  public GossipDAS(String prefix) {
    GossipDAS.prefix = prefix;
    _init();
    msgReport = Configuration.getBoolean(prefix + "." + PAR_MSG, false);
    sentMsg = new TreeMap<Long, Message>();
    tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);

    kv = new HashSet<>();
    isBuilder = true;
    isValidator = false;
    samplingOp = new LinkedHashMap<Long, SamplingOperation>();
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public abstract Object clone();

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
      logger.info("Reporting " + msgReport + " " + m.getType() + " " + m.getSize());

      if (msgReport
          && (m.getType() == Message.MSG_GET_SAMPLE
              || m.getType() == Message.MSG_GET_SAMPLE_RESPONSE
              || m.getType() == Message.MSG_SEED_SAMPLE))
        KademliaObserver.reportMsg(m, false, this.getNodeId());
    }

    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_INIT_NEW_BLOCK:
        m = (Message) event;
        handleInitNewBlock(m, myPid);
        break;
      case Message.MSG_GET_SAMPLE:
        m = (Message) event;
        handleGetSample(m, myPid);
        break;
      case Message.MSG_GET_SAMPLE_RESPONSE:
        m = (Message) event;
        handleGetSampleResponse(m, myPid);
        break;

      default:
        break;
    }
  }

  public void setBuilderAddress(BigInteger address) {
    this.builderAddress = address;
    searchTable.setBuilderAddress(address);
  }

  protected void handleInitNewBlock(Message m, int myPid) {
    row = new int[currentBlock.getSize()];
    column = new int[currentBlock.getSize()];
    kv.clear();
  }

  protected abstract void handleGetSample(Message m, int myPid);

  protected abstract void handleGetSampleResponse(Message m, int myPid);

  /**
   * sets the GossipSubProtocol instance can be used to run gossip operations
   *
   * @param prot GossipSubProtocol
   */
  public void setGossipProtocol(GossipSubProtocol prot) {
    this.gossipsub = prot;
    transport = (BwTransport) (Network.prototype).getProtocol(tid);
    if (this.isBuilder) {
      transport.setBw(KademliaCommonConfigDas.BUILDER_UPLOAD_RATE);
    } else {
      transport.setBw(KademliaCommonConfigDas.VALIDATOR_UPLOAD_RATE);
    }
    this.gossipsub.setTransport(this.transport);
    // this.logger = prot.getLogger();
    this.gossipsub.setEventsCallback(this);
    // Initialize the logger with the node ID as its name
    this.logger = Logger.getLogger(this.getNodeId().toString());

    // Disable the logger's parent handlers to avoid duplicate output
    this.logger.setUseParentHandlers(false);

    // Set the logger's level to WARNING
    this.logger.setLevel(Level.WARNING);
    // logger.setLevel(Level.ALL);

    // Create a console handler for the logger
    ConsoleHandler handler = new ConsoleHandler();
    // Set the handler's formatter to a custom format that includes the time and logger name
    handler.setFormatter(
        new SimpleFormatter() {
          private static final String format = "[%d][%s] %3$s %n";

          @Override
          public synchronized String format(LogRecord lr) {
            return String.format(format, CommonState.getTime(), logger.getName(), lr.getMessage());
          }
        });
    // Add the console handler to the logger
    this.logger.addHandler(handler);
    /*searchTable = new SearchTable(currentBlock, this.getKademliaId());*/
  }

  public BigInteger getNodeId() {
    return this.gossipsub.getGossipNode().getId();
  }

  public void setSearchTable(SearchTable searchTable) {
    this.searchTable = searchTable;
  }

  protected void reconstruct(Sample s) {
    column[s.getColumn() - 1]++;
    row[s.getRow() - 1]++;
    if (column[s.getColumn() - 1] >= column.length / 2
        && column[s.getColumn() - 1] != column.length) {
      Sample[] samples = currentBlock.getSamplesByColumn(s.getColumn());
      for (Sample sam : samples) {
        kv.add(sam.getIdByRow());
        // kv.add((BigInteger) sam.getIdByRow(), sam);
        // kv.add((BigInteger) sam.getIdByColumn(), sam);
      }
      column[s.getColumn() - 1] = currentBlock.getSize();
    }
    if (row[s.getRow() - 1] >= row.length / 2 && row[s.getRow() - 1] != row.length) {
      Sample[] samples = currentBlock.getSamplesByRow(s.getRow());
      for (Sample sam : samples) {
        kv.add(sam.getIdByRow());
        // kv.add((BigInteger) sam.getIdByRow(), sam);
        // kv.add((BigInteger) sam.getIdByColumn(), sam);
      }
      row[s.getRow() - 1] = currentBlock.getSize();
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
  protected void sendMessage(Message m, BigInteger destId) {

    // int destpid;
    assert m.src != null;
    assert m.dst != null;

    Node src = this.gossipsub.getNode();
    Node dest = GossipSubProtocol.nodeIdtoNode(destId, this.gossipsub.getProtocolID());

    if (msgReport
        && (m.getType() == Message.MSG_GET_SAMPLE
            || m.getType() == Message.MSG_GET_SAMPLE_RESPONSE
            || m.getType() == Message.MSG_SEED_SAMPLE))
      KademliaObserver.reportMsg(m, true, this.getNodeId());

    if (m.getType() != Message.MSG_GET_SAMPLE_RESPONSE && m.getType() != Message.MSG_SEED_SAMPLE) {
      transport.send(src, dest, m, this.protocolId);
    } else {
      // Send message taking into account the transmission delay and the availability of upload
      // interface
      // Timeout t = new Timeout(destId, m.id, m.operationId);
      Sample[] samples;
      if (m.getType() == Message.MSG_SEED_SAMPLE) {
        SeedingSampleBody body = (SeedingSampleBody) m.body;
        samples = (Sample[]) body.getsamplesList();
      } else {
        samples = (Sample[]) m.body;
      }
      // Sample[] samples = (Sample[]) m.body;
      // Neighbour[] nghbrs = (Neighbour[]) m.value;
      double msgSize = 0.0;
      if (samples != null) msgSize = samples.length * KademliaCommonConfigDas.SAMPLE_SIZE;
      long propagationLatency = transport.getLatency(src, dest);
      // Add the transmission time of the message (upload)
      double transDelay = 0.0;
      if (this.isValidator) {
        transDelay = 1000 * msgSize / KademliaCommonConfigDas.VALIDATOR_UPLOAD_RATE;
      } else if (this.isBuilder) {
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
      EDSimulator.add(latency, m, dest, this.protocolId);
    }

    // Setup timeout
    if (m.getType() == Message.MSG_GET_SAMPLE) { // is a request
      Timeout t = new Timeout(destId, m.id, m.operationId);
      long latency = transport.getLatency(src, dest);
      logger.info("Send message added " + m.id + " " + latency + " " + destId);

      // add to sent msg
      this.sentMsg.put(m.id, m);
      /// BigInteger[] samples = (Sample[]) m.body;

      long timeout = latency * 2 * 4; // 4 RTT
      if (timeout < 250) timeout = 250;
      EDSimulator.add(timeout, t, src, this.protocolId); // set delay = 2*RTT
    }
  }

  public void setProtocolId(int id) {
    this.protocolId = id;
  }
}
