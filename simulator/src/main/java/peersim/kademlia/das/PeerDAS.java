package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.logging.Logger;
import peersim.config.Configuration;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.kademlia.KademliaObserver;
import peersim.kademlia.Message;
import peersim.kademlia.SimpleEvent;
import peersim.kademlia.das.operations.SamplingOperation;
import peersim.kademlia.gossipsub.GossipEvent;
import peersim.kademlia.gossipsub.GossipSubProtocol;

public abstract class PeerDAS implements Cloneable, EDProtocol, GossipEvent {

  protected static String prefix = null;
  protected GossipSubProtocol gossipsub;
  /** allow to call the service initializer only once */
  protected static boolean _ALREADY_INSTALLED = false;

  protected int[] row, column;
  protected HashSet<BigInteger> kv;

  protected Logger logger;
  private boolean msgReport;
  protected static final String PAR_TRANSPORT = "transport";
  protected static final String PAR_GOSSIP = "gossipsub";
  protected static final String PAR_MSG = "reportMsg";
  protected TreeMap<Long, Message> sentMsg;
  protected Block currentBlock;
  protected SearchTable searchTable;
  protected LinkedHashMap<Long, SamplingOperation> samplingOp;

  protected boolean isBuilder, isValidator;

  public PeerDAS(String prefix) {
    PeerDAS.prefix = prefix;
    _init();
    msgReport = Configuration.getBoolean(prefix + "." + PAR_MSG, false);
    sentMsg = new TreeMap<Long, Message>();

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
    this.logger = prot.getLogger();
    this.gossipsub.setEventsCallback(this);
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
}
