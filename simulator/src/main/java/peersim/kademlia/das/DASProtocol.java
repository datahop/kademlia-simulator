package peersim.kademlia.das;

/**
 * A Kademlia implementation for PeerSim extending the EDProtocol class.<br>
 * See the Kademlia bibliografy for more information about the protocol.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
import java.util.TreeMap;
import java.util.logging.Logger;
import peersim.config.Configuration;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.Message;
import peersim.kademlia.RoutingTable;
import peersim.kademlia.SimpleEvent;
import peersim.transport.UnreliableTransport;

public class DASProtocol implements Cloneable, EDProtocol {

  private static final String PAR_TRANSPORT = "transport";
  private static final String PAR_DASPROTOCOL = "dasprotocol";

  private static String prefix = null;
  private UnreliableTransport transport;
  private int tid;
  private int protocolId;
  // private int kademliaId;
  /** trace message sent for timeout purpose */
  private TreeMap<Long, Long> sentMsg;

  private KademliaProtocol kadProtocol;
  /** allow to call the service initializer only once */
  private static boolean _ALREADY_INSTALLED = false;

  private Logger logger;

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
    sentMsg = new TreeMap<Long, Long>();
    tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);

    // kademliaId = Configuration.getPid(prefix + "." + PAR_KADEMLIA);
  }

  /**
   * This procedure is called only once and allow to inizialize the internal state of
   * KademliaProtocol. Every node shares the same configuration, so it is sufficient to call this
   * routine once.
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

    // Parse message content Activate the correct event manager fot the particular event
    this.protocolId = myPid;

    Message m;

    SimpleEvent s = (SimpleEvent) event;
    if (s instanceof Message) {
      m = (Message) event;
      m.dst = this.getKademliaProtocol().getNode();
    }

    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_INIT_NEW_BLOCK:
        m = (Message) event;
        handleInitNewBlock(m, myPid);
        break;
    }
  }

  public void setKademliaProtocol(KademliaProtocol prot) {
    this.kadProtocol = prot;
    this.logger = prot.getLogger();
  }

  public KademliaProtocol getKademliaProtocol() {
    // System.out.println(
    //    "getKademliaProtocol " + kademliaId + " " + (Network.prototype).getProtocol(kademliaId));
    // return (KademliaProtocol) (Network.prototype).getProtocol(kademliaId);
    return kadProtocol;
  }

  /**
   * Start a topic query opearation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  private void handleInitNewBlock(Message m, int myPid) {}

  // public void refreshBucket(TicketTable rou, BigInteger node, int distance) {
  public void refreshBucket(RoutingTable rou, int distance) {}
}
