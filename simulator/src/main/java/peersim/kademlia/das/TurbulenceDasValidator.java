package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.dynamics.NodeInitializer;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.KademliaNode;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.UniformRandomGenerator;
import peersim.kademlia.Util;

/**
 * Turbulcen class is only for test/statistical purpose. This Control execute a node add or remove
 * (failure) with a given probability.<br>
 * The probabilities are configurabily from the parameters p_idle, p_add, p_rem.<br>
 * - p_idle (default = 0): probability that the current execution does nothing (i.e. no adding and
 * no failures).<br>
 * - p_add (default = 0.5): probability that a new node is added in this execution.<br>
 * - p_rem (deafult = 0.5): probability that this execution will result in a failure of an existing
 * node.<br>
 * If the user desire to change one probability, all the probability value MUST be indicated in the
 * configuration file. <br>
 * Other parameters:<br>
 * - maxsize (default: infinite): max size of network. If this value is reached no more add
 * operation are performed.<br>
 * - minsize (default: 1): min size of network. If this value is reached no more remove operation
 * are performed.<br>
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class TurbulenceDasValidator implements Control {

  private static final String PAR_PROT = "protocolkad";
  private static final String PAR_PROT_DAS_BUILDER = "protocoldasbuilder";
  private static final String PAR_PROT_DAS_VAL = "protocoldasvalidator";
  private static final String PAR_PROT_DAS_NONVAL = "protocoldasnonvalidator";
  private static final String PAR_PROT_EVIL_DAS = "protocolEvildas";
  private static final String PAR_TRANSPORT = "transport";
  private static final String PAR_INIT = "init";

  /** Specify a minimum size for the network. by default there is no limit */
  private static final String PAR_MINSIZE = "minsize";

  /** Specify a maximum size for the network.by default there is limit of 1 */
  private static final String PAR_MAXSIZE = "maxsize";

  /** Idle probability */
  private static final String PAR_IDLE = "p_idle";

  /** Probability to add a node (in non-idle execution) */
  private static final String PAR_ADD = "p_add";

  /**
   * Probability to fail a node (in non-idle execution). Note: nodes will NOT be removed from the
   * network, but will be set as "DOWN", in order to let peersim exclude automatically the
   * delivering of events destinates to them
   */
  private static final String PAR_REM = "p_rem";

  /** Node initializers to apply on the newly added nodes */
  protected NodeInitializer[] inits;

  private String prefix;
  private int kademliaid;
  private int dasprotbuildid;
  private int dasprotvalid;
  private int dasprotnonvalid;
  private int dasevilprotid;
  private int transportid;
  private int maxsize;
  private int minsize;
  private double p_idle;
  private double p_add;
  private double p_rem;
  private UniformRandomGenerator urg;

  // ______________________________________________________________________________________________
  public TurbulenceDasValidator(String prefix) {
    this.prefix = prefix;
    kademliaid = Configuration.getPid(this.prefix + "." + PAR_PROT);
    dasprotbuildid = Configuration.getPid(this.prefix + "." + PAR_PROT_DAS_BUILDER);
    dasprotvalid = Configuration.getPid(this.prefix + "." + PAR_PROT_DAS_VAL);
    dasprotnonvalid = Configuration.getPid(this.prefix + "." + PAR_PROT_DAS_NONVAL);

    dasevilprotid = Configuration.getPid(this.prefix + "." + PAR_PROT_EVIL_DAS);
    transportid = Configuration.getPid(this.prefix + "." + PAR_TRANSPORT);

    minsize = Configuration.getInt(this.prefix + "." + PAR_MINSIZE, 1);
    maxsize = Configuration.getInt(this.prefix + "." + PAR_MAXSIZE, Integer.MAX_VALUE);

    Object[] tmp = Configuration.getInstanceArray(prefix + "." + PAR_INIT);
    inits = new NodeInitializer[tmp.length];
    for (int i = 0; i < tmp.length; ++i) inits[i] = (NodeInitializer) tmp[i];
    urg = new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);

    // Load probability from configuration file
    p_idle = Configuration.getDouble(this.prefix + "." + PAR_IDLE, 0); // idle default 0
    p_add = Configuration.getDouble(this.prefix + "." + PAR_ADD, 0.5); // add default 0.5
    p_rem = Configuration.getDouble(this.prefix + "." + PAR_REM, 0.5); // add default 0.5

    // Check probability values
    if (p_idle < 0 || p_idle > 1) {
      System.err.println(
          "Wrong event probabilty in Turbulence class: the probability PAR_IDLE must be between 0 and 1");
    } else if (p_add < 0 || p_add > 1) {
      System.err.println(
          "Wrong event probabilty in Turbulence class: the probability PAR_ADD must be between 0 and 1");
    } else if (p_rem < 0 || p_rem > 1) {
      System.err.println(
          "Wrong event probabilty in Turbulence class: the probability PAR_REM must be between 0 and 1");
    } else if (p_idle + p_add + p_rem > 1) {
      System.err.println(
          "Wrong event probabilty in Turbulence class: the sum of PAR_IDLE, PAR_ADD and PAR_REM must be 1");
    }

    System.err.println(
        String.format(
            "Turbulence: [p_idle=%f] [p_add=%f] [p_rem=%f] [p_tot=%f]",
            p_idle, p_add, p_rem, p_idle + p_add + p_rem));
    System.err.println(
        String.format(
            "Turbulence: [p_idle=%f] [p_add=%f] [(min,max)=(%d,%d)]",
            p_idle, p_add, maxsize, minsize));
  }

  // ______________________________________________________________________________________________
  public boolean add() {

    // Add Node
    Node newNode = (Node) Network.prototype.clone();
    for (int j = 0; j < inits.length; ++j) inits[j].initialize(newNode);
    Network.add(newNode);

    // Get kademlia protocol of new node
    KademliaProtocol newKad = (KademliaProtocol) (newNode.getProtocol(kademliaid));
    // newNode.setProtocol(kademliaid, newKad);
    // Set node ID

    KademliaNode node = new KademliaNode(urg.generate(), "127.0.0.1", 0);

    DASProtocol dasProt = ((DASProtocol) (newNode.getProtocol(dasprotvalid)));

    newKad.setNode(node);
    newKad.setProtocolID(kademliaid);
    newKad.setEventsCallback(dasProt);

    dasProt.setKademliaProtocol(newKad);
    dasProt.setDASProtocolID(dasprotbuildid);

    newNode.setKademliaProtocol(newKad);
    newNode.setProtocol(dasprotbuildid, dasProt);

    newNode.setDASProtocol(dasProt);
    newNode.setProtocol(dasprotvalid, null);
    newNode.setProtocol(dasevilprotid, null);
    newNode.setProtocol(dasprotnonvalid, null);

    int count = 0;
    BigInteger builderAddress = BigInteger.valueOf(0);
    for (int i = 0; i < Network.size(); ++i) {
      if (Network.get(i).isUp()) count++;
      if (Network.get(i).getDASProtocol().isBuilder()) {
        builderAddress = Network.get(i).getDASProtocol().getKademliaId();
        dasProt.setBuilderAddress(builderAddress);
      }
    }

    Node builder = Util.nodeIdtoNode(builderAddress, kademliaid);
    DASProtocol builderDAS = builder.getDASProtocol();
    builderDAS.getSearchTable().addValidatorNodes(new BigInteger[] {dasProt.getKademliaId()});
    int k = 0;
    while (k < 100) {
      Node n = Network.get(CommonState.r.nextInt(Network.size()));
      if (n.isUp()) {
        KademliaProtocol jKad = (KademliaProtocol) n.getProtocol(kademliaid);
        if (jKad.getKademliaNode().isServer()) {
          newKad.getRoutingTable().addNeighbour(jKad.getKademliaNode().getId());
          dasProt.searchTable.addNeighbour(
              new Neighbour(jKad.getKademliaNode().getId(), n, n.getDASProtocol().isEvil()));
        }
      }
      k++;
    }
    /*for (int j = 0; j < 3; j++) {
      // send message
      EDSimulator.add(0, Util.generateFindNodeMessage(), newNode, kademliaid);
    }
    EDSimulator.add(
        0, Util.generateFindNodeMessage(newKad.getKademliaNode().getId()), newNode, kademliaid);*/
    System.out.println(
        CommonState.getTime()
            + " Adding validator node "
            + count
            + " "
            + newKad.getKademliaNode().getId()
            + " "
            + dasProt.getBuilderAddress());
    return false;
  }

  // ______________________________________________________________________________________________
  public boolean rem() {
    // Select one random node to remove
    Node remove;
    DASProtocol dasProt;
    int i = Network.size();
    do {
      remove = Network.get(CommonState.r.nextInt(Network.size()));
      dasProt = ((DASProtocol) (remove).getProtocol(dasprotbuildid));
      // } while ((remove == null) || dasProt.isBuilder() || dasProt.isValidator() ||
      // (!remove.isUp()));
      i--;
    } while ((remove == null || dasProt.isBuilder() || !remove.isUp() || !dasProt.isValidator())
        && i > 0);
    // Remove node (set its state to DOWN)

    System.out.println("Removing validator " + dasProt.getKademliaId());

    remove.setFailState(Node.DOWN);
    return false;
  }

  // ______________________________________________________________________________________________
  public boolean execute() {
    // Throw the dice
    double dice = CommonState.r.nextDouble();
    if (dice < p_idle) return false;

    // Get network size
    int sz = Network.size();
    for (int i = 0; i < Network.size(); i++) if (!Network.get(i).isUp()) sz--;

    // Perform the correct operation basing on the probability
    if (dice < p_idle) {
      return false; // do nothing
    } else if (dice < (p_idle + p_add) && sz < maxsize) {
      return add();
    } else if (sz > minsize) {
      return rem();
    }

    return false;
  }
} // End of class
