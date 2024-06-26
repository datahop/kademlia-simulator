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

/** TurbulenceDasValidator adds/removes validators with a certain probability every step */
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
    newNode.setKademliaProtocol(newKad);
    newKad.setProtocolID(kademliaid);
    // newNode.setProtocol(kademliaid, newKad);
    // Set node ID
    UniformRandomGenerator urg =
        new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    KademliaNode node = new KademliaNode(urg.generate(), "127.0.0.1", 0);
    ((KademliaProtocol) (newNode.getProtocol(kademliaid))).setNode(node);

    DASProtocol dasProt = ((DASProtocol) (newNode.getProtocol(dasprotvalid)));

    newNode.setProtocol(dasprotbuildid, dasProt);
    newKad.setNode(node);

    dasProt.setKademliaProtocol(newKad);
    dasProt.setDASProtocolID(dasprotbuildid);
    newKad.setEventsCallback(dasProt);
    newNode.setDASProtocol(dasProt);
    newNode.setProtocol(dasprotvalid, null);
    newNode.setProtocol(dasevilprotid, null);
    newNode.setProtocol(dasprotnonvalid, null);

    for (int i = 0; i < Network.size(); ++i) {
      if (Network.get(i).getDASProtocol().isBuilder()) {
        BigInteger builderAddress = Network.get(i).getDASProtocol().getKademliaId();
        dasProt.setBuilderAddress(builderAddress);
        Network.get(i)
            .getDASProtocol()
            .addKnownValidator(new BigInteger[] {dasProt.getKademliaId()});

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

        break;
      }
    }

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
