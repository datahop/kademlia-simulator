package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.KademliaNode;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.UniformRandomGenerator;

/**
 * This control initializes the whole network (that was already created by peersim) assigning a
 * unique NodeId, randomly generated, to every node.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class CustomDistributionDas implements peersim.core.Control {

  private static final String PAR_PROT = "protocolkad";
  private static final String PAR_PROT_DAS = "protocoldas";

  private int protocolID;
  private int protocolDasID;

  private BigInteger builderAddress;
  private UniformRandomGenerator urg;

  public CustomDistributionDas(String prefix) {
    protocolID = Configuration.getPid(prefix + "." + PAR_PROT);
    protocolDasID = Configuration.getPid(prefix + "." + PAR_PROT_DAS);
    urg = new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
  }

  /**
   * Scan over the nodes in the network and assign a randomly generated NodeId in the space
   * 0..2^BITS, where BITS is a parameter from the kademlia protocol (usually 160)
   *
   * @return boolean always false
   */
  public boolean execute() {
    // BigInteger tmp;
    for (int i = 0; i < Network.size(); ++i) {
      Node generalNode = Network.get(i);
      BigInteger id;
      // BigInteger attackerID = null;
      KademliaNode node;
      // String ip_address;
      id = urg.generate();
      // node = new KademliaNode(id, randomIpAddress(r), 0);
      node = new KademliaNode(id, "0.0.0.0", 0);

      KademliaProtocol kadProt = ((KademliaProtocol) (Network.get(i).getProtocol(protocolID)));
      generalNode.setKademliaProtocol(kadProt);
      kadProt.setNode(node);
      kadProt.setProtocolID(protocolID);
      DASProtocol dasProtocol = ((DASProtocol) (Network.get(i).getProtocol(protocolDasID)));
      dasProtocol.setKademliaProtocol(kadProt);
      kadProt.setEventsCallback(dasProtocol);

      if (i == 0) {
        dasProtocol.setBuilder(true);
        builderAddress = dasProtocol.getKademliaProtocol().getKademliaNode().getId();
      } else dasProtocol.setBuilderAddress(builderAddress);
    }

    return false;
  }
}
