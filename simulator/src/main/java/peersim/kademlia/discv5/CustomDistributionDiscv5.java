package peersim.kademlia.discv5;

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
public class CustomDistributionDiscv5 implements peersim.core.Control {

  private static final String PAR_PROT = "protocolkad";
  private static final String PAR_PROT_DISCV5 = "protocoldiscv5";

  private int protocolID;
  private int protocolDiscv5ID;

  private UniformRandomGenerator urg;

  public CustomDistributionDiscv5(String prefix) {
    protocolID = Configuration.getPid(prefix + "." + PAR_PROT);
    protocolDiscv5ID = Configuration.getPid(prefix + "." + PAR_PROT_DISCV5);
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
      System.out.println("Custom distribution " + kadProt + " " + node.getId());
      generalNode.setKademliaProtocol(kadProt);
      kadProt.setNode(node);
      kadProt.setProtocolID(protocolID);

      Discv5Protocol discv5Prot = ((Discv5Protocol) (Network.get(i).getProtocol(protocolDiscv5ID)));
      discv5Prot.setKademliaProtocol(kadProt);
    }

    return false;
  }
}
