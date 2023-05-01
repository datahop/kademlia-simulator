package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
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
  private static final String PAR_VALIDATOR_RATE = "validator_rate";
  private int protocolID;
  private int protocolDasID;
  private double validatorRate;
  private BigInteger builderAddress;
  private UniformRandomGenerator urg;

  public CustomDistributionDas(String prefix) {
    protocolID = Configuration.getPid(prefix + "." + PAR_PROT);
    protocolDasID = Configuration.getPid(prefix + "." + PAR_PROT_DAS);
    validatorRate = Configuration.getDouble(prefix + "." + PAR_VALIDATOR_RATE, 1.0);
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

    int numValidators = (int) (Network.size() * validatorRate);
    List<BigInteger> validatorsIds = new ArrayList<>();
    List<DASProtocol> validators = new ArrayList<>();
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
      kadProt.setEventsCallback(dasProtocol);
      dasProtocol.setKademliaProtocol(kadProt);

      if (numValidators >= i) {
        dasProtocol.setValidator(true);
        validatorsIds.add(dasProtocol.getKademliaId());
        validators.add(dasProtocol);
      }
      if (i == 0) {
        dasProtocol.setBuilder(true);
        builderAddress = dasProtocol.getKademliaProtocol().getKademliaNode().getId();
      } else dasProtocol.setBuilderAddress(builderAddress);
    }

    System.out.println("Validators " + validatorsIds.size() + " " + validators.size());

    for (DASProtocol validator : validators) {
      validator.addKnownValidator(validatorsIds.toArray(new BigInteger[0]));
    }

    return false;
  }
}
