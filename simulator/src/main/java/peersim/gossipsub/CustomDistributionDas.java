package peersim.gossipsub;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.UniformRandomGenerator;
import peersim.kademlia.das.KademliaCommonConfigDas;
import peersim.kademlia.das.SearchTable;

/**
 * This control initializes the whole network (that was already created by peersim) assigning a
 * unique NodeId, randomly generated, to every node.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class CustomDistributionDas implements peersim.core.Control {

  private static final String PAR_PROT_KAD = "protocolkad";
  private static final String PAR_PROT_DAS_BUILDER = "protocoldasbuilder";
  private static final String PAR_PROT_DAS_VALIDATOR = "protocoldasvalidator";
  private static final String PAR_PROT_DAS_NON_VALIDATOR = "protocoldasnonvalidator";
  private static final String PAR_PROT_EVIL_KAD = "protocolEvilkad";
  private static final String PAR_PROT_EVIL_DAS = "protocolEvildas";
  private static final String PAR_EVIL_RATIO_VAL = "evilNodeRatioValidator";
  private static final String PAR_EVIL_RATIO_NONVAL = "evilNodeRatioNonValidator";

  private static final String PAR_VALIDATOR_RATE = "validator_rate";

  /** Protocol identifiers for Kademlia, DAS, etc. * */
  // private int protocolEvilKadID;

  private int protocolDasBuilderID;

  private int protocolDasValidatorID;
  private int protocolDasNonValidatorID;

  private int protocolEvilDasID;
  /** Ratio of evil nodes to total number of nodes * */
  private double evilRatioValidator;

  private double evilRatioNonValidator;
  private double validatorRate;

  private BigInteger builderAddress;
  private UniformRandomGenerator urg;

  public CustomDistributionDas(String prefix) {
    // protocolEvilKadID = Configuration.getPid(prefix + "." + PAR_PROT_EVIL_KAD, protocolKadID);
    protocolDasBuilderID = Configuration.getPid(prefix + "." + PAR_PROT_DAS_BUILDER);
    protocolDasValidatorID = Configuration.getPid(prefix + "." + PAR_PROT_DAS_VALIDATOR);
    protocolDasNonValidatorID = Configuration.getPid(prefix + "." + PAR_PROT_DAS_NON_VALIDATOR);
    protocolEvilDasID = Configuration.getPid(prefix + "." + PAR_PROT_EVIL_DAS, 0);
    evilRatioValidator = Configuration.getDouble(prefix + "." + PAR_EVIL_RATIO_VAL, 0.0);
    evilRatioNonValidator = Configuration.getDouble(prefix + "." + PAR_EVIL_RATIO_NONVAL, 0.0);
    urg = new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    validatorRate = Configuration.getDouble(prefix + "." + PAR_VALIDATOR_RATE, 1.0);
  }

  /**
   * Scan over the nodes in the network and assign a randomly generated NodeId in the space
   * 0..2^BITS, where BITS is a parameter from the kademlia protocol (usually 160)
   *
   * @return boolean always false
   */
  public boolean execute() {

    int numValidators = (int) (Network.size() * validatorRate);

    System.out.println("Validators " + numValidators);

    int numEvilValidatorNodes = (int) (numValidators * evilRatioValidator);
    int numEvilNonValidatorNodes = (int) ((Network.size() - numValidators) * evilRatioNonValidator);
    System.out.println(
        "Number of malicious nodes: " + numEvilValidatorNodes + " " + numEvilNonValidatorNodes);
    List<BigInteger> validatorsIds = new ArrayList<>();
    // List<BigInteger> nonValidatorsIds = new ArrayList<>();

    for (int i = 0; i < Network.size(); ++i) {
      Node generalNode = Network.get(i);
      GossipNode node;
      // String ip_address;
      BigInteger id = urg.generate();
      // node = new KademliaNode(id, randomIpAddress(r), 0);
      node = new GossipNode(id);

      // KademliaProtocol kadProt = null;
      GossipSubDas dasProt = null;

      if (i == 0) {
        dasProt = ((GossipSubDas) (Network.get(i).getProtocol(protocolDasBuilderID)));
        dasProt.setNode(node);

      } else if ((i > 0) && (i < (numEvilValidatorNodes + 1))) {
        dasProt = ((GossipSubDas) (Network.get(i).getProtocol(protocolEvilDasID)));
        dasProt.setNode(node);
      } else if (i >= (numEvilValidatorNodes + numEvilNonValidatorNodes + 1)
          && i
              < (numEvilValidatorNodes
                  + numEvilNonValidatorNodes
                  + (numValidators - numEvilValidatorNodes)
                  + 1)) {
        dasProt = ((GossipSubDas) (Network.get(i).getProtocol(protocolDasValidatorID)));
        dasProt.setNode(node);
        // validatorsIds.add(kadProt.getKademliaNode().getId());
        validatorsIds.add(dasProt.getGossipNode().getId());

      } else {
        dasProt = ((GossipSubDas) (Network.get(i).getProtocol(protocolDasNonValidatorID)));
        dasProt.setNode(node);
        // nonValidatorsIds.add(kadProt.getKademliaNode().getId());
        // node.setServer(false);
      }

      // dasProt.setKademliaProtocol(kadProt);
      // kadProt.setEventsCallback(dasProt);

      // if (i == 0) builderAddress = dasProt.getKademliaProtocol().getKademliaNode().getId();
      // dasProt.setBuilderAddress(builderAddress);

      /*System.out.println(
      "Dasprot id "
          + protocolDasBuilderID
          + " "
          + protocolDasValidatorID
          + " "
          + protocolDasNonValidatorID
          + " "
          + protocolKadID);*/

      // generalNode.setProtocol(protocolKadID, kadProt);
      // generalNode.setKademliaProtocol(kadProt);
      generalNode.setGossipProtocol(dasProt);
      // dasProt.setDASProtocolID(protocolDasBuilderID);
      dasProt.setProtocolID(protocolDasBuilderID);

      generalNode.setProtocol(protocolDasBuilderID, dasProt);
      generalNode.setProtocol(protocolEvilDasID, null);
      generalNode.setProtocol(protocolDasValidatorID, null);
      generalNode.setProtocol(protocolDasNonValidatorID, null);
    }

    // System.out.println("Validators " + validatorsIds.size());
    // for (DASProtocol validator : validators) {
    /*for (int i = 0; i < Network.size(); i++) {
      Node generalNode = Network.get(i);
      if (i == 0) generalNode.getDASProtocol().setNonValidators(nonValidatorsIds);
      generalNode.getDASProtocol().addKnownValidator(validatorsIds.toArray(new BigInteger[0]));
    }*/
    SearchTable.addValidatorNodes(validatorsIds.toArray(new BigInteger[0]));

    KademliaCommonConfigDas.networkSize = Network.size();
    KademliaCommonConfigDas.validatorsSize = numValidators;
    return false;
  }
}
