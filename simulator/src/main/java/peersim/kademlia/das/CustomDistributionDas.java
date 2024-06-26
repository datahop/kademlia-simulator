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

  private static final String PAR_PROT_KAD = "protocolkad";
  private static final String PAR_PROT_DAS_BUILDER = "protocoldasbuilder";
  private static final String PAR_PROT_DAS_VALIDATOR = "protocoldasvalidator";
  private static final String PAR_PROT_DAS_NON_VALIDATOR = "protocoldasnonvalidator";
  // private static final String PAR_PROT_EVIL_KAD = "protocolEvilkad";
  private static final String PAR_PROT_EVIL_DAS = "protocolEvildas";
  private static final String PAR_PROT_EVIL_VAL_DAS = "protocolEvilValDas";
  private static final String PAR_EVIL_RATIO_VAL = "evilNodeRatioValidator";
  private static final String PAR_EVIL_RATIO_NONVAL = "evilNodeRatioNonValidator";

  private static final String PAR_VALIDATOR_RATE = "validator_rate";

  /** Protocol identifiers for Kademlia, DAS, etc. * */
  private int protocolKadID;

  // private int protocolEvilKadID;
  private int protocolDasBuilderID;
  private int protocolDasValidatorID;
  private int protocolDasNonValidatorID;

  private int protocolEvilDasID;
  private int protocolEvilValDasID;

  /** Ratio of evil nodes to total number of nodes * */
  private double evilRatioValidator;

  private double evilRatioNonValidator;
  private double validatorRate;

  private BigInteger builderAddress;
  private UniformRandomGenerator urg;

  public CustomDistributionDas(String prefix) {
    protocolKadID = Configuration.getPid(prefix + "." + PAR_PROT_KAD);
    // protocolEvilKadID = Configuration.getPid(prefix + "." + PAR_PROT_EVIL_KAD, protocolKadID);
    protocolDasBuilderID = Configuration.getPid(prefix + "." + PAR_PROT_DAS_BUILDER);
    protocolDasValidatorID = Configuration.getPid(prefix + "." + PAR_PROT_DAS_VALIDATOR);
    protocolDasNonValidatorID = Configuration.getPid(prefix + "." + PAR_PROT_DAS_NON_VALIDATOR);
    protocolEvilDasID = Configuration.getPid(prefix + "." + PAR_PROT_EVIL_DAS, 0);
    protocolEvilValDasID = Configuration.getPid(prefix + "." + PAR_PROT_EVIL_VAL_DAS, 0);

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
    List<BigInteger> nonValidatorsIds = new ArrayList<>();
    List<Neighbour> evilNeighbours = new ArrayList<>();
    List<Node> validators = new ArrayList<>();
    // List<BigInteger> evilIds = new ArrayList<>();
    numValidators = numValidators - numEvilValidatorNodes;

    for (int i = 0; i < Network.size(); ++i) {
      Node generalNode = Network.get(i);
      BigInteger id;
      KademliaNode node;
      id = urg.generate();
      node = new KademliaNode(id, "0.0.0.0", 0);

      KademliaProtocol kadProt = null;
      DASProtocol dasProt = null;

      kadProt = ((KademliaProtocol) (Network.get(i).getProtocol(protocolKadID)));
      kadProt.setProtocolID(protocolKadID);
      kadProt.setNode(node);

      if (i == 0) {

        dasProt = ((DASProtocol) (Network.get(i).getProtocol(protocolDasBuilderID)));
        builderAddress = node.getId();
        validators.add(generalNode);
      } else if ((i > 0) && (i < (numEvilValidatorNodes + 1))) {
        dasProt = ((DASProtocol) (Network.get(i).getProtocol(protocolEvilValDasID)));
        validatorsIds.add(kadProt.getKademliaNode().getId());
        // evilNodes.add(generalNode);
        evilNeighbours.add(new Neighbour(id, generalNode, true));
        // evilIds.add(id);
      } else if ((i > numEvilValidatorNodes)
          && (i < (numEvilValidatorNodes + numEvilNonValidatorNodes + 1))) {
        dasProt = ((DASProtocol) (Network.get(i).getProtocol(protocolEvilDasID)));
        nonValidatorsIds.add(kadProt.getKademliaNode().getId());
        // evilNodes.add(generalNode);
        evilNeighbours.add(new Neighbour(id, generalNode, true));
        // evilIds.add(id);
      } else if (i > (numEvilValidatorNodes + numEvilNonValidatorNodes)
          && i < (numEvilValidatorNodes + numEvilNonValidatorNodes + (numValidators) + 1)) {
        dasProt = ((DASProtocol) (Network.get(i).getProtocol(protocolDasValidatorID)));
        validatorsIds.add(kadProt.getKademliaNode().getId());
      } else {
        dasProt = ((DASProtocol) (Network.get(i).getProtocol(protocolDasNonValidatorID)));
        nonValidatorsIds.add(kadProt.getKademliaNode().getId());
        // node.setServer(false);
      }

      dasProt.setKademliaProtocol(kadProt);
      kadProt.setEventsCallback(dasProt);

      dasProt.setBuilderAddress(builderAddress);

      if (dasProt instanceof DASProtocolBuilder) System.out.println("DASProtocol Builder " + i);
      generalNode.setProtocol(protocolKadID, kadProt);
      generalNode.setKademliaProtocol(kadProt);
      generalNode.setDASProtocol(dasProt);
      dasProt.setDASProtocolID(protocolDasBuilderID);

      generalNode.setProtocol(protocolDasBuilderID, dasProt);
      generalNode.setProtocol(protocolEvilDasID, null);
      generalNode.setProtocol(protocolEvilValDasID, null);
      generalNode.setProtocol(protocolDasValidatorID, null);
      generalNode.setProtocol(protocolDasNonValidatorID, null);
    }

    System.out.println("Validators " + validatorsIds.size());
    System.out.println("Non-Validators " + nonValidatorsIds.size());

    // for (DASProtocol validator : validators) {
    for (int i = 0; i < Network.size(); i++) {
      Node generalNode = Network.get(i);
      // if (i == 0) //
      generalNode.getDASProtocol().setNonValidators(nonValidatorsIds);
      generalNode.getDASProtocol().addKnownValidator(validatorsIds.toArray(new BigInteger[0]));
      // Populate builder's searchtable with all the validators
      if (generalNode.getDASProtocol().isBuilder()) {
        for (Node n : validators)
          generalNode
              .getDASProtocol()
              .getSearchTable()
              .addNeighbour(new Neighbour(n.getDASProtocol().getKademliaId(), n, false));
      }
      // bootstrap searchtables of evil nodes with other evil nodes
      if (generalNode.getDASProtocol().isEvil()) {
        SearchTable sTable = generalNode.getDASProtocol().getSearchTable();
        for (Neighbour evilNghbr : evilNeighbours) {
          if (evilNghbr.getId().equals(generalNode.getDASProtocol().getKademliaId()))
            // skip myself
            continue;
          sTable.addNeighbour(evilNghbr);
        }
      }
    }

    KademliaCommonConfigDas.networkSize = Network.size();
    KademliaCommonConfigDas.validatorsSize = numValidators;
    return false;
  }
}
