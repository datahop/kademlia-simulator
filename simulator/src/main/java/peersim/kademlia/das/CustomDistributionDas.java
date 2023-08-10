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
  private static final String PAR_PROT_EVIL_KAD = "protocolEvilkad";
  private static final String PAR_PROT_EVIL_DAS = "protocolEvildas";
  private static final String PAR_EVIL_RATIO_VAL = "evilNodeRatioValidator";
  private static final String PAR_EVIL_RATIO_NONVAL = "evilNodeRatioNonValidator";

  private static final String PAR_VALIDATOR_RATE = "validator_rate";

  /** Protocol identifiers for Kademlia, DAS, etc. * */
  private int protocolKadID;

  private int protocolEvilKadID;
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
    protocolKadID = Configuration.getPid(prefix + "." + PAR_PROT_KAD);
    protocolEvilKadID = Configuration.getPid(prefix + "." + PAR_PROT_EVIL_KAD, protocolKadID);
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
    List<BigInteger> nonValidatorsIds = new ArrayList<>();

    for (int i = 0; i < Network.size(); ++i) {
      Node generalNode = Network.get(i);
      BigInteger id;
      KademliaNode node;
      id = urg.generate();
      node = new KademliaNode(id, "0.0.0.0", 0);

      KademliaProtocol kadProt = null;
      DASProtocol dasProt = null;

      /** Generate honest and evil nodes * */
      /*if (i == 0) {
        kadProt = ((KademliaProtocol) (Network.get(i).getProtocol(protocolKadID)));
        dasProt = ((DASProtocol) (Network.get(i).getProtocol(protocolDasBuilderID)));
        System.out.println("Init builder " + i);
        kadProt.setProtocolID(protocolKadID);
        dasProt.setDASProtocolID(protocolDasBuilderID);
        dasProt.setKademliaProtocol(kadProt);
        kadProt.setEventsCallback(dasProt);
        kadProt.setNode(node);

        builderAddress = dasProt.getKademliaProtocol().getKademliaNode().getId();

        generalNode.setProtocol(protocolDasValidatorID, null);
        generalNode.setProtocol(protocolDasNonValidatorID, null);
        generalNode.setProtocol(protocolDasBuilderID, dasProt);
        generalNode.setProtocol(protocolEvilDasID, null);
      } else if ((i > 0) && (i < (numEvilValidatorNodes + 1))) {
        System.out.println("Init evil validator " + i);

        kadProt = ((KademliaProtocol) (Network.get(i).getProtocol(protocolEvilKadID)));
        dasProt = ((EvilDASProtocol) (Network.get(i).getProtocol(protocolEvilDasID)));
        kadProt.setProtocolID(protocolEvilKadID);
        dasProt.setDASProtocolID(protocolEvilDasID);
        dasProt.setKademliaProtocol(kadProt);
        kadProt.setEventsCallback(dasProt);
        kadProt.setNode(node);
        node.setEvil(true);
        dasProt.setBuilderAddress(builderAddress);

        generalNode.setProtocol(protocolDasValidatorID, null);
        generalNode.setProtocol(protocolDasNonValidatorID, null);
        generalNode.setProtocol(protocolDasBuilderID, null);
        generalNode.setProtocol(protocolEvilDasID, dasProt);

        validatorsIds.add(dasProt.getKademliaId());
        validators.add(dasProt);
      } else if (i >= (numEvilValidatorNodes + 1)
          && i < (numEvilValidatorNodes + numEvilNonValidatorNodes + 1)) {
        System.out.println("Init evil non validator " + i);
        kadProt = ((KademliaProtocol) (Network.get(i).getProtocol(protocolEvilKadID)));
        dasProt = ((EvilDASProtocol) (Network.get(i).getProtocol(protocolEvilDasID)));
        kadProt.setProtocolID(protocolEvilKadID);
        dasProt.setDASProtocolID(protocolEvilDasID);
        dasProt.setKademliaProtocol(kadProt);
        kadProt.setEventsCallback(dasProt);
        kadProt.setNode(node);
        node.setEvil(true);
        dasProt.setBuilderAddress(builderAddress);

        generalNode.setProtocol(protocolDasValidatorID, null);
        generalNode.setProtocol(protocolDasNonValidatorID, null);
        generalNode.setProtocol(protocolDasBuilderID, null);
        generalNode.setProtocol(protocolEvilDasID, dasProt);

      } else if (i >= (numEvilValidatorNodes + numEvilNonValidatorNodes + 1)
          && i
              < (numEvilValidatorNodes
                  + numEvilNonValidatorNodes
                  + (numValidators - numEvilValidatorNodes)
                  + 1)) {
        assert (!node.isEvil());
        System.out.println("Init validator " + i);

        kadProt = ((KademliaProtocol) (Network.get(i).getProtocol(protocolKadID)));
        dasProt = ((DASProtocol) (Network.get(i).getProtocol(protocolDasValidatorID)));
        kadProt.setProtocolID(protocolKadID);
        dasProt.setDASProtocolID(protocolDasValidatorID);
        dasProt.setBuilderAddress(builderAddress);
        dasProt.setKademliaProtocol(kadProt);
        kadProt.setEventsCallback(dasProt);
        kadProt.setNode(node);

        validatorsIds.add(kadProt.getKademliaNode().getId());
        validators.add(dasProt);

        generalNode.setProtocol(protocolDasValidatorID, dasProt);
        generalNode.setProtocol(protocolDasNonValidatorID, null);
        generalNode.setProtocol(protocolDasBuilderID, null);
        generalNode.setProtocol(protocolEvilDasID, null);

      } else {
        assert (!node.isEvil());
        System.out.println("Init non-validator " + i);
        kadProt = ((KademliaProtocol) (Network.get(i).getProtocol(protocolKadID)));
        dasProt = ((DASProtocol) (Network.get(i).getProtocol(protocolDasNonValidatorID)));
        kadProt.setProtocolID(protocolKadID);
        dasProt.setDASProtocolID(protocolDasNonValidatorID);
        dasProt.setBuilderAddress(builderAddress);
        dasProt.setKademliaProtocol(kadProt);
        kadProt.setEventsCallback(dasProt);
        kadProt.setNode(node);

        generalNode.setProtocol(protocolDasValidatorID, null);
        generalNode.setProtocol(protocolDasNonValidatorID, dasProt);
        generalNode.setProtocol(protocolDasBuilderID, null);
        generalNode.setProtocol(protocolEvilDasID, null);
      }*/

      kadProt = ((KademliaProtocol) (Network.get(i).getProtocol(protocolKadID)));
      kadProt.setProtocolID(protocolKadID);
      kadProt.setNode(node);

      if (i == 0) {
        dasProt = ((DASProtocol) (Network.get(i).getProtocol(protocolDasBuilderID)));

      } else if ((i > 0) && (i < (numEvilValidatorNodes + 1))) {
        dasProt = ((DASProtocol) (Network.get(i).getProtocol(protocolEvilDasID)));
      } else if (i >= (numEvilValidatorNodes + numEvilNonValidatorNodes + 1)
          && i
              < (numEvilValidatorNodes
                  + numEvilNonValidatorNodes
                  + (numValidators - numEvilValidatorNodes)
                  + 1)) {
        dasProt = ((DASProtocol) (Network.get(i).getProtocol(protocolDasValidatorID)));
        validatorsIds.add(kadProt.getKademliaNode().getId());

      } else {
        dasProt = ((DASProtocol) (Network.get(i).getProtocol(protocolDasNonValidatorID)));
        nonValidatorsIds.add(kadProt.getKademliaNode().getId());
      }

      dasProt.setKademliaProtocol(kadProt);
      kadProt.setEventsCallback(dasProt);

      if (i == 0) builderAddress = dasProt.getKademliaProtocol().getKademliaNode().getId();
      dasProt.setBuilderAddress(builderAddress);

      /*System.out.println(
      "Dasprot id "
          + protocolDasBuilderID
          + " "
          + protocolDasValidatorID
          + " "
          + protocolDasNonValidatorID
          + " "
          + protocolKadID);*/

      generalNode.setProtocol(protocolKadID, kadProt);
      generalNode.setKademliaProtocol(kadProt);
      generalNode.setDASProtocol(dasProt);
      dasProt.setDASProtocolID(protocolDasBuilderID);

      generalNode.setProtocol(protocolDasBuilderID, dasProt);
      generalNode.setProtocol(protocolEvilDasID, null);
      generalNode.setProtocol(protocolDasValidatorID, null);
      generalNode.setProtocol(protocolDasNonValidatorID, null);
    }

    // System.out.println("Validators " + validatorsIds.size());
    // for (DASProtocol validator : validators) {
    for (int i = 0; i < Network.size(); i++) {
      Node generalNode = Network.get(i);
      if (i == 0) generalNode.getDASProtocol().setNonValidators(nonValidatorsIds);
      generalNode.getDASProtocol().addKnownValidator(validatorsIds.toArray(new BigInteger[0]));
    }

    return false;
  }
}
