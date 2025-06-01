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
import peersim.kademlia.UniformRandomGenerator;
import peersim.kademlia.gossipsub.GossipSubProtocol;

/**
 * This control initializes the whole network (that was already created by peersim) assigning a
 * unique NodeId, randomly generated, to every node.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class CustomDistributionGossipDas implements peersim.core.Control {

  private static final String PAR_PROT_KAD = "protocolkad";
  private static final String PAR_PROT_DAS_BUILDER = "protocoldasbuilder";
  private static final String PAR_PROT_DAS_VALIDATOR = "protocoldasvalidator";
  private static final String PAR_PROT_DAS_NON_VALIDATOR = "protocoldasnonvalidator";
  private static final String PAR_PROT_EVIL_DAS = "protocolEvildas";
  private static final String PAR_PROT_EVIL_VAL_DAS = "protocolEvilValDas";
  private static final String PAR_EVIL_RATIO_VAL = "evilNodeRatioValidator";
  private static final String PAR_EVIL_RATIO_NONVAL = "evilNodeRatioNonValidator";

  private static final String PAR_VALIDATOR_RATE = "validator_rate";
  private static final String PAR_ROWCOL_TOPIC = "colrow_topic";
  /** Protocol identifiers for Kademlia, DAS, etc. * */
  private int protocolKadID;

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

  private GossipTopicMap topicMap;

  public CustomDistributionGossipDas(String prefix) {
    protocolKadID = Configuration.getPid(prefix + "." + PAR_PROT_KAD);
    protocolDasBuilderID = Configuration.getPid(prefix + "." + PAR_PROT_DAS_BUILDER);
    protocolDasValidatorID = Configuration.getPid(prefix + "." + PAR_PROT_DAS_VALIDATOR);
    protocolDasNonValidatorID = Configuration.getPid(prefix + "." + PAR_PROT_DAS_NON_VALIDATOR);
    protocolEvilDasID = Configuration.getPid(prefix + "." + PAR_PROT_EVIL_DAS, 0);
    protocolEvilValDasID = Configuration.getPid(prefix + "." + PAR_PROT_EVIL_VAL_DAS, 0);

    evilRatioValidator = Configuration.getDouble(prefix + "." + PAR_EVIL_RATIO_VAL, 0.0);
    evilRatioNonValidator = Configuration.getDouble(prefix + "." + PAR_EVIL_RATIO_NONVAL, 0.0);
    urg = new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    validatorRate = Configuration.getDouble(prefix + "." + PAR_VALIDATOR_RATE, 1.0);
    int numRowsColsTOpic = Configuration.getInt(prefix + "." + PAR_ROWCOL_TOPIC, 1);
    topicMap = new GossipTopicMap(numRowsColsTOpic);
  }

  public boolean execute() {

    int numValidators = (int) (Network.size() * validatorRate);

    System.out.println(
        "Validators " + numValidators + " " + evilRatioValidator + " " + evilRatioNonValidator);

    int numEvilValidatorNodes = (int) (numValidators * evilRatioValidator);
    int numEvilNonValidatorNodes = (int) ((Network.size() - numValidators) * evilRatioNonValidator);
    System.out.println(
        "Number of malicious nodes: " + numEvilValidatorNodes + " " + numEvilNonValidatorNodes);
    List<BigInteger> validatorsIds = new ArrayList<>();
    List<BigInteger> nonValidatorsIds = new ArrayList<>();
    List<Node> evilNodes = new ArrayList<>();
    List<Node> validators = new ArrayList<>();
    List<BigInteger> evilIds = new ArrayList<>();
    numValidators = numValidators - numEvilValidatorNodes;
    SearchTable searchTable = new SearchTable();

    for (int i = 0; i < Network.size(); ++i) {
      Node generalNode = Network.get(i);
      BigInteger id;
      KademliaNode node;
      id = urg.generate();
      node = new KademliaNode(id, "0.0.0.0", 0);

      GossipSubProtocol gossipProt = null;
      GossipDAS dasProt = null;

      gossipProt = ((GossipSubProtocol) (Network.get(i).getProtocol(protocolKadID)));
      gossipProt.setProtocolID(protocolKadID);
      gossipProt.setNode(node);

      if (i == 0) {
        dasProt = ((GossipDAS) (Network.get(i).getProtocol(protocolDasBuilderID)));
        builderAddress = node.getId();
        validators.add(generalNode);
      } else if ((i > 0) && (i < (numEvilValidatorNodes + 1))) {
        dasProt = ((GossipDAS) (Network.get(i).getProtocol(protocolEvilValDasID)));
        validatorsIds.add(gossipProt.getGossipNode().getId());
        evilNodes.add(generalNode);
        evilIds.add(id);
      } else if ((i > numEvilValidatorNodes)
          && (i < (numEvilValidatorNodes + numEvilNonValidatorNodes + 1))) {
        dasProt = ((GossipDAS) (Network.get(i).getProtocol(protocolEvilDasID)));
        nonValidatorsIds.add(gossipProt.getGossipNode().getId());
        evilNodes.add(generalNode);
        evilIds.add(id);
      } else if (i > (numEvilValidatorNodes + numEvilNonValidatorNodes)
          && i < (numEvilValidatorNodes + numEvilNonValidatorNodes + (numValidators) + 1)) {
        dasProt = ((GossipDAS) (Network.get(i).getProtocol(protocolDasValidatorID)));
        validatorsIds.add(gossipProt.getGossipNode().getId());
      } else {
        dasProt = ((GossipDAS) (Network.get(i).getProtocol(protocolDasNonValidatorID)));
        nonValidatorsIds.add(gossipProt.getGossipNode().getId());
      }

      dasProt.setGossipProtocol(generalNode, gossipProt, topicMap);
      dasProt.setProtocolId(protocolDasBuilderID);
      // gossipProt.setEventsCallback(dasProt);

      if (dasProt instanceof GossipDASBuilder) System.out.println("DASProtocol Builder " + i);
      generalNode.setProtocol(protocolKadID, gossipProt);
      generalNode.setGossipProtocol(gossipProt);
      generalNode.setGossipDASProtocol(dasProt);
      // dasProt.setDASProtocolID(protocolDasBuilderID);

      generalNode.setProtocol(protocolDasBuilderID, dasProt);
      generalNode.setProtocol(protocolEvilDasID, null);
      generalNode.setProtocol(protocolEvilValDasID, null);
      generalNode.setProtocol(protocolDasValidatorID, null);
      generalNode.setProtocol(protocolDasNonValidatorID, null);

      generalNode.getGossipDASProtocol().setSearchTable(searchTable);
      generalNode.getGossipDASProtocol().setBuilderAddress(builderAddress);
    }

    System.out.println("Validators " + validatorsIds.size());
    System.out.println("Non-Validators " + nonValidatorsIds.size());

    searchTable.setBuilderAddress(builderAddress);
    searchTable.addNodes(nonValidatorsIds.toArray(new BigInteger[0]));
    searchTable.addValidatorNodes(validatorsIds.toArray(new BigInteger[0]));
    searchTable.setEvil(evilNodes);
    searchTable.setEvilIds(evilIds);

    KademliaCommonConfigDas.networkSize = Network.size();
    KademliaCommonConfigDas.validatorsSize = numValidators;

    return false;
  }
}
