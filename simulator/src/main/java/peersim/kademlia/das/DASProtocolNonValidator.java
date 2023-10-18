package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import peersim.core.Node;
import peersim.kademlia.Message;
import peersim.kademlia.Util;

public class DASProtocolNonValidator extends DASProtocol {

  protected static String prefix = null;
  protected List<BigInteger> validatorsContacted;

  public DASProtocolNonValidator(String prefix) {
    super(prefix);
    DASProtocolNonValidator.prefix = prefix;
    isValidator = false;
    isBuilder = false;
    validatorsContacted = new ArrayList<>();
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block non-validator node - getting samples " + this);
    // super.handleInitGetSample(m, myPid);
    if (currentBlock == null) System.err.println("Error block not init yet");
    BigInteger[] samples = (BigInteger[]) m.body;
    BigInteger radius =
        currentBlock.computeRegionRadius(
            KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER,
            searchTable.getValidatorsIndexed().size());
    for (BigInteger sample : samples) {
      // for (BigInteger id : searchTable.getNodesbySample(sample, radius)) {
      for (BigInteger id : searchTable.getValidatorNodesbySample(sample, radius)) {
        if (!validatorsContacted.contains(id)) {
          Message msg = generateGetSampleMessage(samples);
          msg.operationId = -1;
          msg.src = this.kadProtocol.getKademliaNode();
          Node n = Util.nodeIdtoNode(id, kademliaId);
          msg.dst = n.getKademliaProtocol().getKademliaNode();
          sendMessage(msg, id, myPid);
          validatorsContacted.add(id);
        }
      }
    }
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    logger.warning("Init block non-validator node - start sampling " + this);
    super.handleInitNewBlock(m, myPid);
    validatorsContacted.clear();
    startRandomSampling();
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    DASProtocolNonValidator dolly = new DASProtocolNonValidator(DASProtocolNonValidator.prefix);
    return dolly;
  }
}
