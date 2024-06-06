package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.HashSet;
import peersim.core.Node;
import peersim.kademlia.Message;
import peersim.kademlia.Util;

//DAS Protocol process functions executed only by regular-nodes. It gets samples from validators (init seeding), and starts random sampling, for every block.
public class DASProtocolNonValidator extends DASProtocol {

  protected static String prefix = null;

  protected HashSet<BigInteger> reqSamples;

  public DASProtocolNonValidator(String prefix) {
    super(prefix);
    DASProtocolNonValidator.prefix = prefix;
    isValidator = false;
    isBuilder = false;
    reqSamples = new HashSet<>();
  }

  @Override
  protected void handleSeedSample(Message m, int myPid) {
    logger.warning("Non-validator should not receive seed sample");
    System.exit(-1);

  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block non-validator node - getting samples " + this);
    BigInteger[] samples = {(BigInteger) m.body};
    BigInteger radius =
        currentBlock.computeRegionRadius(
            KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER,
            searchTable.getValidatorsIndexed().size());
    for (BigInteger sample : samples) {
      if (!reqSamples.contains(sample)) {
        for (BigInteger id : searchTable.getValidatorNodesbySample(sample, radius)) {
          Message msg = generateGetSampleMessage(samples);
          msg.operationId = -1;
          msg.src = this.kadProtocol.getKademliaNode();
          Node n = Util.nodeIdtoNode(id, kademliaId);
          msg.dst = n.getKademliaProtocol().getKademliaNode();
          sendMessage(msg, id, myPid);
          reqSamples.add(sample);
        }
      }
    }
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    logger.warning("Init block non-validator node - start sampling " + this);
    super.handleInitNewBlock(m, myPid);
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
