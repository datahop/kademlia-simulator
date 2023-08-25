package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.HashSet;
import peersim.core.Node;
import peersim.kademlia.Message;
import peersim.kademlia.Util;

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
    /*if (m.body == null) return;

    Sample[] samples = (Sample[]) m.body;
    for (Sample s : samples) {
      logger.warning("Received sample:" + kv.occupancy() + " " + s.getRow() + " " + s.getColumn());

      kv.add((BigInteger) s.getIdByRow(), s);
      kv.add((BigInteger) s.getIdByColumn(), s);
      // count # of samples for each row and column
      column[s.getColumn() - 1]++;
      row[s.getRow() - 1]++;
    }*/
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block non-validator node - getting samples " + this);
    // super.handleInitGetSample(m, myPid);
    BigInteger[] samples = {(BigInteger) m.body};
    BigInteger radius =
        currentBlock.computeRegionRadius(
            KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER, validatorsList.length);
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
    super.handleInitNewBlock(m, myPid);
    logger.warning("Starting random sampling");
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
