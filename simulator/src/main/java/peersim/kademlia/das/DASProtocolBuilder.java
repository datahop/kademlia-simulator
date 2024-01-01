package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.Message;
import peersim.kademlia.Util;

public class DASProtocolBuilder extends DASProtocol {

  protected static String prefix = null;
  protected HashMap<BigInteger, List<BigInteger>> samplesToRequest;

  public DASProtocolBuilder(String prefix) {
    super(prefix);
    DASProtocolBuilder.prefix = prefix;
    isBuilder = true;
    isValidator = false;
    samplesToRequest = new HashMap<>();
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block  builder node - getting samples " + this);
    System.err.println("Wrong eventInit block  builder node - getting samples ");
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
    logger.warning("Builder new block:" + currentBlock.getBlockId());

    int samplesWithinRegion = 0; // samples that are within at least one node's region
    int samplesWithinRegionColumn = 0; // samples that are within at least one node's region

    int samplesValidators = 0;
    int samplesNonValidators = 0;
    samplesToRequest.clear();
    BigInteger radiusNonValidator =
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);

    while (currentBlock.hasNext()) {
      boolean inRegion = false;
      Sample s = currentBlock.next();
      // kv.add(s.getId(), s);
      // kv.add(s.getIdByColumn(), s);
      BigInteger radiusValidator =
          currentBlock.computeRegionRadius(
              KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER,
              searchTable.getValidatorsIndexed().size());

      // while (!inRegion) {

      List<BigInteger> idsValidators =
          searchTable.getValidatorNodesbySample(s.getIdByRow(), radiusValidator);
      idsValidators.addAll(
          searchTable.getValidatorNodesbySample(s.getIdByColumn(), radiusValidator));
      for (BigInteger id : idsValidators) {

        logger.info(
            "Sending sample to validator "
                + s.getIdByRow()
                + " "
                + s.getIdByColumn()
                + " to "
                + id);
        Node n = Util.nodeIdtoNode(id, kademliaId);
        DASProtocol dasProt = ((DASProtocol) (n.getDASProtocol()));
        if (dasProt.isBuilder()) continue;
        if (n.isUp()) {

          if (!samplesToRequest.containsKey(id)) {
            List<BigInteger> samples = new ArrayList<>();
            samples.add(s.getId());
            samplesToRequest.put(id, samples);
          } else {
            samplesToRequest.get(id).add(s.getId());
          }
          samplesValidators++;
          if (inRegion == false) {
            samplesWithinRegion++;
            inRegion = true;
          }
        }
      }

      List<BigInteger> idsNonValidators =
          searchTable.getNonValidatorNodesbySample(s.getIdByRow(), radiusNonValidator);
      idsNonValidators.addAll(
          searchTable.getNonValidatorNodesbySample(s.getIdByColumn(), radiusNonValidator));
      for (BigInteger id : idsNonValidators) {
        logger.info(
            "Sending sample to non-validator "
                + s.getIdByRow()
                + " "
                + s.getIdByColumn()
                + " to "
                + id);
        Node n = Util.nodeIdtoNode(id, kademliaId);
        DASProtocol dasProt = ((DASProtocol) (n.getDASProtocol()));
        if (dasProt.isBuilder()) continue;
        if (n.isUp()) {
          samplesNonValidators++;

          if (!dasProt.isValidator()) {

            if (!samplesToRequest.containsKey(id)) {
              List<BigInteger> samples = new ArrayList<>();
              samples.add(s.getId());
              samplesToRequest.put(id, samples);
            } else {
              samplesToRequest.get(id).add(s.getId());
            }
          }
        }
      }
    }

    for (int i = 0; i < Network.size(); i++) {
      Node n = Network.get(i);
      DASProtocol dasProt = n.getDASProtocol();
      BigInteger id = dasProt.getKademliaId();
      if (!dasProt.isBuilder && samplesToRequest.containsKey(id)) {
        EDSimulator.add(
            1,
            generateNewSampleMessage(samplesToRequest.get(id).toArray(new BigInteger[0])),
            n,
            dasProt.getDASProtocolID());
      }
    }

    logger.warning(
        samplesWithinRegion
            + " "
            + samplesWithinRegionColumn
            + " samples out of "
            + currentBlock.getNumSamples()
            + " samples are within a node's region"
            + " "
            + samplesValidators
            + " "
            + samplesNonValidators);
  }

  @Override
  protected void handleGetSampleResponse(Message m, int myPid) {
    logger.warning("Received sample builder node: do nothing");
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    DASProtocolBuilder dolly = new DASProtocolBuilder(DASProtocolBuilder.prefix);
    return dolly;
  }
}
