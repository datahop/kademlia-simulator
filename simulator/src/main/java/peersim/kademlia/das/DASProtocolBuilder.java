package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import peersim.core.Node;
import peersim.kademlia.Message;
import peersim.kademlia.Util;

// DAS Protocol process functions executed only by builder. It basically seeds validators every
// block.
public class DASProtocolBuilder extends DASProtocol {

  protected static String prefix = null;

  public DASProtocolBuilder(String prefix) {
    super(prefix);
    DASProtocolBuilder.prefix = prefix;
    isBuilder = true;
    isValidator = false;
  }

  @Override
  protected void handleGetSample(Message m, int myPid) {
    /** Ignore sample request * */
    logger.warning("Builder handle get sample - return nothing " + this);
  }

  @Override
  protected void handleSeedSample(Message m, int myPid) {
    System.err.println("Builder should not receive seed sample");
    System.exit(-1);
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
    logger.warning("Builder new block:" + currentBlock.getBlockId());

    int samplesWithinRegion = 0; // samples that are within at least one node's region
    int samplesValidators = 0;
    int samplesNonValidators = 0;

    BigInteger radiusNonValidator =
        currentBlock.computeRegionRadius(KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER);

    int actualRow = 0;
    while (currentBlock.getSize()>0) {
      boolean inRegion = false;
      Sample[] sampleRow = currentBlock.getSamplesByRow(actualRow); //get all sample of the row

      BigInteger radiusValidator =
          currentBlock.computeRegionRadius(
              1,
              searchTable.getValidatorsIndexed().size());


      //Get the id of all validators we need to send the message
      List<BigInteger> idsValidators = new ArrayList<>();
      for (Sample sample : sampleRow){
        idsValidators.addAll(searchTable.getValidatorNodesbySample(sample.getIdByRow(), radiusValidator));
      }

      //remove duplicate
      Set<BigInteger> set = new HashSet<>(idsValidators);
      idsValidators = new ArrayList<>(set);
      //==================================================


      int numberValidatorRow = idsValidators.size();  //Get the number of validators
      logger.warning(
        "Number of Validator for this row is: "
        + numberValidatorRow);
      
      //Get size of Parcels to send 
      int sizeParcels = (currentBlock.getSize() / numberValidatorRow)*KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER;

      if (currentBlock.getSize() / numberValidatorRow != 0) {
        sizeParcels += KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER;
      }

      //==================================================
      //A faire Créer le parcel pour chaque validator en prenant en compte le nombre de copie dont on a besoin

      int indexSampleList = 0;
      for (BigInteger id : idsValidators) {

        Sample[] validatorParcel = new Sample[0];
        int k = 0;
        while(validatorParcel.length!=sizeParcels) {
          Sample s = sampleRow[indexSampleList%sampleRow.length];
          validatorParcel[k] = s;
          indexSampleList++;
          k++;
        }

        //==================================================

          logger.warning(
              "Sending row "
                  + actualRow
                  + " "
                  + "parcel to validator "
                  + id);
          Node n = Util.nodeIdtoNode(id, kademliaId);
          DASProtocol dasProt = ((DASProtocol) (n.getDASProtocol()));
          if (dasProt.isBuilder()) continue;
          if (n.isUp()) {
            Sample[] samples = validatorParcel;
            Message msg = generateSeedSampleMessage(samples);
            msg.operationId = -1;
            msg.src = this.getKademliaProtocol().getKademliaNode();
            msg.dst = n.getKademliaProtocol().getKademliaNode();
            sendMessage(msg, id, dasProt.getDASProtocolID());
            samplesValidators++;
            if (inRegion == false) {
              samplesWithinRegion++;
              inRegion = true;
            }
          }
        }
        if (!inRegion) radiusValidator = radiusValidator.multiply(BigInteger.valueOf(2));
      }

      actualRow ++;

    logger.warning(
        samplesWithinRegion
            + " samples out of "
            + currentBlock.getNumSamples()
            + " samples are within a node's region"
            + " "
            + samplesValidators
            + " "
            + samplesNonValidators);
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Error. Init block builder node - getting samples. do nothing " + this);
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
