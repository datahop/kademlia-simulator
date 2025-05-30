package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import peersim.config.Configuration;
import peersim.core.Node;
import peersim.kademlia.Message;
import peersim.kademlia.Util;

// DAS Protocol process functions executed only by builder. It basically seeds validators every
// block.
public class DASProtocolBuilder extends DASProtocol {

  protected static final String PAR_BUILDER = "builderStrategy";
  protected static final String PAR_REDUNDANCY = "builderRedundancy";
  protected static final String PAR_ROWCOLUMNY = "validatorRowColumn";

  protected static String prefix = null;

  public DASProtocolBuilder(String prefix) {
    super(prefix);
    DASProtocolBuilder.prefix = prefix;
    KademliaCommonConfigDas.builderStrategy =
        Configuration.getInt(prefix + "." + PAR_BUILDER, KademliaCommonConfigDas.builderStrategy);

    KademliaCommonConfigDas.builderRedundancy =
        Configuration.getInt(
            prefix + "." + PAR_REDUNDANCY, KademliaCommonConfigDas.builderRedundancy);

    KademliaCommonConfigDas.validatorRowColumn =
        Configuration.getInt(
            prefix + "." + PAR_ROWCOLUMNY, KademliaCommonConfigDas.validatorRowColumn);

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

    // ===============
    // Row Sampling
    // ===============
    int actualRow = 1;
    while (currentBlock.getSize() >= actualRow) {
      boolean inRegion = false;
      Sample[] sampleRow;
      logger.warning("Builder strategy:" + KademliaCommonConfigDas.builderStrategy);
      if (KademliaCommonConfigDas.builderStrategy == 1) {
        sampleRow =
            currentBlock.getNSamplesByRow(
                actualRow,
                KademliaCommonConfigDas.BLOCK_DIM_SIZE / 2); // get half ofall sample of the row
      } else {
        sampleRow = currentBlock.getSamplesByRow(actualRow); // get all sample of the row
      }

      BigInteger radiusValidator =
          currentBlock.computeRegionRadius(
              KademliaCommonConfigDas.validatorRowColumn,
              searchTable.getValidatorsIndexed().size());

      // Get the id of all validators we need to send the message
      List<BigInteger> idsValidators = new ArrayList<>();
      for (Sample sample : sampleRow) {
        idsValidators.addAll(
            searchTable.getValidatorNodesbySample(sample.getIdByRow(), radiusValidator));
      }

      // remove duplicate
      Set<BigInteger> set = new HashSet<>(idsValidators);
      idsValidators = new ArrayList<>(set);

      int numberValidatorRow = idsValidators.size(); // Get the number of validators
      logger.warning(
          "Block "
              + currentBlock.getBlockId()
              + " Number of Validator for row"
              + actualRow
              + " is: "
              + numberValidatorRow);

      // Get size of Parcels to send
      int sizeParcels = 0;
      sizeParcels = (currentBlock.getSize() / numberValidatorRow);
      int redundancyFactor = 1;
      if (KademliaCommonConfigDas.builderStrategy == 2) {
        redundancyFactor = KademliaCommonConfigDas.builderRedundancy;
      }

      int indexSampleList = 0;
      for (BigInteger id : idsValidators) {

        // --------------------------
        // Create Row Parcels to send
        // --------------------------

        Sample[] validatorParcel = new Sample[sizeParcels * redundancyFactor];
        int k = 0;
        while (k != sizeParcels * redundancyFactor) {
          Sample s = sampleRow[indexSampleList % sampleRow.length];
          validatorParcel[k] = s;
          indexSampleList++;
          k++;
        }

        // ----------------
        // Send Row Parcels
        // ----------------

        logger.warning("Sending row " + actualRow + " " + "parcel to validator " + id);
        Node n = Util.nodeIdtoNode(id, kademliaId);
        DASProtocol dasProt = ((DASProtocol) (n.getDASProtocol()));
        if (dasProt.isBuilder()) continue;
        if (n.isUp()) {
          Sample[] samples = validatorParcel;
          Message msg = generateSeedSampleMessage(samples, idsValidators, true);
          msg.operationId = -1;
          msg.src = this.getKademliaProtocol().getKademliaNode();
          msg.dst = n.getKademliaProtocol().getKademliaNode();
          sendMessage(msg, id, dasProt.getDASProtocolID(), 0);
          samplesValidators++;
          if (inRegion == false) {
            samplesWithinRegion++;
            inRegion = true;
          }
        }
      }
      if (!inRegion) radiusValidator = radiusValidator.multiply(BigInteger.valueOf(2));
      actualRow++;
    }

    int actualColumn = 1;

    while (currentBlock.getSize() >= actualColumn) {
      boolean inRegion = false;
      Sample[] sampleColumn;
      if (KademliaCommonConfigDas.builderStrategy == 0) {
        sampleColumn = currentBlock.getSamplesByColumn(actualColumn); // get all sample of the row
      } else if (KademliaCommonConfigDas.builderStrategy == 1) {
        sampleColumn =
            currentBlock.getNSamplesByColumn(
                actualColumn,
                KademliaCommonConfigDas.BLOCK_DIM_SIZE / 2); // get all sample of the row
      } else {
        sampleColumn = currentBlock.getSamplesByColumn(actualColumn);
      }

      BigInteger radiusValidator =
          currentBlock.computeRegionRadius(
              KademliaCommonConfigDas.validatorRowColumn,
              searchTable.getValidatorsIndexed().size());

      // Get the id of all validators we need to send the message
      List<BigInteger> idsValidators = new ArrayList<>();
      for (Sample sample : sampleColumn) {
        idsValidators.addAll(
            searchTable.getValidatorNodesbySample(sample.getIdByColumn(), radiusValidator));
      }

      // remove duplicate
      Set<BigInteger> set = new HashSet<>(idsValidators);
      idsValidators = new ArrayList<>(set);

      int numberValidatorColumn = idsValidators.size(); // Get the number of validators
      logger.warning(
          "Block "
              + currentBlock.getBlockId()
              + " Number of Validator for column"
              + actualColumn
              + " is: "
              + numberValidatorColumn);

      // Get size of Parcels to send
      int sizeParcels = 0;
      sizeParcels = (currentBlock.getSize() / numberValidatorColumn);
      int redundancyFactor = 1;
      if (KademliaCommonConfigDas.builderStrategy == 2) {
        redundancyFactor = KademliaCommonConfigDas.builderRedundancy;
      }

      int indexSampleList = 0;
      for (BigInteger id : idsValidators) {

        // --------------------------
        // Create Row Parcels to send
        // --------------------------

        Sample[] validatorParcel = new Sample[sizeParcels * redundancyFactor];
        int k = 0;
        while (k != sizeParcels * redundancyFactor) {
          Sample s = sampleColumn[indexSampleList % sampleColumn.length];
          validatorParcel[k] = s;
          indexSampleList++;
          k++;
        }

        // ----------------
        // Send Row Parcels
        // ----------------

        logger.warning("Sending column " + actualColumn + " " + "parcel to validator " + id);
        Node n = Util.nodeIdtoNode(id, kademliaId);
        DASProtocol dasProt = ((DASProtocol) (n.getDASProtocol()));
        if (dasProt.isBuilder()) continue;
        if (n.isUp()) {
          Sample[] samples = validatorParcel;
          Message msg = generateSeedSampleMessage(samples, idsValidators, false);
          msg.operationId = -1;
          msg.src = this.getKademliaProtocol().getKademliaNode();
          msg.dst = n.getKademliaProtocol().getKademliaNode();
          sendMessage(msg, id, dasProt.getDASProtocolID(), 0);
          samplesValidators++;
          if (inRegion == false) {
            samplesWithinRegion++;
            inRegion = true;
          }
        }
      }
      if (!inRegion) radiusValidator = radiusValidator.multiply(BigInteger.valueOf(2));
      actualColumn++;
    }

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
