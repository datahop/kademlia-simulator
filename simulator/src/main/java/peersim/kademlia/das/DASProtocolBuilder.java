package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.kademlia.Message;
import peersim.kademlia.Util;

public class DASProtocolBuilder extends DASProtocol {

  protected static String prefix = null;

  public DASProtocolBuilder(String prefix) {
    super(prefix);
    DASProtocolBuilder.prefix = prefix;
    isBuilder = true;
    isValidator = false;
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block  builder node - getting samples " + this);
    System.err.println("Wrong eventInit block  builder node - getting samples ");
    System.exit(-1);
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
    logger.warning("Builder new block:" + currentBlock.getBlockId());

    rowSeeding();
    columnSeeding();
  }

  // Generating specific messages to be sent
  protected Message generateSeedSampleMessage(
      Sample[] s, List<BigInteger> validators, boolean isRow) {
    SeedingSampleBody body = new SeedingSampleBody(s, validators, isRow);
    Message m = new Message(Message.MSG_SEED_SAMPLE, body);
    m.timestamp = CommonState.getTime();

    return m;
  }

  private void rowSeeding(){
        // ===============
    // Row Seeding
    // ===============
    int actualRow = 1;
    while (currentBlock.getSize() >= actualRow) {

      Sample[] sampleRow = currentBlock.getSamplesByRow(actualRow); // get all sample of the row
      BigInteger radiusValidator =
          currentBlock.computeRegionRadius(1, searchTable.getValidatorsIndexed().size());

      // Get the id of all validators we need to send the message
      List<BigInteger> idsValidators = new ArrayList<>();
      for (Sample sample : sampleRow) {
        List<BigInteger> ids =
            searchTable.getValidatorNodesbySample(sample.getIdByRow(), radiusValidator);
        if (ids != null && ids.size() > 0) idsValidators.addAll(ids);
      }

      if (idsValidators.size() == 0) {
        actualRow++;
        continue;
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
              + numberValidatorRow
              + " radius "
              + radiusValidator
              + " validators "
              + searchTable.getValidatorsIndexed().size()
              + " row "
              + sampleRow.length);

      // Get size of Parcels to send
      int sizeParcels = 0;
      sizeParcels = (currentBlock.getSize() / numberValidatorRow);
      int redundancyFactor = 1;

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

        logger.warning(
            "Sending row "
                + actualRow
                + " "
                + "parcel to validator "
                + id
                + " samples "
                + validatorParcel.length);
        Node n = Util.nodeIdtoNode(id, kademliaId);
        DASProtocol dasProt = ((DASProtocol) (n.getDASProtocol()));
        if (dasProt.isBuilder()) continue;
        if (n.isUp()) {
          Sample[] samples = validatorParcel;
          Message msg = generateSeedSampleMessage(samples, idsValidators, true);
          msg.operationId = -1;
          msg.src = this.getKademliaProtocol().getKademliaNode();
          msg.dst = n.getKademliaProtocol().getKademliaNode();
          sendMessage(msg, id, dasProt.getDASProtocolID());
          //samplesValidators++;
        }
      }

      //samplesWithinRegion += sampleRow.length;
      actualRow++;
    }
  }

  private void columnSeeding(){
    // ===============
    // Column Seeding
    // ===============
    int actualColumn = 1;
    while (currentBlock.getSize() >= actualColumn) {

      Sample[] sampleColumn= currentBlock.getSamplesByColumn(actualColumn); // get all sample of the column
      BigInteger radiusValidator =
          currentBlock.computeRegionRadius(1, searchTable.getValidatorsIndexed().size());

      // Get the id of all validators we need to send the message
      List<BigInteger> idsValidators = new ArrayList<>();
      for (Sample sample : sampleColumn) {
        List<BigInteger> ids =
            searchTable.getValidatorNodesbySample(sample.getIdByColumn(), radiusValidator);
        if (ids != null && ids.size() > 0) idsValidators.addAll(ids);
      }

      if (idsValidators.size() == 0) {
        actualColumn++;
        continue;
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
              + numberValidatorColumn
              + " radius "
              + radiusValidator
              + " validators "
              + searchTable.getValidatorsIndexed().size()
              + " column "
              + sampleColumn.length);

      // Get size of Parcels to send
      int sizeParcels = 0;
      sizeParcels = (currentBlock.getSize() / numberValidatorColumn);
      int redundancyFactor = 1;

      int indexSampleList = 0;

      for (BigInteger id : idsValidators) {

        // --------------------------
        // Create column Parcels to send
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
        // Send column Parcels
        // ----------------

        logger.warning(
            "Sending column "
                + actualColumn
                + " "
                + "parcel to validator "
                + id
                + " samples "
                + validatorParcel.length);
        Node n = Util.nodeIdtoNode(id, kademliaId);
        DASProtocol dasProt = ((DASProtocol) (n.getDASProtocol()));
        if (dasProt.isBuilder()) continue;
        if (n.isUp()) {
          Sample[] samples = validatorParcel;
          Message msg = generateSeedSampleMessage(samples, idsValidators, true);
          msg.operationId = -1;
          msg.src = this.getKademliaProtocol().getKademliaNode();
          msg.dst = n.getKademliaProtocol().getKademliaNode();
          sendMessage(msg, id, dasProt.getDASProtocolID());
          //samplesValidators++;
        }
      }

      //samplesWithinRegion += sampleRow.length;
      actualColumn++;
    }
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
