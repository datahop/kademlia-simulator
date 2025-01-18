package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import peersim.core.Node;
import peersim.kademlia.Message;
import peersim.kademlia.Util;
import peersim.kademlia.das.operations.ValidatorSamplingOperation;

public class DASProtocolValidator extends DASProtocol {

  protected static String prefix = null;
  protected boolean started;

  public DASProtocolValidator(String prefix) {
    super(prefix);
    DASProtocolValidator.prefix = prefix;
    isValidator = true;
    isBuilder = false;
    started = false;
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    if (!init) return;
    logger.warning("Init block validator node - getting samples " + this);
    if (currentBlock == null) System.err.println("Error block not init yet");
    BigInteger[] samples = (BigInteger[]) m.body;

    Message msg = generateGetSampleMessage(samples);
    msg.operationId = -1;
    msg.src = this.kadProtocol.getKademliaNode();
    Node n = Util.nodeIdtoNode(builderAddress, kademliaId);
    msg.dst = n.getKademliaProtocol().getKademliaNode();
    sendMessage(msg, builderAddress, myPid);
  }

  @Override
  protected void handleSeedSample(Message m, int myPid) {

    SeedingSampleBody body = (SeedingSampleBody) m.body;
    Sample[] samples = (Sample[]) body.getsamplesList();
    logger.warning("Seed received " + samples.length + " samples.");
    for (Sample s : samples) {
      logger.warning(
          "Sample received "
              + s.getId()
              + " "
              + s.getIdByColumn()
              + " from "
              + m.src.getId()
              + " "
              + m.id);

      kv.add((BigInteger) s.getIdByRow());
      // kv.add((BigInteger) s.getIdByRow(), s);
      // kv.add((BigInteger) s.getIdByColumn(), s);
      // count # of samples for each row and column and reconstruct if more than half received
      reconstruct(s);
    }
    List<BigInteger> validatorList = body.getValidators();
    boolean isRow = body.getIsRow();
    if (isRow) {
      createValidatorSamplingOperation(samples[0].getRow(), 0, time, validatorList);
    } else {
      createValidatorSamplingOperation(0, samples[0].getColumn(), time, validatorList);
    }
    /*if (!started) {
      started = true;
      startRowsandColumnsSampling();
    }*/
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
    started = false;
    /*if (!isEvil) {
      startRowsandColumnsSampling();
      startRandomSampling();
    }*/
  }

  /**
   * Starts getting rows and columns, only for validators
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  protected void startRowsandColumnsSampling() {
    logger.warning(
        "Starting rows and columns fetch "
            + rowWithHighestNumSamples()
            + " "
            + row[rowWithHighestNumSamples()]
            + " "
            + columnWithHighestNumSamples()
            + " "
            + column[columnWithHighestNumSamples()]);

    // start 2 row 2 column Validator operation (1 row/column with the highest number of samples
    // already downloaded and another random)
    /*createValidatorSamplingOperation(
        currentBlock.findClosestRow(
            this.getKademliaId(),
            currentBlock.computeRegionRadius(
                KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER,
                KademliaCommonConfigDas.validatorsSize)),
        0,
        time);
    createValidatorSamplingOperation(
        0,
        currentBlock.findClosestColumn(
            this.getKademliaId(),
            currentBlock.computeRegionRadius(
                KademliaCommonConfigDas.NUM_SAMPLE_COPIES_PER_PEER,
                KademliaCommonConfigDas.validatorsSize)),
        time);*/
    // createValidatorSamplingOperation(
    //     CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, 0, time);
    // createValidatorSamplingOperation(
    //     0, CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, time);
    // createValidatorSamplingOperation(
    //     CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, 0, time);
    //  createValidatorSamplingOperation(
    //      0, CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, time);

    /*int row = searchTable.getValidatorRow(this.getKademliaId());
    if (row == 0) {
      row = CommonState.r.nextInt(currentBlock.getSize()) + 1;
    }
    int column = searchTable.getValidatorColumn(this.getKademliaId());
    if (column == 0) {
      column = CommonState.r.nextInt(currentBlock.getSize()) + 1;
    }

    createValidatorSamplingOperation(row, 0, time);*/
    // createValidatorSamplingOperation(0, column, time);
  }

  private void createValidatorSamplingOperation(
      int row, int column, long timestamp, List<BigInteger> validatorList) {
    ValidatorSamplingOperation op =
        new ValidatorSamplingOperation(
            this.getKademliaId(),
            timestamp,
            currentBlock,
            searchTable,
            row,
            column,
            this.isValidator,
            KademliaCommonConfigDas.validatorsSize,
            validatorList,
            this);
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started validator " + op.getId());

    List<Sample> samplesFound = new ArrayList<>();
    if (row > 0) {
      Sample[] samples = currentBlock.getSamplesByRow(row);
      for (Sample s : samples) {
        if (kv.contains(s.getIdByRow())) {
          samplesFound.add(s);
        }
      }
    } else {
      Sample[] samples = currentBlock.getSamplesByColumn(column);
      for (Sample s : samples) {
        if (kv.contains(s.getIdByColumn())) {
          samplesFound.add(s);
        }
      }
    }
    op.elaborateResponse(samplesFound.toArray(new Sample[0]));
    // op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    doSampling(op);
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    DASProtocolValidator dolly = new DASProtocolValidator(DASProtocolValidator.prefix);
    return dolly;
  }
}
