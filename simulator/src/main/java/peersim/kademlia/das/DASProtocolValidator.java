package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.List;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.kademlia.Message;
import peersim.kademlia.das.operations.ValidatorSamplingOperation;

// DAS Protocol process functions executed only by validators. It stores samples received by the
// builder, and it starts random sampling and row/column fetching every block.
public class DASProtocolValidator extends DASProtocol {

  protected static final String PAR_VALIDATOR = "validatorStrategy";

  protected static String prefix = null;

  public DASProtocolValidator(String prefix) {
    super(prefix);
    DASProtocolValidator.prefix = prefix;
    KademliaCommonConfigDas.validatorStrategy =
        Configuration.getInt(
            prefix + "." + PAR_VALIDATOR, KademliaCommonConfigDas.validatorStrategy);

    if (KademliaCommonConfigDas.validatorStrategy == 0) {
      KademliaCommonConfigDas.random_sampling_aggressiveness_step = Network.size();
    } else if (KademliaCommonConfigDas.validatorStrategy == 1) {
      KademliaCommonConfigDas.random_sampling_aggressiveness_step =
          KademliaCommonConfigDas.N_SAMPLES;
    }

    isValidator = true;
    isBuilder = false;
  }

  @Override
  protected void handleSeedSample(Message m, int myPid) {
    logger.warning("seed sample received");
    if (m.body == null) return;

    SeedingSampleBody body = (SeedingSampleBody) m.body;
    Sample[] samples = (Sample[]) body.getsamplesList();
    List<BigInteger> validatorList = body.getValidators();
    boolean isRow = body.getIsRow();
    for (Sample s : samples) {

      logger.warning(
          "Received sample:"
              + kv.occupancy()
              + " "
              + s.getRow()
              + " "
              + s.getColumn()
              + " "
              + s.getIdByRow()
              + " "
              + s.getIdByColumn());

      kv.add((BigInteger) s.getIdByRow(), s);
      kv.add((BigInteger) s.getIdByColumn(), s);
      // count # of samples for each row and column
      reconstruct(s);
      column[s.getColumn() - 1]++;
      row[s.getRow() - 1]++;
    }
    if (isRow) {
      createValidatorSamplingOperation(samples[0].getRow(), 0, time, validatorList);
    } else {
      createValidatorSamplingOperation(0, samples[0].getColumn(), time, validatorList);
    }
    startRowsandColumnsSampling();
    startRandomSampling();
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Error. Init block validator node - getting samples. do nothing " + this);
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
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

    op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    // doRowColumnSampling(op);
    // doRandomSampling(op);
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
