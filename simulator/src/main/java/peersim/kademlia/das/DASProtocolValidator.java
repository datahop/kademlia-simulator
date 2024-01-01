package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.kademlia.Message;
import peersim.kademlia.Util;
import peersim.kademlia.das.operations.ValidatorSamplingOperation;

public class DASProtocolValidator extends DASProtocol {

  protected static String prefix = null;

  public DASProtocolValidator(String prefix) {
    super(prefix);
    DASProtocolValidator.prefix = prefix;
    isValidator = true;
    isBuilder = false;
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
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
    if (!isEvil) {
      startRowsandColumnsSampling();
      // startRandomSampling();
    }
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
    createValidatorSamplingOperation(
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
        time);
    createValidatorSamplingOperation(
        CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, 0, time);
    createValidatorSamplingOperation(
        0, CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, time);
  }

  private void createValidatorSamplingOperation(int row, int column, long timestamp) {
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
            this);
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started validator " + op.getId());

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
