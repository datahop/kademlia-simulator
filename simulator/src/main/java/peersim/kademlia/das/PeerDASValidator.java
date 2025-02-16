package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.List;
import peersim.core.CommonState;
import peersim.kademlia.KademliaObserver;
import peersim.kademlia.Message;
import peersim.kademlia.das.operations.SamplingOperation;
import peersim.kademlia.das.operations.ValidatorSamplingOperation;

public class PeerDASValidator extends PeerDAS {

  protected boolean started;
  int row, column;

  public PeerDASValidator(String prefix) {
    super(prefix);
    started = false;
    isValidator = true;
    isBuilder = false;
    row = column = 0;
  }

  @Override
  public Object clone() {
    PeerDASValidator dolly = new PeerDASValidator(PeerDASValidator.prefix);
    return dolly;
  }

  protected void handleInitNewBlock(Message m, int myPid) {
    currentBlock = (Block) m.body;
    logger.warning("Validator Init block");

    if (!started) {
      started = true;
      row = CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1;
      String topic = "Row" + row;
      gossipsub.Join(topic);
      column = CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1;
      topic = "Column" + column;
      gossipsub.Join(topic);
    } else {
      createValidatorSamplingOperation(row, 0, CommonState.getTime(), null);
      createValidatorSamplingOperation(0, column, CommonState.getTime(), null);
    }
    super.handleInitNewBlock(m, myPid);
  }

  private void createValidatorSamplingOperation(
      int row, int column, long timestamp, List<BigInteger> validatorList) {
    ValidatorSamplingOperation op =
        new ValidatorSamplingOperation(
            this.getNodeId(),
            timestamp,
            currentBlock,
            searchTable,
            row,
            column,
            this.isValidator,
            KademliaCommonConfigDas.validatorsSize,
            validatorList,
            null);
    long id = 0;
    if (row > 0) id = (long) row;
    else id = (long) column;
    samplingOp.put(id, op);
    logger.warning("Sampling operation started validator " + op.getId());
    /*  List<Sample> samplesFound = new ArrayList<>();
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
    op.elaborateResponse(samplesFound.toArray(new Sample[0]));*/
    // op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    // doSampling(op);
  }

  @Override
  protected void handleGetSample(Message m, int myPid) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'handleGetSample'");
  }

  @Override
  protected void handleGetSampleResponse(Message m, int myPid) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'handleGetSampleResponse'");
  }

  @Override
  public void messageReceived(Message m) {
    // TODO Auto-generated method stub
    // throw new UnsupportedOperationException("Unimplemented method 'messageReceived'");
    Sample s = (Sample) m.value;
    logger.info("Sample received row:" + s.getRow() + " column:" + s.getColumn());
    if (samplingOp.get((long) s.getRow()) != null) {
      SamplingOperation op = samplingOp.get((long) s.getRow());
      Sample[] samples = {s};
      op.elaborateResponse(samples);
      logger.info("Operation found:" + op.getSamples().length);
      if (op.completed()) KademliaObserver.reportOperation(op);
    }
    if (samplingOp.get((long) s.getColumn()) != null) {
      SamplingOperation op = samplingOp.get((long) s.getColumn());
      Sample[] samples = {s};
      op.elaborateResponse(samples);
      logger.info("Operation found:" + op.getSamples().length);
      if (op.completed()) KademliaObserver.reportOperation(op);
    }
  }
}
