package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.core.CommonState;
import peersim.kademlia.KademliaObserver;
import peersim.kademlia.Message;
import peersim.kademlia.das.operations.RandomSamplingOperationDHT;
import peersim.kademlia.das.operations.SamplingOperation;
import peersim.kademlia.das.operations.ValidatorSamplingOperationDHT;
import peersim.kademlia.operations.GetOperation;
import peersim.kademlia.operations.Operation;

public class DASDHTProtocolValidator extends DASDHTProtocol {

  protected static String prefix = null;

  public DASDHTProtocolValidator(String prefix) {
    super(prefix);
    DASDHTProtocolValidator.prefix = prefix;
    isValidator = true;
    isBuilder = false;
  }

  @Override
  protected void handleSeedSample(Message m, int myPid) {
    logger.warning("seed sample receveived");
    if (m.body == null) return;
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block validator node - getting samples " + this);
    // super.handleInitGetSample(m, myPid);
  }

  @Override
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);

    // logger.warning("Starting validator (rows and columns) sampling");
    startRowsandColumnsSampling();
    // logger.warning("Starting random sampling");
    startRandomSampling();
  }

  /**
   * Starts getting rows and columns, only for validators
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  protected void startRowsandColumnsSampling() {

    // start 2 row 2 column Validator operation (1 row/column with the highest number of samples
    // already downloaded and another random)
    createValidatorSamplingOperation(
        CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, 0, time);
    createValidatorSamplingOperation(
        0, CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, time);
    createValidatorSamplingOperation(
        CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, 0, time);
    createValidatorSamplingOperation(
        0, CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, time);
  }

  private void createValidatorSamplingOperation(int row, int column, long timestamp) {
    ValidatorSamplingOperationDHT op =
        new ValidatorSamplingOperationDHT(
            this.getKademliaId(),
            timestamp,
            currentBlock,
            searchTable,
            row,
            column,
            this.isValidator,
            validatorsList.length,
            this);

    op.elaborateResponse(this.kadProtocol.kv.getAll().toArray(new Sample[0]));
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started validator " + op.getId() + " " + row + " " + column);

    // op.setAvailableRequests(KademliaCommonConfigDas.ALPHA);
    op.setAvailableRequests(KademliaCommonConfigDas.ALPHA);
    doSampling(op);
  }

  protected boolean doSampling(SamplingOperation sop) {

    if (sop.completed()) {
      samplingOp.remove(sop.getId());
      KademliaObserver.reportOperation(sop);
      logger.warning(
          "Sampling operation finished dosampling "
              + sop.getId()
              + " "
              + CommonState.getTime()
              + " "
              + sop.getTimestamp());

      return true;
    } else {
      boolean success = false;
      logger.warning("Dosampling " + sop.getAvailableRequests());

      if (sop instanceof ValidatorSamplingOperationDHT) {
        ValidatorSamplingOperationDHT vop = (ValidatorSamplingOperationDHT) sop;
        for (BigInteger parcel : vop.getParcels()) {
          logger.warning("Sending parcel request " + parcel);
          Message msg = generateGetMessageSample(parcel);
          Operation get = this.kadProtocol.handleInit(msg, kademliaId);
          kadOps.put(get, sop);
          success = true;
        }
      }

      if (sop instanceof RandomSamplingOperationDHT) {
        RandomSamplingOperationDHT rop = (RandomSamplingOperationDHT) sop;
        for (BigInteger parcel : rop.getParcels()) {
          logger.warning("Sending parcel request " + parcel);
          Message msg = generateGetMessageSample(parcel);
          Operation get = this.kadProtocol.handleInit(msg, kademliaId);
          kadOps.put(get, sop);
          success = true;
        }
      }
      return success;
    }
  }

  // ______________________________________________________________________________________________
  /**
   * Generates a PUT message for t1 key and string message
   *
   * @return Message
   */
  private Message generateGetMessageSample(BigInteger s) {

    // Existing active destination node
    Message m = Message.makeInitGetValue(s);
    m.timestamp = CommonState.getTime();

    return m;
  }

  /**
   * Starts the random sampling operation
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  protected void startRandomSampling() {

    logger.warning("Starting random sampling");
    SamplingOperation op =
        new RandomSamplingOperationDHT(
            this.getKademliaId(),
            null,
            time,
            currentBlock,
            searchTable,
            this.isValidator,
            validatorsList.length,
            this);
    op.elaborateResponse(this.kadProtocol.kv.getAll().toArray(new Sample[0]));
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started random");
    op.setAvailableRequests(KademliaCommonConfigDas.ALPHA);
    doSampling(op);
  }

  @Override
  public void nodesFound(Operation op, BigInteger[] neighbours) {}

  @Override
  public void missing(BigInteger sample, Operation op) {}

  /*@Override
  public void putValueReceived(Object o) {
    Sample s = (Sample) o;
    logger.warning("Sample received put operation " + s.getId());

    column[s.getColumn() - 1]++;
    row[s.getRow() - 1]++;

    if (!samplingStarted) {
      logger.warning("Starting validator (rows and columns) sampling");
      startRowsandColumnsSampling();
      startRandomSampling();
      samplingStarted = true;
    }
  }*/

  @Override
  public void operationComplete(Operation op) {
    logger.warning("Operation complete " + op.getClass().getSimpleName());

    if (op instanceof GetOperation) {
      GetOperation get = (GetOperation) op;
      Parcel p = (Parcel) get.getValue();
      SamplingOperation sop = kadOps.get(get);
      kadOps.remove(get);
      logger.warning("Operation " + sop.getId());

      if (sop instanceof ValidatorSamplingOperationDHT && p == null) {
        BigInteger parcelId = (BigInteger) get.getBody();
        logger.warning("Sending parcel request " + parcelId);
        Message msg = generateGetMessageSample(parcelId);
        Operation gop = this.kadProtocol.handleInit(msg, kademliaId);
        kadOps.put(gop, sop);
      } else if (sop instanceof ValidatorSamplingOperationDHT && p != null) {
        ValidatorSamplingOperationDHT vop = (ValidatorSamplingOperationDHT) sop;
        vop.receivedParcel(p.getId());
        if (sop != null && !sop.completed()) {
          logger.warning("Get operation DASDHT " + p.getId() + " " + sop.getId());

          sop.elaborateResponse(p.getSamples());
          sop.addHops(get.getHops());
          for (Long msg : get.getMessages()) {
            sop.addMessage(msg);
          }
          logger.warning(
              "Get operation completed "
                  + p.getId()
                  + " found "
                  + sop.samplesCount()
                  + " "
                  + sop.completed());

          if (sop.completed()) {
            KademliaObserver.reportOperation(sop);
            logger.warning(
                "Sampling operation finished operationComplete "
                    + sop.getId()
                    + " "
                    + CommonState.getTime()
                    + " "
                    + sop.getTimestamp());
          } // else doSampling(sop);
        }
      }

      if (sop instanceof RandomSamplingOperationDHT && p == null) {
        BigInteger parcelId = (BigInteger) get.getBody();
        logger.warning("Sending parcel request " + parcelId);
        Message msg = generateGetMessageSample(parcelId);
        Operation gop = this.kadProtocol.handleInit(msg, kademliaId);
        kadOps.put(gop, sop);
      } else if (sop instanceof RandomSamplingOperationDHT && p != null) {
        RandomSamplingOperationDHT vop = (RandomSamplingOperationDHT) sop;
        vop.receivedParcel(p.getId());
        if (sop != null && !sop.completed()) {
          logger.warning("Get operation DASDHT " + p.getId() + " " + sop.getId());

          sop.elaborateResponse(p.getSamples());
          sop.addHops(get.getHops());
          for (Long msg : get.getMessages()) {
            sop.addMessage(msg);
          }
          logger.warning(
              "Get operation completed "
                  + p.getId()
                  + " found "
                  + sop.samplesCount()
                  + " "
                  + sop.completed());

          if (sop.completed()) {
            KademliaObserver.reportOperation(sop);
            logger.warning(
                "Sampling operation finished operationComplete "
                    + sop.getId()
                    + " "
                    + CommonState.getTime()
                    + " "
                    + sop.getTimestamp());
          } // else doSampling(sop);
        }
      }
    }
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    DASDHTProtocolValidator dolly = new DASDHTProtocolValidator(DASDHTProtocolValidator.prefix);
    return dolly;
  }
}
