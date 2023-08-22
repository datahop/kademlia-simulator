package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.core.CommonState;
import peersim.kademlia.KademliaObserver;
import peersim.kademlia.Message;
import peersim.kademlia.das.operations.SamplingOperation;
import peersim.kademlia.operations.GetOperation;
import peersim.kademlia.operations.Operation;

public class DASDHTProtocol extends DASProtocol {

  protected static String prefix = null;

  public DASDHTProtocol(String prefix) {
    super(prefix);
    DASDHTProtocol.prefix = prefix;
  }

  public Object clone() {
    DASDHTProtocol dolly = new DASDHTProtocol(DASDHTProtocol.prefix);
    return dolly;
  }

  /**
   * Start a topic query opearation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected void handleInitNewBlock(Message m, int myPid) {
    time = CommonState.getTime();
    currentBlock = (Block) m.body;
    kv.erase();
    // samplesRequested = 0;
    row = new int[KademliaCommonConfigDas.BLOCK_DIM_SIZE + 1];
    column = new int[KademliaCommonConfigDas.BLOCK_DIM_SIZE + 1];
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Error. Init block  getting samples dht node. do nothing " + this);
    // super.handleInitGetSample(m, myPid);
  }

  /**
   * Starts the random sampling operation
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  protected void startRandomSampling() {

    /*logger.warning("Starting random sampling");
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
    op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    samplingOp.put(op.getId(), op);
    logger.warning("Sampling operation started random");
    op.setAvailableRequests(KademliaCommonConfigDas.ALPHA);
    doSampling(op);*/
  }

  /**
   * Starts getting rows and columns, only for validators
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  protected void startRowsandColumnsSampling() {
    /*logger.warning(
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
    createValidatorSamplingOperation(rowWithHighestNumSamples(), 0, time);
    createValidatorSamplingOperation(0, columnWithHighestNumSamples(), time);
    createValidatorSamplingOperation(
        CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, 0, time);
    createValidatorSamplingOperation(
        0, CommonState.r.nextInt(KademliaCommonConfigDas.BLOCK_DIM_SIZE) + 1, time);*/
  }

  /**
   * Starts the random sampling operation
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  private void createValidatorSamplingOperation(int row, int column, long timestamp) {

    /*ValidatorSamplingOperationDHT op =
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

    op.elaborateResponse(kv.getAll().toArray(new Sample[0]));
    samplingOp.put(op.getId(), op);
    logger.warning(
        "Sampling operation started validator "
            + op.getId()
            + " "
            + KademliaCommonConfigDas.ALPHA
            + " "
            + timestamp);

    op.setAvailableRequests(KademliaCommonConfigDas.ALPHA);
    doSampling(op);*/
  }

  protected boolean doSampling(SamplingOperation sop) {

    /*if (sop.completed()) {
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

      while (sop.getAvailableRequests() > 0 && sop.getSamples().length > 0) {
        BigInteger[] reqSamples = sop.getSamples();
        BigInteger sample = reqSamples[CommonState.r.nextInt(reqSamples.length)];
        logger.warning("Requesting sample " + sample + " " + reqSamples.length);
        int req = sop.getAvailableRequests() - 1;
        sop.setAvailableRequests(req);
        Message msg = generateGetMessageSample(sample);
        Operation get = this.kadProtocol.handleInit(msg, kademliaId);
        kadOps.put(get, sop);
        success = true;
      }

      return success;
    }*/
    return false;
  }

  @Override
  public void operationComplete(Operation op) {
    logger.warning("Operation complete " + op.getClass().getSimpleName());

    if (op instanceof GetOperation) {
      GetOperation get = (GetOperation) op;
      Sample s = (Sample) get.getValue();
      SamplingOperation sop = kadOps.get(get);
      logger.warning("Get operation DASDHT " + s + " " + sop);
      kadOps.remove(get);
      if (sop != null && s != null && !sop.completed()) {
        Sample[] samples = {s};
        sop.elaborateResponse(samples);
        sop.addHops(get.getHops());
        for (Long msg : get.getMessages()) {
          sop.addMessage(msg);
        }
        logger.warning(
            "Get operation completed "
                + s.getId()
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

        } else doSampling(sop);
      }
    }
  }

  @Override
  public void putValueReceived(Object o) {
    Parcel p = (Parcel) o;
    logger.warning("Parcel received put operation " + p.getId());
  }

  @Override
  public void nodesFound(Operation op, BigInteger[] neighbours) {}

  @Override
  public void missing(BigInteger sample, Operation op) {}

  // ______________________________________________________________________________________________
  /**
   * Generates a PUT message for t1 key and string message
   *
   * @return Message
   */
  protected Message generatePutMessageSample(Parcel p) {

    // Existing active destination node
    Message m = Message.makeInitPutValue(p.getId(), p);
    m.timestamp = CommonState.getTime();

    return m;
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
}
