package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.core.CommonState;
import peersim.kademlia.KademliaObserver;
import peersim.kademlia.Message;
import peersim.kademlia.das.operations.RandomSamplingOperationDHT;
import peersim.kademlia.das.operations.SamplingOperation;
import peersim.kademlia.operations.GetOperation;
import peersim.kademlia.operations.Operation;

public class DASDHTProtocolNonValidator extends DASDHTProtocol {

  protected static String prefix = null;

  public DASDHTProtocolNonValidator(String prefix) {
    super(prefix);
    DASDHTProtocolNonValidator.prefix = prefix;
  }

  public Object clone() {
    DASDHTProtocolNonValidator dolly =
        new DASDHTProtocolNonValidator(DASDHTProtocolNonValidator.prefix);
    return dolly;
  }

  /**
   * Start a topic query opearation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected void handleInitNewBlock(Message m, int myPid) {
    super.handleInitNewBlock(m, myPid);
    logger.warning("non-validator new block:" + currentBlock.getBlockId());
    startRandomSampling();
  }

  @Override
  protected void handleInitGetSample(Message m, int myPid) {
    logger.warning("Init block non-validato node - getting samples " + this);
    // super.handleInitGetSample(m, myPid);
  }

  @Override
  protected void handleGetSampleResponse(Message m, int myPid) {
    logger.warning("non-validato Received sample : do nothing");
  }

  @Override
  protected void handleGetSample(Message m, int myPid) {
    /** Ignore sample request * */
    logger.warning("non-validator handle get sample - return nothing " + this);
  }

  @Override
  protected void handleSeedSample(Message m, int myPid) {
    System.err.println("non-validator should not receive seed sample");
    System.exit(-1);
  }

  /**
   * Starts the random sampling operation
   *
   * @param m initial message
   * @param myPid protocol pid
   */
  protected void startRandomSampling() {

    logger.warning("non-validator Starting random sampling");
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
    logger.warning("non-validator Sampling operation started random");
    op.setAvailableRequests(KademliaCommonConfigDas.ALPHA);
    doSampling(op);
  }

  protected boolean doSampling(SamplingOperation sop) {

    if (sop.completed()) {
      samplingOp.remove(sop.getId());
      KademliaObserver.reportOperation(sop);
      logger.warning(
          " non-validator Sampling operation finished dosampling "
              + sop.getId()
              + " "
              + CommonState.getTime()
              + " "
              + sop.getTimestamp());

      return true;
    } else {
      boolean success = false;
      logger.warning("non-validator Dosampling " + sop.getAvailableRequests());

      for (BigInteger sample : sop.getSamples()) {
        Message msg = generateGetMessageSample(sample);
        Operation get = this.kadProtocol.handleInit(msg, kademliaId);
        kadOps.put(get, sop);
        success = true;
      }
      return success;
    }
  }

  @Override
  public void operationComplete(Operation op) {
    logger.warning("non-validator Operation complete " + op.getClass().getSimpleName());

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
            "non-validator Get operation completed "
                + s.getId()
                + " found "
                + sop.samplesCount()
                + " "
                + sop.completed());

        if (sop.completed()) {
          KademliaObserver.reportOperation(sop);
          logger.warning(
              "non-validator Sampling operation finished operationComplete "
                  + sop.getId()
                  + " "
                  + CommonState.getTime()
                  + " "
                  + sop.getTimestamp());
        } // else doSampling(sop);
      }
    }
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
  private Message generateGetMessageSample(BigInteger s) {

    // Existing active destination node
    Message m = Message.makeInitGetValue(s);
    m.timestamp = CommonState.getTime();

    return m;
  }
}
