package peersim.kademlia.das;

import java.math.BigInteger;
import peersim.kademlia.Message;
import peersim.kademlia.operations.Operation;

public class DASDHTProtocolBuilder extends DASDHTProtocol {

  protected static String prefix = null;

  public DASDHTProtocolBuilder(String prefix) {
    super(prefix);
    DASDHTProtocolBuilder.prefix = prefix;
  }

  public Object clone() {
    DASDHTProtocolBuilder dolly = new DASDHTProtocolBuilder(DASDHTProtocolBuilder.prefix);
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
    // startRandomSampling();

    currentBlock.generateRowParcels(KademliaCommonConfigDas.PARCEL_SIZE);
    currentBlock.generateColumnParcels(KademliaCommonConfigDas.PARCEL_SIZE);
    /*while (currentBlock.hasNext()) {
      Sample s = currentBlock.next();
      Message msg = generatePutMessageSample(s);
      this.kadProtocol.handleInit(msg, kademliaId);
    }*/
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

  @Override
  public void operationComplete(Operation op) {}

  @Override
  public void nodesFound(Operation op, BigInteger[] neighbours) {}

  @Override
  public void missing(BigInteger sample, Operation op) {}
}
