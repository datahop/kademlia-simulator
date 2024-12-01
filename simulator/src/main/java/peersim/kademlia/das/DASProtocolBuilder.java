package peersim.kademlia.das;

import java.math.BigInteger;
import java.util.List;
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

    searchTable.assignByRowColumn(currentBlock);

    currentBlock.initIterator();
    while (currentBlock.hasNext()) {
      Sample s = currentBlock.next();

      List<BigInteger> nodesByRow = searchTable.getNodesBySample(s.getId());
      if (nodesByRow != null) {
        for (BigInteger id : nodesByRow) {
          Message msg = new Message(Message.MSG_SEED_SAMPLE, new Sample[] {s});
          msg.operationId = -1;
          msg.src = this.kadProtocol.getKademliaNode();
          Node n = Util.nodeIdtoNode(id, kademliaId);
          msg.dst = n.getKademliaProtocol().getKademliaNode();
          sendMessage(msg, id, myPid);
          System.out.println(
              "Sending row " + s.getRow() + " column " + s.getColumn() + " to " + id);
        }
      }

      List<BigInteger> nodesByColumn = searchTable.getNodesBySample(s.getIdByColumn());
      if (nodesByColumn != null) {
        for (BigInteger id : nodesByColumn) {
          Message msg = new Message(Message.MSG_SEED_SAMPLE, new Sample[] {s});
          msg.operationId = -1;
          msg.src = this.kadProtocol.getKademliaNode();
          Node n = Util.nodeIdtoNode(id, kademliaId);
          msg.dst = n.getKademliaProtocol().getKademliaNode();
          sendMessage(msg, id, myPid);
          System.out.println(
              "Sending row " + s.getRow() + " column " + s.getColumn() + " to " + id);
        }
      }
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
