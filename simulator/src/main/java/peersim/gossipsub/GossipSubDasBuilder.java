package peersim.gossipsub;

import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.Sample;

public class GossipSubDasBuilder extends GossipSubDas {

  public GossipSubDasBuilder(String prefix) {
    super(prefix);
    // TODO Auto-generated constructor stub
  }

  /**
   * Replicate this object by returning an identical copy. It is called by the initializer and do
   * not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    GossipSubDasBuilder dolly = new GossipSubDasBuilder(GossipSubDasBuilder.prefix);
    return dolly;
  }

  protected void handleMessage(Message m, int myPid) {}

  protected void handleInitNewBlock(Message m, int myPid) {
    currentBlock = (Block) m.body;
    logger.warning("Builder Init block");

    if (!started) {
      started = true;
      for (int l = 1; l < Network.size(); l++) {
        Node n2 = Network.get(l);
        GossipSubProtocol prot2 = (GossipSubProtocol) n2.getProtocol(myPid);
        for (int j = 1; j <= GossipCommonConfig.BLOCK_DIM_SIZE; j++) {
          String topic = "Row" + j;
          prot2.getTable().addPeer(topic, this.node.getId());
          topic = "Column" + j;
          prot2.getTable().addPeer(topic, this.node.getId());
        }
        // EDSimulator.add(0, generateNewBlockMessage(b), n2, protocol);
      }
      for (int j = 1; j <= GossipCommonConfig.BLOCK_DIM_SIZE; j++) {
        String topic = "Row" + j;
        handleJoin(Message.makeInitJoinMessage(topic), myPid);
        // EDSimulator.add(0, Message.makeInitJoinMessage(topic), n, protocol);
        topic = "Column" + j;
        handleJoin(Message.makeInitJoinMessage(topic), myPid);
        // EDSimulator.add(0, Message.makeInitJoinMessage(topic), n, protocol);
      }
    } else {
      for (int i = 0; i < GossipCommonConfig.BLOCK_DIM_SIZE; i++) {
        for (int j = 0; j < GossipCommonConfig.BLOCK_DIM_SIZE; j++) {
          Sample s = currentBlock.getSample(i, j);
          String topic = "Row" + (s.getRow());
          Message msg = Message.makePublishMessage(topic, s);
          msg.src = this.node;
          handlePublish(msg, myPid);
          topic = "Column" + (s.getColumn());
          msg = Message.makePublishMessage(topic, s);
          msg.src = this.node;
          handlePublish(msg, myPid);
        }
      }
    }
  }
}
