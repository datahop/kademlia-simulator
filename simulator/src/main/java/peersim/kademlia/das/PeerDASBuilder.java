package peersim.kademlia.das;

import peersim.kademlia.Message;
import peersim.kademlia.gossipsub.GossipSubProtocol;

public class PeerDASBuilder extends PeerDAS {

  protected boolean started;

  public PeerDASBuilder(String prefix) {
    super(prefix);
    started = false;
    /*for (int l = 1; l < Network.size(); l++) {
      Node n2 = Network.get(l);
      GossipSubProtocol prot2 = (GossipSubProtocol) n2.getGossipProtocol();
      for (int j = 1; j <= KademliaCommonConfigDas.BLOCK_DIM_SIZE; j++) {
        String topic = "Row" + j;
        prot2.getTable().addPeer(topic, this.getNodeId());
        topic = "Column" + j;
        prot2.getTable().addPeer(topic, this.getNodeId());
      }
    }*/
  }

  @Override
  public Object clone() {
    PeerDASBuilder dolly = new PeerDASBuilder(PeerDASBuilder.prefix);
    return dolly;
  }

  protected void handleInitNewBlock(Message m, int myPid) {
    currentBlock = (Block) m.body;
    logger.warning("Builder Init block");

    if (!started) {
      started = true;

      for (int j = 1; j <= KademliaCommonConfigDas.BLOCK_DIM_SIZE; j++) {
        String topic = "Row" + j;
        gossipsub.Join(topic);
        GossipSubProtocol.getTable().addPeer(topic, gossipsub.getGossipNode().getId());
        topic = "Column" + j;
        gossipsub.Join(topic);
        GossipSubProtocol.getTable().addPeer(topic, gossipsub.getGossipNode().getId());
      }

    } else {
      for (int i = 1; i <= KademliaCommonConfigDas.BLOCK_DIM_SIZE; i++) {
        Sample[] samples = currentBlock.getSamplesByRow(i);
        String topic = "Row" + i;
        for (Sample s : samples) {
          Message msg = Message.makePublishMessage(topic, s);
          msg.src = this.gossipsub.node;
          gossipsub.Publish(msg, myPid);
        }
        samples = currentBlock.getSamplesByColumn(i);
        topic = "Column" + i;
        for (Sample s : samples) {
          Message msg = Message.makePublishMessage(topic, s);
          msg.src = this.gossipsub.node;
          gossipsub.Publish(msg, myPid);
        }
      }
    }
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
    throw new UnsupportedOperationException("Unimplemented method 'messageReceived'");
  }
}
