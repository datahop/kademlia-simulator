package peersim.kademlia.das;

import java.util.Arrays;
import peersim.config.Configuration;
import peersim.kademlia.Message;
import peersim.kademlia.gossipsub.GossipSubProtocol;

public class GossipDASBuilder extends GossipDAS {

  protected boolean started;

  public GossipDASBuilder(String prefix) {
    super(prefix);
    started = false;
    bw = Configuration.getInt(prefix + "." + PAR_BW, KademliaCommonConfigDas.BUILDER_UPLOAD_RATE);
  }

  @Override
  public Object clone() {
    GossipDASBuilder dolly = new GossipDASBuilder(GossipDASBuilder.prefix);
    return dolly;
  }

  protected void handleInitNewBlock(Message m, int myPid) {
    currentBlock = (Block) m.body;
    logger.warning("Builder Init block");

    if (!started) {
      started = true;

      String rowTopic = "", columnTopic = "";
      for (int j = 1; j <= KademliaCommonConfigDas.BLOCK_DIM_SIZE; j++) {
        String topic = topicMap.getRowTopic(j);
        if (rowTopic != topic) {
          gossipsub.Join(topic);
          GossipSubProtocol.getTable().addPeer(topic, gossipsub.getGossipNode().getId());
        }
        rowTopic = topic;

        topic = topicMap.getColumnTopic(j);
        if (columnTopic != topic) {
          gossipsub.Join(topic);
          GossipSubProtocol.getTable().addPeer(topic, gossipsub.getGossipNode().getId());
        }
        columnTopic = topic;
      }
    } else {
      for (int i = 1; i <= KademliaCommonConfigDas.BLOCK_DIM_SIZE; i++) {
        Sample[] samples = currentBlock.getSamplesByRow(i);
        String topic = topicMap.getRowTopic(i);
        Message msg =
            Message.makePublishMessage(topic, Arrays.copyOfRange(samples, 0, samples.length / 2));
        msg.src = this.gossipsub.node;
        gossipsub.Publish(msg, myPid);

        samples = currentBlock.getSamplesByColumn(i);
        topic = topicMap.getColumnTopic(i);
        msg = Message.makePublishMessage(topic, Arrays.copyOfRange(samples, 0, samples.length / 2));
        msg.src = this.gossipsub.node;
        gossipsub.Publish(msg, myPid);
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
