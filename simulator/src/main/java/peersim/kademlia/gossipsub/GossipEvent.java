package peersim.kademlia.gossipsub;

import peersim.kademlia.Message;

public interface GossipEvent {
  public void messageReceived(Message m);
}
