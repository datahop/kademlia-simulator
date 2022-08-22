package peersim.kademlia;

import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

public class RefreshBuckets implements Control {

  private String prefix;

  // ______________________________________________________________________________________________
  public RefreshBuckets(String prefix) {
    this.prefix = prefix;
  }

  // ______________________________________________________________________________________________
  public boolean execute() {
    for (int i = 0; i < Network.size(); i++) {
      Node iNode = Network.get(i);
      if (iNode.getFailState() == Node.OK) {
        KademliaProtocol iKad = iNode.getKademliaProtocol();
        iKad.refreshBuckets();
      }
    }

    return false;
  }
} // End of class
