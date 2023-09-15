package peersim.kademlia.das;

import peersim.core.Network;
import peersim.core.Node;

public class RefreshSearchTable {
  private String prefix;

  // ______________________________________________________________________________________________
  public RefreshSearchTable(String prefix) {
    this.prefix = prefix;
  }

  // ______________________________________________________________________________________________
  public boolean execute() {
    for (int i = 0; i < Network.size(); i++) {
      Node iNode = Network.get(i);
      if (iNode.getFailState() == Node.OK) {
        DASProtocol das = iNode.getDASProtocol();
        das.refreshSearchTable();
      }
    }

    return false;
  }
}
