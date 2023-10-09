package peersim.kademlia.das;

import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

public class RefreshSearchTable implements Control {
  private String prefix;

  // ______________________________________________________________________________________________
  public RefreshSearchTable(String prefix) {
    this.prefix = prefix;
  }

  // ______________________________________________________________________________________________
  public boolean execute() {
    System.out.println("RefreshSearchTable execute " + CommonState.getTime());
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
