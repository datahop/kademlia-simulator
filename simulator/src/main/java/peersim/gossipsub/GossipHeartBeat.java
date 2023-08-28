package peersim.gossipsub;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

public class GossipHeartBeat implements Control {

  private String prefix;
  private static final String PAR_PROT = "protocol";
  protected final int pid;

  // ______________________________________________________________________________________________
  public GossipHeartBeat(String prefix) {
    this.prefix = prefix;
    pid = Configuration.getPid(prefix + "." + PAR_PROT);
  }

  // ______________________________________________________________________________________________
  public boolean execute() {
    for (int i = 0; i < Network.size(); i++) {
      Node iNode = Network.get(i);
      if (iNode.getFailState() == Node.OK) {
        GossipSubProtocol iKad = (GossipSubProtocol) iNode.getProtocol(pid);
        iKad.heartBeat();
      }
    }

    return false;
  }
} // End of class
