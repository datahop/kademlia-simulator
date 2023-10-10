package peersim.kademlia.das;

import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.Message;
import peersim.kademlia.gossipsub.GossipSubProtocol;

/**
 * This control generates samples every 5 min that are stored in a single node (builder) and starts
 * random sampling from the rest of the nodes In parallel, random lookups are started to start
 * discovering nodes
 *
 * @author Sergi Rene
 * @version 1.0
 */

// ______________________________________________________________________________________________
public class DiscInfoGenerator implements Control {

  Block b;
  private long ID_GENERATOR = 0;

  private boolean first = true, second = false;

  // ______________________________________________________________________________________________
  public DiscInfoGenerator(String prefix) {}

  // ______________________________________________________________________________________________
  /**
   * generates a GET message for t1 key.
   *
   * @return Message
   */
  private Message generateNewBlockMessage(Block b) {

    Message m = Message.makeInitNewBlock(b);
    m.timestamp = CommonState.getTime();

    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * every call of this control generates and send a random find node message
   *
   * @return boolean
   */
  public boolean execute() {

    if (first) {
      for (int i = 0; i < Network.size(); i++) {
        Node start = Network.get(i);
        for (int l = 0; l < Network.size(); l++) {
          if (i != l) {
            Node n2 = Network.get(l);
            GossipSubProtocol prot2 = (GossipSubProtocol) n2.getDASProtocol();
            prot2.getTable().addPeer("disc", start.getDASProtocol().getKademliaId());
          }
        }

        EDSimulator.add(
            i * 100,
            Message.makeInitJoinMessage("disc"),
            start,
            start.getDASProtocol().getDASProtocolID());
      }

      first = false;
      second = true;
    } else /*if (second)*/ {

      // SearchTable.createSampleMap(b);
      for (int i = 0; i < Network.size(); i++) {
        Node n = Network.get(i);
        // b.initIterator();
        // we add 1 ms delay to be sure the builder starts before validators.

        EDSimulator.add(
            0,
            Message.makePublishMessage("disc", n.getDASProtocol().getKademliaId()),
            n,
            n.getDASProtocol().getDASProtocolID());
      }
      ID_GENERATOR++;
      second = false;
    }

    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
