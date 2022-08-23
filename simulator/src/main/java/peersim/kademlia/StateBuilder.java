package peersim.kademlia;

import java.util.Comparator;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.transport.Transport;

/**
 * Initialization class that performs the bootsrap filling the k-buckets of all initial nodes.<br>
 * In particular every node is added to the routing table of every other node in the network. In the
 * end however the various nodes doesn't have the same k-buckets because when a k-bucket is full a
 * random node in it is deleted.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class StateBuilder implements peersim.core.Control {

  private static final String PAR_PROT = "protocol";
  private static final String PAR_TRANSPORT = "transport";

  private String prefix;
  private int kademliaid;
  private int transportid;

  public StateBuilder(String prefix) {
    this.prefix = prefix;
    kademliaid = Configuration.getPid(this.prefix + "." + PAR_PROT);
    transportid = Configuration.getPid(this.prefix + "." + PAR_TRANSPORT);
  }

  // ______________________________________________________________________________________________
  public final KademliaProtocol get(int i) {
    return ((KademliaProtocol) (Network.get(i)).getProtocol(kademliaid));
  }

  // ______________________________________________________________________________________________
  public final Transport getTr(int i) {
    return ((Transport) (Network.get(i)).getProtocol(transportid));
  }

  // ______________________________________________________________________________________________
  public static void o(Object o) {
    System.out.println(o);
  }

  // ______________________________________________________________________________________________
  public boolean execute() {

    // Sort the network by nodeId (Ascending)
    Network.sort(
        new Comparator<Node>() {

          public int compare(Node o1, Node o2) {
            Node n1 = (Node) o1;
            Node n2 = (Node) o2;
            KademliaProtocol p1 = (KademliaProtocol) (n1.getProtocol(kademliaid));
            KademliaProtocol p2 = (KademliaProtocol) (n2.getProtocol(kademliaid));
            return Util.put0(p1.getNode().getId()).compareTo(Util.put0(p2.getNode().getId()));
          }
        });

    int sz = Network.size();

    // for every node take 50 random node and add to k-bucket of it
    for (int i = 0; i < sz; i++) {
      Node iNode = Network.get(i);
      KademliaProtocol iKad = (KademliaProtocol) (iNode.getProtocol(kademliaid));

      for (int k = 0; k < 100; k++) {
        KademliaProtocol jKad =
            (KademliaProtocol) (Network.get(CommonState.r.nextInt(sz)).getProtocol(kademliaid));
        iKad.getRoutingTable().addNeighbour(jKad.getNode().getId());
      }
    }

    // add other 50 near nodes
    for (int i = 0; i < sz; i++) {
      Node iNode = Network.get(i);
      KademliaProtocol iKad = (KademliaProtocol) (iNode.getProtocol(kademliaid));

      int start = i;
      if (i > sz - 50) {
        start = sz - 25;
      }
      for (int k = 0; k < 50; k++) {
        start = start++;
        if (start > 0 && start < sz) {
          KademliaProtocol jKad = (KademliaProtocol) (Network.get(start++).getProtocol(kademliaid));
          iKad.getRoutingTable().addNeighbour(jKad.getNode().getId());
        }
      }
    }

    return false;
  } // end execute()
}
