package peersim.kademlia;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.Security;
import org.apache.tuweni.devp2p.EthereumNodeRecord;
import org.apache.tuweni.io.Base64URLSafe;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.json.JSONException;
import org.json.JSONObject;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.dynamics.NodeInitializer;

/**
 * This control initializes the whole network (that was already created by peersim) assigning a
 * unique NodeId, randomly generated, to every node.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class DnsListDistribution implements peersim.core.Control {

  private static final String PAR_PROT = "protocol";

  private int protocolID;
  private UniformRandomGenerator urg;

  protected NodeInitializer[] inits;

  private static final String PAR_INIT = "init";

  private JSONObject json;

  public DnsListDistribution(String prefix) {
    System.out.println("DNSLISTDIstr " + prefix);
    protocolID = Configuration.getPid(prefix + "." + PAR_PROT);
    urg = new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    Object[] tmp = Configuration.getInstanceArray(prefix + "." + PAR_INIT);
    inits = new NodeInitializer[tmp.length];
    for (int i = 0; i < tmp.length; ++i) inits[i] = (NodeInitializer) tmp[i];
    try {
      // json =
      // readJsonFromUrl("https://raw.githubusercontent.com/ethereum/discv4-dns-lists/master/all.mainnet.ethdisco.net/nodes.json");
      json =
          readJsonFromUrl(
              "https://raw.githubusercontent.com/ethereum/discv4-dns-lists/master/all.json");
      // json = readJsonFromFile("all.json");
      // json = readJsonFromFile("nodes.json");

    } catch (Exception e) {
      System.err.println("Exception " + e);
    }
  }

  /**
   * Scan over the nodes in the network and assign a randomly generated NodeId in the space
   * 0..2^BITS, where BITS is a parameter from the kademlia protocol (usually 160)
   *
   * @return boolean always false
   */
  public boolean execute() {
    Security.addProvider(new BouncyCastleProvider());
    int i = 0;
    for (String keyStr : json.keySet()) {
      if (i < Network.size()) {
        JSONObject json2 = json.getJSONObject(keyStr);
        // System.out.println("Record: "+ json2.getString("record").substring(4)+"
        // "+json.keySet().size());

        // EthereumNodeRecord enr =
        // EthereumNodeRecord.fromRLP(Bytes.fromHexString(json2.getString("record")));
        System.out.println("Setting up node: " + i);
        System.out.println("Record key: " + keyStr);
        EthereumNodeRecord enr =
            EthereumNodeRecord.fromRLP(
                (Base64URLSafe.decode(json2.getString("record").substring(4))));
        // System.out.println("Record id: "+ ((Bytes)enr.getData().get("id")).toString());
        System.out.println(
            "Record id: "
                + enr.publicKey()
                    .bytes()
                    .slice(KademliaCommonConfig.BITS / 8)
                    .toUnsignedBigInteger());
        System.out.println("Record ip: " + enr.ip().toString().substring(1));

        KademliaNode node =
            new KademliaNode(
                enr.publicKey().bytes().slice(KademliaCommonConfig.BITS / 8).toUnsignedBigInteger(),
                enr.ip().toString().substring(1),
                0);
        // KademliaNode node = new
        // KademliaNode(enr.publicKey().bytes().slice(KademliaCommonConfig.BITS/8).toUnsignedBigInteger(), enr.ip().toString(), enr.tcp());

        // node.setProtocolId(protocolID);
        /*if(i>=Network.size()) {
        	Node newNode = (Node) Network.prototype.clone();
        	for (int j = 0; j < inits.length; ++j)
        		inits[j].initialize(newNode);
        	Network.add(newNode);

        	// get kademlia protocol of new node
        	KademliaProtocol newKad = (KademliaProtocol) (newNode.getProtocol(protocolID));

        }*/

        Node generalNode = Network.get(i);

        generalNode.setKademliaProtocol(
            (KademliaProtocol) (Network.get(i).getProtocol(protocolID)));
        ((KademliaProtocol) (Network.get(i).getProtocol(protocolID))).setNode(node);
        ((KademliaProtocol) (Network.get(i).getProtocol(protocolID))).setProtocolID(protocolID);

        // ((KademliaProtocol) (Network.get(i).getProtocol(protocolID))).setNode(node);
        i++;
      } else {
        break;
      }
    }

    while (i < Network.size()) Network.remove(i);

    return false;
  }

  private static String readAll(Reader rd) throws IOException {
    StringBuilder sb = new StringBuilder();
    int cp;
    while ((cp = rd.read()) != -1) {
      sb.append((char) cp);
    }
    return sb.toString();
  }

  public static JSONObject readJsonFromFile(String filename) throws IOException, JSONException {
    BufferedReader rd = null;

    try {

      rd = new BufferedReader(new FileReader(filename));
      String jsonText = readAll(rd);
      // System.out.println("JSON string "+jsonText);
      JSONObject json = new JSONObject(jsonText);
      return json;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (org.json.JSONException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
    InputStream is = new URL(url).openStream();
    try {
      BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
      String jsonText = readAll(rd);
      // System.out.println("JSON string "+jsonText);
      JSONObject json = new JSONObject(jsonText);
      return json;
    } finally {
      is.close();
    }
  }
}
