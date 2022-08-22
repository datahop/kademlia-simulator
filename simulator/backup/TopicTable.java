package peersim.kademlia;

import java.math.BigInteger;
import java.util.HashMap;

interface TopicTable {

  public boolean register(TopicRegistration r);

  public TopicRegistration[] getRegistration(Topic t, KademliaNode src);

  public String dumpRegistrations();

  public HashMap<Topic, Integer> getRegbyTopic();

  public HashMap<String, Integer> getRegbyRegistrar();

  public HashMap<String, Integer> getRegEvilbyRegistrar();

  public HashMap<String, HashMap<BigInteger, Integer>> getRegbyRegistrant();

  public HashMap<String, HashMap<BigInteger, Integer>> getRegEvilbyRegistrant();

  public double topicTableUtilisation();

  public void setHostID(BigInteger id);
}
