package peersim.kademlia;

import java.util.*;
import java.util.Arrays;
import org.apache.commons.math3.distribution.ZipfDistribution;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class ZipfTrafficGenerator extends Discv5ZipfTrafficGenerator {

  // ______________________________________________________________________________________________
  private static final String PAR_MAXTOPIC = "maxtopicnum";
  private static final String PAR_LOOKUPS = "randomlookups";

  private final int maxtopicNum;
  private final int randomLookups;

  private Integer[] topicsCounts;
  private Integer[] topicsCounts2;

  // ______________________________________________________________________________________________
  public ZipfTrafficGenerator(String prefix) {
    super(prefix);
    maxtopicNum = Configuration.getInt(prefix + "." + PAR_MAXTOPIC);
    randomLookups = Configuration.getInt(prefix + "." + PAR_LOOKUPS, 0);

    zipf = new ZipfDistribution(maxtopicNum, exp);

    topicsCounts = new Integer[maxtopicNum];

    topicsCounts2 = new Integer[maxtopicNum];

    Arrays.fill(topicsCounts, Integer.valueOf(0));

    Arrays.fill(topicsCounts2, Integer.valueOf(0));
    calculateTopics();
    calculateEvil();
  }

  // ______________________________________________________________________________________________
  /**
   * every call of this control generates and send a random find node message
   *
   * @return boolean
   */
  boolean isDistributionCorrect(Integer[] a) {
    System.out.println("Zipf distribution for topic popularity:");
    System.out.println("count[topic " + (0) + "] = " + a[0]);
    for (int i = 1; i < maxtopicNum; i++) {
      System.out.println("count[topic " + (i) + "] = " + a[i]);
      // if(a[i-1] < a[i]) return true;
    }
    return true;
  }

  private void calculateTopics() {

    for (int i = 0; i < Network.size(); ++i) {
      int topicIndex = zipf.sample() - 1;
      topicsCounts[topicIndex]++;
    }

    // for (int i = 0;i<maxtopicNum;i++)
    //	System.out.println("Topic "+i+" "+topicsCounts[i]);

    // System.out.println("Topic evil
    // "+(topicsCounts[targetTopic-1]-(topicsCounts[targetTopic-1]/(1+percentEvil))));

  }

  private int calculateEvil() {
    int count = 0;
    for (int i = 0; i < Network.size(); ++i) {
      Node start = Network.get(i);
      KademliaProtocol prot = (KademliaProtocol) start.getKademliaProtocol();
      if (prot.getNode().is_evil) {
        topicsCounts[Integer.parseInt(prot.getTargetTopic().getTopic().substring(1))]--;
        topicsCounts2[Integer.parseInt(prot.getTargetTopic().getTopic().substring(1))]++;
      }
    }

    for (int i = 0; i < maxtopicNum; i++) System.out.println("Topic " + i + " " + topicsCounts2[i]);

    return count;
  }

  public boolean execute() {
    // execute it only once
    if (first) {
      // need Integer to sort in reverse order later on

      int topicIndex = 0;
      for (int i = 0; i < Network.size(); i++) {
        Node start = Network.get(i);
        KademliaProtocol prot = (KademliaProtocol) start.getKademliaProtocol();
        Topic topic = null;
        String topicString = "";

        if (topicsCounts[topicIndex] == 0) topicIndex++;

        // if the node is malicious, it targets only one topic read from config
        if (prot.getNode().is_evil) {
          topic = prot.getTargetTopic();

        } else {
          // not extremely efficient, but we want random distribution of topics
          // int topicIndex = zipf.sample() - 1;

          topicsCounts[topicIndex]--;
          topicsCounts2[topicIndex]++;

          System.out.println("Node " + i + " randomed t" + topicIndex);
          topicString = new String("t" + topicIndex);
          topic = new Topic(topicString);
        }

        if (randomLookups == 1) {
          for (int j = 0; j < 3; j++) {
            Node nod = Network.get(i);
            Message lookup = generateFindNodeMessage();
            EDSimulator.add(0, lookup, nod, nod.getKademliaProtocol().getProtocolID());
          }
        }

        int time = CommonState.r.nextInt(KademliaCommonConfig.AD_LIFE_TIME);
        Message registerMessage = generateRegisterMessage(topic.getTopic());
        Message lookupMessage = generateTopicLookupMessage(topic.getTopic());
        prot.getNode().setTopic(topic.getTopic(), start);

        if (registerMessage != null)
          EDSimulator.add(
              time, registerMessage, start, start.getKademliaProtocol().getProtocolID());
        // start lookup messages later
        if (lookupMessage != null)
          EDSimulator.add(
              2 * KademliaCommonConfig.AD_LIFE_TIME + time,
              lookupMessage,
              start,
              start.getKademliaProtocol().getProtocolID());
      }
      assert isDistributionCorrect(topicsCounts2) : "Zipf distribution incorrect";
      first = false;
    }
    return false;
  }
  /*public boolean execute() {
  	//execute it only once
  	if(first) {
  		//need Integer to sort in reverse order later on
  	    Integer [] topicsCounts = new Integer[maxtopicNum];
  	    System.out.println("maxTopicNum: " + maxtopicNum);
  	    Arrays.fill(topicsCounts,  Integer.valueOf(0));

  		for(int i = 0; i < Network.size(); i++)
  		{
  			Node start = Network.get(i);
  			KademliaProtocol prot = (KademliaProtocol)start.getKademliaProtocol();
                 Topic topic = null;
                 String topicString="";


                 // if the node is malicious, it targets only one topic read from config
                 if (prot.getNode().is_evil) {
                     if (attackTopicIndex == -1) {
                         topic = prot.getTargetTopic();
                     } else {
                         topicString = new String("t" + attackTopicIndex);
                         topic = new Topic(topicString);
                         prot.setTargetTopic(topic);
                     }

                 } else {
                 	//not extremely efficient, but we want random distribution of topics
                 	int topicIndex = zipf.sample() - 1;
                 	topicsCounts[topicIndex]++;
                 	System.out.println("Node " + i + " randomed t" + topicIndex);
                     topicString = new String("t" + topicIndex);
                     topic = new Topic(topicString);
                 }

                 if(randomLookups==1) {
  				for(int j = 0;j<3;j++) {
  					Node nod = Network.get(i);
  					Message lookup = generateFindNodeMessage();
  					EDSimulator.add(0, lookup, nod, nod.getKademliaProtocol().getProtocolID());
  				}

                 }

  			int time = CommonState.r.nextInt(KademliaCommonConfig.AD_LIFE_TIME);
  		    Message registerMessage = generateRegisterMessage(topic.getTopic());
  		    Message lookupMessage = generateTopicLookupMessage(topic.getTopic());
  			prot.getNode().setTopic(topic.getTopic(), start);

  		    if(registerMessage != null) EDSimulator.add(time, registerMessage, start, start.getKademliaProtocol().getProtocolID());
  		    //start lookup messages later
  		    if(lookupMessage != null)EDSimulator.add(2*KademliaCommonConfig.AD_LIFE_TIME + time, lookupMessage, start, start.getKademliaProtocol().getProtocolID());



             }
  		assert isDistributionCorrect(topicsCounts) : "Zipf distribution incorrect";
  		first=false;

  	}
  	return false;

  }*/
}
