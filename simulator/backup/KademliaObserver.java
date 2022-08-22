package peersim.kademlia;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.operations.FindOperation;
import peersim.kademlia.operations.LookupOperation;
import peersim.kademlia.operations.LookupTicketOperation;
import peersim.kademlia.operations.Operation;
import peersim.kademlia.operations.RegisterOperation;
import peersim.util.IncrementalStats;

/**
 * This class implements a simple observer of search time and hop average in finding a node in the
 * network
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class KademliaObserver implements Control {

  private static final String PAR_RANGE_EXPERIMENT = "rangeExperiment";
  private static final String PAR_STEP = "step";
  private static final String PAR_REPORT_MSG = "reportMsg";
  private static final String PAR_REPORT_REG = "reportReg";
  private static final int numMsgTypes = 13;

  private static String logFolderName;
  private String parameterName;
  /** keep statistics of the number of hops of every message delivered. */
  public static IncrementalStats hopStore = new IncrementalStats();

  /** keep statistics of the time every message delivered. */
  public static IncrementalStats timeStore = new IncrementalStats();

  /** keep statistic of number of message delivered */
  private static IncrementalStats msg_deliv = new IncrementalStats();

  /** keep statistic of number of message sent */
  public static IncrementalStats msg_sent = new IncrementalStats();

  /** keep statistic of number of find operation */
  public static IncrementalStats find_total = new IncrementalStats();

  /** Successfull find operations */
  public static IncrementalStats find_ok = new IncrementalStats();

  /** keep statistic of number of register operation */
  public static IncrementalStats register_total = new IncrementalStats();

  /** keep statistic of number of register operation */
  public static IncrementalStats lookup_total = new IncrementalStats();

  /** keep statistic of number of register operation */
  public static IncrementalStats register_ok = new IncrementalStats();

  // public static HashMap<String, Set<BigInteger>> registeredTopics = new HashMap<String,
  // Set<BigInteger>>();

  public static HashMap<String, HashMap<BigInteger, RegistrationLog>> registeredTopics =
      new HashMap<String, HashMap<BigInteger, RegistrationLog>>();

  public static TreeMap<String, Integer> activeRegistrations = new TreeMap<String, Integer>();

  public static TreeMap<String, Integer> activeRegistrationsByMalicious =
      new TreeMap<String, Integer>();

  private static HashMap<BigInteger, BigInteger> nodeInfo = new HashMap<BigInteger, BigInteger>();
  private static HashSet<BigInteger> writtenNodeIDs = new HashSet<BigInteger>();

  private static FileWriter msgWriter;
  private static FileWriter opWriter;

  // private static HashMap<Topic,Integer> regByTopic;
  private static HashMap<String, HashMap<BigInteger, Integer>> regByRegistrant;
  private static HashMap<String, HashMap<BigInteger, Integer>> regByRegistrantEvil;
  private static HashMap<String, HashMap<BigInteger, Integer>> regByRegistrar;
  private static HashMap<String, HashMap<BigInteger, Integer>> regByRegistrarEvil;
  private static HashMap<String, HashMap<BigInteger, Integer>> competingTickets;

  // Waiting times
  private static HashMap<String, Long> waitingTimes = new HashMap<String, Long>();
  private static HashMap<String, Long> waitingTimesByEvilNodes = new HashMap<String, Long>();
  private static HashMap<String, Integer> numOfReportedWaitingTimes =
      new HashMap<String, Integer>();
  private static HashMap<String, Integer> numOfReportedWaitingTimesByEvilNodes =
      new HashMap<String, Integer>();
  private static HashMap<String, Long> cumWaitingTimes = new HashMap<String, Long>();
  private static HashMap<String, Long> cumWaitingTimesByEvilNodes = new HashMap<String, Long>();
  private static HashMap<String, Integer> numOfReportedCumWaitingTimes =
      new HashMap<String, Integer>();
  private static HashMap<String, Integer> numOfReportedCumWaitingTimesByEvilNodes =
      new HashMap<String, Integer>();
  private static HashMap<String, Integer> numOfRejectedRegistrations =
      new HashMap<String, Integer>();
  private static HashMap<String, Integer> numOfRejectedRegistrationsByEvilNodes =
      new HashMap<String, Integer>();
  private static int ticket_request_to_evil_nodes = 0;
  private static int ticket_request_to_good_nodes = 0;

  private static HashMap<String, Long> betterTimes = new HashMap<String, Long>();
  private static HashMap<String, Integer> betterTimesCount = new HashMap<String, Integer>();

  private static HashMap<String, Long> timeOverThreshold = new HashMap<String, Long>();
  private static HashMap<String, Integer> timeOverThresholdCount = new HashMap<String, Integer>();

  private static HashMap<BigInteger, HashMap<String, Integer>> perNodeStats =
      new HashMap<BigInteger, HashMap<String, Integer>>();
  private static HashMap<Integer, Integer> msgSentPerType = new HashMap<Integer, Integer>();
  private static HashMap<String, Integer> registerOverhead = new HashMap<String, Integer>();
  private static HashMap<String, Integer> numberOfRegistrations = new HashMap<String, Integer>();

  private static HashMap<BigInteger, Integer> avgCounter;

  private HashMap<String, Integer> topicsList;

  private static int observerStep;
  private static int reportMsg;
  private static int reportReg;
  /** Prefix to be printed in output */
  private String prefix;

  private static KademliaProtocol kadProtocol;

  private static HashSet<String> topicsSet;
  private static List<String> all_topics;

  // Eclipsing Stats:
  private static HashMap<String, Integer> numEclipsedLookupOperations = new HashMap<>();
  private static HashMap<String, Double> percentEclipsedDiscoveredInLookupOperations =
      new HashMap<>();
  private static HashMap<String, Integer> numLookupOperationsPerTopic = new HashMap<>();

  public KademliaObserver(String prefix) {
    this.prefix = prefix;
    this.observerStep = Configuration.getInt(prefix + "." + PAR_STEP);
    this.parameterName = Configuration.getString(prefix + "." + PAR_RANGE_EXPERIMENT, "");
    this.reportMsg = Configuration.getInt(prefix + "." + PAR_REPORT_MSG, 1);
    this.reportReg = Configuration.getInt(prefix + "." + PAR_REPORT_REG, 1);

    topicsList = new HashMap<String, Integer>();
    // if (!this.parameterName.isEmpty())
    //    this.parameterValue = Configuration.getDouble(prefix + "." + PAR_RANGE_EXPERIMENT, -1);

    topicsSet = new HashSet<String>();
    if (this.parameterName.isEmpty()) this.logFolderName = "./logs";
    else
      // this.logFolderName = this.parameterName + "-" + String.valueOf(this.parameterValue);
      this.logFolderName = this.parameterName;
    File directory = new File(this.logFolderName);
    if (!directory.exists()) {
      directory.mkdir();
    }

    try {
      if (reportMsg == 1) {
        msgWriter = new FileWriter(this.logFolderName + "/" + "messages.csv");
        msgWriter.write("id,type,src,dst,topic,bucket,waiting_time,sent/received\n");
      }
      opWriter = new FileWriter(this.logFolderName + "/" + "operations.csv");
      opWriter.write(
          "time,id,type,src,dst,used_hops,returned_hops,malicious,discovered,discovered_list,discovered_malicious,queried_malicious,topic,topicID,evil,hops,ratio\n");
      // regByTopic = new HashMap<Topic,Integer>();
      regByRegistrant = new HashMap<String, HashMap<BigInteger, Integer>>();
      regByRegistrar = new HashMap<String, HashMap<BigInteger, Integer>>();
      regByRegistrarEvil = new HashMap<String, HashMap<BigInteger, Integer>>();
      regByRegistrantEvil = new HashMap<String, HashMap<BigInteger, Integer>>();
      competingTickets = new HashMap<String, HashMap<BigInteger, Integer>>();

      avgCounter = new HashMap<BigInteger, Integer>();
      String filename = this.logFolderName + "/" + "waiting_times.csv";
      File myFile = new File(filename);
      if (myFile.exists()) myFile.delete();

      filename = this.logFolderName + "/" + "registration_stats.csv";
      myFile = new File(filename);
      if (myFile.exists()) myFile.delete();

      filename = this.logFolderName + "/" + "topics.csv";
      myFile = new File(filename);
      if (myFile.exists()) myFile.delete();

      filename = this.logFolderName + "/" + "storage_utilisation.csv";
      myFile = new File(filename);
      if (myFile.exists()) myFile.delete();

      filename = this.logFolderName + "/" + "msg_stats.csv";
      myFile = new File(filename);
      if (myFile.exists()) myFile.delete();

      filename = this.logFolderName + "/" + "eclipse_counts.csv";
      myFile = new File(filename);
      if (myFile.exists()) myFile.delete();

      filename = this.logFolderName + "/" + "overThresholdWaitingTimes.csv";
      myFile = new File(filename);
      if (myFile.exists()) myFile.delete();

      filename = this.logFolderName + "/" + "betterWaitingTimes.csv";
      myFile = new File(filename);
      if (myFile.exists()) myFile.delete();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void addTopicRegistration(Topic t, BigInteger registrant) {

    topicsSet.add(t.getTopic());
    all_topics = new ArrayList<String>(topicsSet);
    Collections.sort(all_topics);
    String topic = t.getTopic();
    if (!registeredTopics.containsKey(topic)) {
      // System.out.println("addTopicRegistration "+topic);

      HashMap<BigInteger, RegistrationLog> set = new HashMap<BigInteger, RegistrationLog>();
      RegistrationLog reg = new RegistrationLog(registrant, CommonState.getTime());
      set.put(registrant, reg);
      registeredTopics.put(topic, set);
    } else {
      HashMap<BigInteger, RegistrationLog> set = registeredTopics.get(topic);
      RegistrationLog reg = new RegistrationLog(registrant, CommonState.getTime());
      set.put(registrant, reg);
      registeredTopics.put(topic, set);
      // System.out.println("addTopicRegistration "+topic+" "+registeredTopics.get(topic).size());
    }

    if (!nodeInfo.containsKey(registrant)) {
      nodeInfo.put(registrant, t.getTopicID());
    }
  }

  public static void addAcceptedRegistration(
      Topic t, BigInteger registrant, BigInteger registrar, long waitingTime, boolean is_evil) {
    String topic = t.getTopic();
    if (registeredTopics.containsKey(topic)) {
      HashMap<BigInteger, RegistrationLog> set = registeredTopics.get(topic);
      if (set.containsKey(registrant)) {
        set.get(registrant).addRegistrar(registrar, CommonState.getTime(), waitingTime);
      }
    }
    Integer count = numberOfRegistrations.get(topic);
    if (count == null) numberOfRegistrations.put(topic, 1);
    else numberOfRegistrations.put(topic, count + 1);

    increaseNodeStatsBy(registrant, "regsPlaced", 1);
    increaseNodeStatsBy(registrar, "regsAccepted", 1);
    if (is_evil) increaseNodeStatsBy(registrar, "regsEvilAccepted", 1);
  }

  public static void addDiscovered(Topic t, BigInteger requesting, BigInteger discovered) {
    String topic = t.getTopic();
    if (registeredTopics.containsKey(topic)) {
      HashMap<BigInteger, RegistrationLog> set = registeredTopics.get(topic);
      if (set.containsKey(discovered)) {
        set.get(discovered).addDiscovered(requesting, CommonState.getTime());
        // set.get(discovered).addRegistrar(requesting, CommonState.getTime());
      }
    }
  }

  public static int topicRegistrationCount(String topic) {
    if (registeredTopics.containsKey(topic)) {
      return registeredTopics.get(topic).size();
    }
    return 0;
  }

  public static void reportEclipsingStats(Operation op) {

    LookupOperation lop = (LookupOperation) op;
    String topic = lop.topic.getTopic();
    Integer count = numLookupOperationsPerTopic.get(topic);
    if (count == null) numLookupOperationsPerTopic.put(topic, 1);
    else numLookupOperationsPerTopic.put(topic, count + 1);

    int numDiscovered = lop.discoveredCount();
    int numEvils = lop.maliciousDiscoveredCount();

    if (lop.isEclipsed()) { // eclipsed
      count = numEclipsedLookupOperations.get(topic);
      if (count == null) numEclipsedLookupOperations.put(topic, 1);
      else numEclipsedLookupOperations.put(topic, count + 1);
    }
    // percent of discovered nodes that are evil out of all discovered in an operation
    Double percentEvilDiscovered = (1.0 * numEvils) / numDiscovered;
    if (numDiscovered == 0) percentEvilDiscovered = 0.0;
    Double percent = percentEclipsedDiscoveredInLookupOperations.get(topic);
    if (percent == null)
      percentEclipsedDiscoveredInLookupOperations.put(topic, percentEvilDiscovered);
    else percentEclipsedDiscoveredInLookupOperations.put(topic, percent + percentEvilDiscovered);
  }

  private static HashMap<String, Integer> createMsgReceivedByNodesEntry() {
    // the last value is a total of all types
    HashMap<String, Integer> msgStats = new HashMap<String, Integer>();
    // init counters for all the message types
    for (int msgType = 0; msgType < numMsgTypes; msgType++) {
      String msgTypeString = new Message(msgType).messageTypetoString();
      msgStats.put(msgTypeString, 0);
      msgStats.put(msgTypeString + "_evil", 0);
    }
    // init counters for all other features
    msgStats.put("numMsg", 0);
    msgStats.put("numMsg_evil", 0);
    msgStats.put("discovered", 0);
    msgStats.put("wasDiscovered", 0);
    msgStats.put("regsPlaced", 0);
    msgStats.put("regsAccepted", 0);
    msgStats.put("regsEvilAccepted", 0);
    msgStats.put("maliciousDiscovered", 0);
    msgStats.put("lookupOperations", 0);
    msgStats.put("lookupAskedNodes", 0);
    msgStats.put("eclipsedLookupOperations", 0);
    msgStats.put("maliciousResultsByHonest", 0);
    msgStats.put("lookupAskedMaliciousNodes", 0);
    msgStats.put("nodeTopic", -1);

    return msgStats;
  }

  public static void increaseNodeStatsBy(BigInteger nodeID, String feature, int increase) {
    HashMap<String, Integer> nodeStats = perNodeStats.get(nodeID);
    if (nodeStats == null) {
      nodeStats = createMsgReceivedByNodesEntry();
      nodeStats.put(
          "isMalicious", Util.nodeIdtoNode(nodeID).getKademliaProtocol().getNode().is_evil ? 1 : 0);
      nodeStats.put(
          "nodeTopic", Util.nodeIdtoNode(nodeID).getKademliaProtocol().getNode().getTopicNum());
    }
    // make sure we initialized the counter for that feature
    assert (nodeStats.keySet().contains(feature));
    nodeStats.put(feature, nodeStats.get(feature) + increase);
    perNodeStats.put(nodeID, nodeStats);
  }

  public static void reportOperation(Operation op) {
    // if(CommonState.getTime()<KademliaCommonConfig.AD_LIFE_TIME)return;

    try {
      // System.out.println("Report operation "+CommonState.getTime()+"
      // "+op.getClass().getSimpleName()+" "+KademliaCommonConfig.AD_LIFE_TIME);
      String result = "";

      if (op instanceof LookupOperation || op instanceof LookupTicketOperation) {
        // result += op.operationId + "," + op.getClass().getSimpleName() + ","  + op.srcNode +"," +
        // op.destNode + "," + op.getUsedCount() + "," +op.getReturnedCount()+
        // ","+((LookupOperation) op).maliciousDiscoveredCount()   + "," +
        // ((LookupOperation)op).discoveredCount() +","+ ((LookupOperation)op).discoveredToString()
        // + "," + ((LookupOperation)op).discoveredMaliciousToString()+","+((LookupOperation)
        // op).maliciousNodesQueries()+","+((LookupOperation)op).topic.topic+ "," +
        // ((LookupOperation)op).topic.topicID
        // +",,"+((LookupOperation)op).discoveredCount()/op.getUsedCount()+"\n";
        LookupOperation lop = (LookupOperation) op;
        reportEclipsingStats(op);

        int discovered = lop.discoveredCount();
        int maliciousDiscovered = lop.maliciousDiscoveredCount();
        assert (discovered >= maliciousDiscovered);
        increaseNodeStatsBy(lop.srcNode, "discovered", discovered);
        increaseNodeStatsBy(lop.srcNode, "maliciousDiscovered", maliciousDiscovered);
        increaseNodeStatsBy(lop.srcNode, "lookupOperations", 1);
        increaseNodeStatsBy(lop.srcNode, "lookupAskedNodes", lop.askedNodeCount());
        increaseNodeStatsBy(
            lop.srcNode, "maliciousResultsByHonest", lop.maliciousResponseByHonest());
        increaseNodeStatsBy(lop.srcNode, "lookupAskedMaliciousNodes", lop.maliciousNodesQueries());
        if (lop.isEclipsed()) {
          increaseNodeStatsBy(op.srcNode, "eclipsedLookupOperations", 1);
        }
        // add info about being discovered by others
        for (KademliaNode discoveredNode : ((LookupOperation) op).getDiscovered()) {
          increaseNodeStatsBy(discoveredNode.getId(), "wasDiscovered", 1);
        }

        String ratioStr = "";

        if (discovered > 0) {
          Double ratio = (double) op.nrHops / lop.discoveredCount();
          ratioStr = ratio.toString();
        }
        result +=
            CommonState.getTime()
                + ","
                + op.operationId
                + ","
                + lop.getClass().getSimpleName()
                + ","
                + lop.srcNode
                + ","
                + lop.destNode
                + ","
                + lop.getUsedCount()
                + ","
                + lop.getReturnedCount()
                + ","
                + lop.maliciousDiscoveredCount()
                + ","
                + lop.discoveredCount()
                + ", ,"
                + lop.discoveredMaliciousToString()
                + ","
                + lop.maliciousNodesQueries()
                + ","
                + lop.topic.topic
                + ","
                + lop.topic.topicID
                + ",,"
                + op.nrHops
                + ","
                + ratioStr
                + "\n";

      } else if (op instanceof RegisterOperation) {
        // System.out.println("register operation reported");
        // System.exit(1);
        result +=
            CommonState.getTime()
                + ","
                + op.operationId
                + ","
                + op.getClass().getSimpleName()
                + ","
                + op.srcNode
                + ","
                + op.destNode
                + ","
                + op.getUsedCount()
                + ","
                + op.getReturnedCount()
                + ","
                + ","
                + ","
                + ","
                + ((RegisterOperation) op).topic.topic
                + ","
                + ((LookupOperation) op).topic.topicID
                + "\n";
      } else if (op instanceof FindOperation && op.getUsedCount() > 0) {
        // double returned_per_hop = (double)((FindOperation)op).getDiscovered()/op.getUsedCount();
        // System.out.println("Returned "+returned_per_hop);
        result +=
            CommonState.getTime()
                + ","
                + op.operationId
                + ","
                + op.getClass().getSimpleName()
                + ","
                + op.srcNode
                + ","
                + op.destNode
                + ","
                + op.getUsedCount()
                + ","
                + op.getReturnedCount()
                + ","
                + ","
                + ((FindOperation) op).getDiscovered()
                + ",,,,"
                + ((FindOperation) op).getTopic()
                + ",,,\n";
      }
      opWriter.write(result);
      opWriter.flush();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void reportActiveRegistration(Topic t, boolean is_evil) {
    if (is_evil) {
      if (!activeRegistrationsByMalicious.containsKey(t.getTopic())) {
        activeRegistrationsByMalicious.put(t.getTopic(), 1);
      } else {
        Integer num = activeRegistrationsByMalicious.get(t.getTopic());
        num++;
        activeRegistrationsByMalicious.put(t.getTopic(), num);
      }
    } else {
      if (!activeRegistrations.containsKey(t.getTopic())) {
        activeRegistrations.put(t.getTopic(), 1);
      } else {
        Integer num = activeRegistrations.get(t.getTopic());
        num++;
        activeRegistrations.put(t.getTopic(), num);
      }
    }
  }

  // Report the cumulative waiting time for accepted tickets
  public static void reportCumulativeTime(Topic topic, long time, boolean is_evil) {
    HashMap<String, Long> cum_waiting_times;
    HashMap<String, Integer> num_reported_cum_waiting_times;
    if (is_evil) {
      cum_waiting_times = cumWaitingTimesByEvilNodes;
      num_reported_cum_waiting_times = numOfReportedCumWaitingTimesByEvilNodes;
    } else {
      cum_waiting_times = cumWaitingTimes;
      num_reported_cum_waiting_times = numOfReportedCumWaitingTimes;
    }

    Long totalCumWaitTime = cum_waiting_times.get(topic.getTopic());
    if (totalCumWaitTime == null) cum_waiting_times.put(topic.getTopic(), time);
    else cum_waiting_times.put(topic.getTopic(), totalCumWaitTime + time);

    Integer count = num_reported_cum_waiting_times.get(topic.getTopic());
    if (count == null) num_reported_cum_waiting_times.put(topic.getTopic(), 1);
    else num_reported_cum_waiting_times.put(topic.getTopic(), count + 1);
  }

  public static void reportWaitingTime(Topic topic, long time, boolean is_evil) {
    if (time == -1) {
      // report rejected registration
      HashMap<String, Integer> rejectedRegs;
      if (is_evil) rejectedRegs = numOfRejectedRegistrationsByEvilNodes;
      else rejectedRegs = numOfRejectedRegistrations;

      Integer rejected = rejectedRegs.get(topic.getTopic());
      if (rejected == null) rejectedRegs.put(topic.getTopic(), 1);
      else rejectedRegs.put(topic.getTopic(), rejected + 1);
      return;
    }

    // report waiting time
    HashMap<String, Long> waiting_times;
    HashMap<String, Integer> reported_waiting_times;
    if (is_evil) {
      waiting_times = waitingTimesByEvilNodes;
      reported_waiting_times = numOfReportedWaitingTimesByEvilNodes;
    } else {
      waiting_times = waitingTimes;
      reported_waiting_times = numOfReportedWaitingTimes;
    }

    Long totalWaitTime = waiting_times.get(topic.getTopic());
    Integer count = reported_waiting_times.get(topic.getTopic());
    if (count == null) {
      waiting_times.put(topic.getTopic(), time);
      reported_waiting_times.put(topic.getTopic(), 1);
    } else {
      long reportedTime = time + totalWaitTime;
      if (reportedTime < 0) reportedTime = Long.MAX_VALUE;
      waiting_times.put(topic.getTopic(), reportedTime);
      reported_waiting_times.put(topic.getTopic(), count + 1);
    }
  }

  public static void reportExpiredRegistration(Topic t, boolean is_evil) {
    if (is_evil) {
      Integer num = activeRegistrationsByMalicious.get(t.getTopic());
      num -= 1;
      activeRegistrationsByMalicious.put(t.getTopic(), num);
    } else {
      Integer num = activeRegistrations.get(t.getTopic());
      num -= 1;
      activeRegistrations.put(t.getTopic(), num);
    }
  }

  private static void accountMsg(Message m) {

    /*if (CommonState.getIntTime() <= 300000)
    return;*/

    Integer numMsg = msgSentPerType.get(m.getType());
    if (numMsg == null) msgSentPerType.put(m.getType(), 1);
    else {
      msgSentPerType.put(m.getType(), numMsg + 1);
    }
    if (m.getType() == Message.MSG_REGISTER) {
      if (m.dest.is_evil) ticket_request_to_evil_nodes++;
      else ticket_request_to_good_nodes++;

      Topic topic = null;
      if (m.body instanceof Ticket) {
        Ticket t = (Ticket) m.body;
        topic = t.getTopic();
      } else {
        topic = (Topic) m.body;
      }

      Integer count = registerOverhead.get(topic.getTopic());
      if (count == null) registerOverhead.put(topic.getTopic(), 1);
      else registerOverhead.put(topic.getTopic(), count + 1);
    }

    if (m.src.is_evil) {
      increaseNodeStatsBy(m.dest.getId(), m.messageTypetoString() + "_evil", 1);
      increaseNodeStatsBy(m.dest.getId(), "numMsg_evil", 1);
    } else {
      increaseNodeStatsBy(m.dest.getId(), m.messageTypetoString(), 1);
      increaseNodeStatsBy(m.dest.getId(), "numMsg", 1);
    }
  }

  private void write_waiting_times() {

    if (waitingTimes.size() == 0) {
      return;
    }
    try {
      String filename = this.logFolderName + "/" + "waiting_times.csv";
      File myFile = new File(filename);
      FileWriter writer;

      if (!myFile.exists()) {
        myFile.createNewFile();
        writer = new FileWriter(myFile, true);
        String title = "time";
        for (String topic : all_topics) {
          title += "," + topic + "_wait";
          title += "," + topic + "_evil_wait";
        }
        for (String topic : all_topics) {
          title += "," + topic + "_cumWait";
          title += "," + topic + "_evil_cumWait";
        }
        for (String topic : all_topics) {
          title += "," + topic + "_reject";
          title += "," + topic + "_evil_reject";
        }
        title += "\n";
        writer.write(title);
      } else {
        writer = new FileWriter(myFile, true);
      }
      writer.write("" + CommonState.getTime());
      for (String topic : all_topics) {
        Long totalWaitTime = waitingTimes.get(topic);
        Integer numOfReported = numOfReportedWaitingTimes.get(topic);
        if (numOfReported == null) writer.write(",0.0");
        else writer.write("," + totalWaitTime.doubleValue() / numOfReported.intValue());

        totalWaitTime = waitingTimesByEvilNodes.get(topic);
        numOfReported = numOfReportedWaitingTimesByEvilNodes.get(topic);
        if (numOfReported == null) writer.write(",0.0");
        else writer.write("," + totalWaitTime.doubleValue() / numOfReported.intValue());
      }
      for (String topic : all_topics) {
        Long totalWaitTime = cumWaitingTimes.get(topic);
        Integer numOfReported = numOfReportedCumWaitingTimes.get(topic);
        if (numOfReported == null) writer.write(",0.0");
        else writer.write("," + totalWaitTime.doubleValue() / numOfReported.intValue());

        totalWaitTime = cumWaitingTimesByEvilNodes.get(topic);
        numOfReported = numOfReportedCumWaitingTimesByEvilNodes.get(topic);
        if (numOfReported == null) writer.write(",0.0");
        else writer.write("," + totalWaitTime.doubleValue() / numOfReported.intValue());
      }
      for (String topic : all_topics) {
        Integer rejected = numOfRejectedRegistrations.get(topic);
        if (rejected == null) writer.write(",0");
        else writer.write("," + rejected);

        rejected = numOfRejectedRegistrationsByEvilNodes.get(topic);
        if (rejected == null) writer.write(",0");
        else writer.write("," + rejected);
      }
      writer.write("\n");
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    numOfReportedWaitingTimes.clear();
    waitingTimes.clear();
    numOfRejectedRegistrations.clear();
    cumWaitingTimes.clear();
    numOfReportedCumWaitingTimes.clear();
  }

  private void write_exchanged_msg_stats_over_time() {

    try {
      String filename = this.logFolderName + "/" + "msg_stats.csv";
      File myFile = new File(filename);
      FileWriter writer;
      if (!myFile.exists()) {
        myFile.createNewFile();
        writer = new FileWriter(myFile, true);
        String title = "time";
        for (int msgType = 0; msgType < numMsgTypes; msgType++) {
          Message m = new Message(msgType);
          title += "," + m.messageTypetoString();
        }
        title += ",regToGoodNodes" + ",regToEvilNodes";
        title += "\n";
        writer.write(title);
      } else {
        writer = new FileWriter(myFile, true);
      }
      writer.write("" + CommonState.getTime());
      for (int msgType = 0; msgType < numMsgTypes; msgType++) {
        Integer count = msgSentPerType.get(msgType);
        if (count == null) count = 0;
        writer.write("," + count);
      }
      int num_good_nodes = 0;
      int num_evil_nodes = 0;
      for (int i = 0; i < Network.size(); ++i) {
        if (Network.get(i).isUp()) {
          if (Network.get(i).getKademliaProtocol().getNode().is_evil) num_evil_nodes++;
          else num_good_nodes++;
        }
      }
      double average_received = ((double) ticket_request_to_good_nodes) / num_good_nodes;
      writer.write("," + average_received);
      average_received = ((double) ticket_request_to_evil_nodes) / num_evil_nodes;
      writer.write("," + average_received);
      writer.write("\n");
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    msgSentPerType.clear(); // = new HashMap<Integer, Integer>();
    ticket_request_to_evil_nodes = 0;
    ticket_request_to_good_nodes = 0;
  }

  public static void reportMsg(Message m, boolean sent) {
    accountMsg(m);
    if (reportMsg == 1) {
      try {
        String result = "";
        if (m.src == null) return; // ignore init messages
        result +=
            m.id + "," + m.messageTypetoString() + "," + m.src.getId() + "," + m.dest.getId() + ",";
        if (m.getType() == Message.MSG_TOPIC_QUERY) {
          result += ((Topic) m.body).topic + ",,,";
        } else if (m.getType() == Message.MSG_REGISTER && m.body instanceof Ticket) {
          int dist = Util.logDistance(((Ticket) m.body).getTopic().getTopicID(), m.dest.getId());
          if (dist < 240) dist = 240;
          result += ((Ticket) m.body).getTopic() + "," + dist + ",,";
        } else if (m.getType() == Message.MSG_REGISTER_RESPONSE && m.body instanceof Ticket) {
          Ticket ticket = (Ticket) m.body;
          if (ticket.isRegistrationComplete()) result += ticket.getTopic() + ",," + "-1" + ",";
          else result += ticket.getTopic() + ",," + ticket.getWaitTime() + ",";

        } else {
          result += ",,,";
        }
        if (sent) {
          result += "sent\n";
        } else {
          result += "received\n";
        }
        msgWriter.write(result);
        msgWriter.flush();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public static void reportBetterWaitingTime(
      String topic, long previousTime, long newTime, long spentTime) {

    Long totalWaitTime = betterTimes.get(topic);
    Integer count = betterTimesCount.get(topic);
    Long time = previousTime - newTime - spentTime;
    if (count == null) {
      betterTimes.put(topic, time);
      betterTimesCount.put(topic, 1);
    } else {
      betterTimes.put(topic, time + totalWaitTime);
      betterTimesCount.put(topic, count + 1);
    }
  }

  public static void reportOverThresholdWaitingTime(
      String topic, BigInteger registrant, BigInteger registrar, long waitingTime) {

    Long totalWaitTime = timeOverThreshold.get(topic);
    Integer count = timeOverThresholdCount.get(topic);
    if (count == null) {
      timeOverThreshold.put(topic, waitingTime);
      timeOverThresholdCount.put(topic, 1);
    } else {
      timeOverThreshold.put(topic, waitingTime + totalWaitTime);
      timeOverThresholdCount.put(topic, count + 1);
    }
    /*try {
           String filename = logFolderName + "/" + "overThresholdWaitingTimes.csv";
           File myFile = new File(filename);
           FileWriter writer;
           if (!myFile.exists()) {
               myFile.createNewFile();
               writer = new FileWriter(myFile, true);
               String title = "time";
               title+= ",registrant,registrar,waitingTime";
               title += "\n";
               writer.write(title);
           }
           else {
               writer = new FileWriter(myFile, true);
           }
           writer.write("" + CommonState.getTime());
           writer.write(","+registrant);
           writer.write(","+registrar);
           writer.write(","+waitingTime);

           writer.write("\n");
           writer.close();
    }catch (IOException e) {
           e.printStackTrace();
       }*/

  }

  private void write_better_waiting_time() {
    try {
      if (all_topics == null) return;

      String filename = logFolderName + "/" + "betterWaitingTimes.csv";
      File myFile = new File(filename);
      FileWriter writer;

      if (!myFile.exists()) {
        myFile.createNewFile();
        writer = new FileWriter(myFile, true);
        String title = "time";
        for (String topic : all_topics) {
          title += "," + topic + "_timeDifference";
          title += "," + topic + "_count";
        }

        title += "\n";
        writer.write(title);
      } else {
        writer = new FileWriter(myFile, true);
      }
      writer.write("" + CommonState.getTime());

      for (String topic : all_topics) {
        if (betterTimesCount.get(topic) != null) {
          writer.write("," + betterTimes.get(topic) / betterTimesCount.get(topic));
          writer.write("," + betterTimesCount.get(topic));
        } else {
          writer.write("," + 0);
          writer.write("," + 0);
        }
      }

      writer.write("\n");
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void write_overthreshold_time() {
    try {
      if (all_topics == null) return;

      String filename = logFolderName + "/" + "overThresholdWaitingTimes.csv";
      File myFile = new File(filename);
      FileWriter writer;
      if (!myFile.exists()) {
        myFile.createNewFile();
        writer = new FileWriter(myFile, true);
        String title = "time";
        for (String topic : all_topics) {
          title += "," + topic + "_waitingtime";
          title += "," + topic + "_count";
        }

        title += "\n";
        writer.write(title);
      } else {
        writer = new FileWriter(myFile, true);
      }
      writer.write("" + CommonState.getTime());

      for (String topic : all_topics) {
        if (timeOverThresholdCount.get(topic) != null) {
          writer.write("," + timeOverThreshold.get(topic) / timeOverThresholdCount.get(topic));
          writer.write("," + timeOverThresholdCount.get(topic));
        } else {
          writer.write("," + 0);
          writer.write("," + 0);
        }
      }

      writer.write("\n");
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void write_msg_received_by_nodes() {

    try {
      String filename = this.logFolderName + "/" + "msg_received.csv";
      FileWriter writer = new FileWriter(filename);
      // get all the keys we store in the hashmap
      Entry<BigInteger, HashMap<String, Integer>> entry =
          perNodeStats.entrySet().stream().findFirst().get();
      Object[] keys = entry.getValue().keySet().toArray();

      String title = "Node";
      for (Object key : keys) {
        title += "," + key;
      }
      title += "\n";
      writer.write(title);

      for (BigInteger id : perNodeStats.keySet()) {
        HashMap<String, Integer> msgStats = perNodeStats.get(id);

        String toWrite = String.valueOf(id);
        for (Object key : keys) {
          toWrite += "," + msgStats.get(key);
        }
        toWrite += "\n";
        writer.write(toWrite);
      }

      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void write_register_overhead() {

    if (!(kadProtocol instanceof Discv5Protocol)) return;

    try {
      String filename = KademliaObserver.logFolderName + "/" + "register_overhead.csv";
      FileWriter writer = new FileWriter(filename);
      if (all_topics == null) {
        writer.close();
        return;
      }
      // write the header
      String header = "topic, registerMessages, registrations, ratio\n";
      writer.write(header);

      int totalMsgs = 0;
      int totalnoReg = 0;
      for (String topic : all_topics) {
        int noMsg = registerOverhead.get(topic) != null ? registerOverhead.get(topic) : 0;
        int noReg = numberOfRegistrations.get(topic) != null ? numberOfRegistrations.get(topic) : 0;
        double ratio = ((double) noMsg) / noReg;
        writer.write(topic + ", " + noMsg + ", " + noReg + ", " + ratio + "\n");
        // track msgs/regs for all topics
        totalMsgs += noMsg;
        totalnoReg += noReg;
      }
      writer.write(
          "all"
              + ", "
              + totalMsgs
              + ", "
              + totalnoReg
              + ", "
              + ((double) totalMsgs) / totalnoReg
              + "\n");
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void write_topics() {
    if (topicsList.size() == 0) return;

    TreeMap<String, Integer> tm = new TreeMap<String, Integer>(topicsList);

    try {
      String filename = this.logFolderName + "/" + "topics.csv";
      File myFile = new File(filename);
      FileWriter writer;
      if (!myFile.exists()) {
        myFile.createNewFile();
        writer = new FileWriter(myFile, true);
        String title = "time";
        for (String topic : tm.keySet()) {
          title += "," + topic;
        }
        title += "\n";
        writer.write(title);
      } else {
        writer = new FileWriter(myFile, true);
      }
      writer.write("" + CommonState.getTime());
      for (String topic : tm.keySet()) {
        writer.write("," + tm.get(topic));
        // activeRegistrations.put(topic, 0);
      }
      writer.write("\n");
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void write_registration_stats() {
    if (activeRegistrations.size() == 0) return;
    try {
      String filename = this.logFolderName + "/" + "registration_stats.csv";
      File myFile = new File(filename);
      FileWriter writer;
      if (!myFile.exists()) {
        myFile.createNewFile();
        writer = new FileWriter(myFile, true);
        String title = "time";
        if (all_topics == null) return;

        for (String topic : all_topics) {
          // System.out.println("Kademliaobserver stats Topic "+topic);
          title += "," + topic + "-normal";
          title += "," + topic + "-evil";
        }
        title += "\n";
        writer.write(title);
      } else {
        writer = new FileWriter(myFile, true);
      }
      writer.write("" + CommonState.getTime());
      for (String topic : all_topics) {
        if (activeRegistrations.get(topic) != null)
          writer.write("," + activeRegistrations.get(topic));
        else writer.write(",0");
        Integer maliciousRegistrants = activeRegistrationsByMalicious.get(topic);
        if (maliciousRegistrants == null) writer.write(",0");
        else {
          writer.write("," + activeRegistrationsByMalicious.get(topic));
          // activeRegistrationsByMalicious.put(topic, 0);
        }
        // activeRegistrations.put(topic, 0);
      }
      writer.write("\n");
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void write_node_info() {
    try {
      String filename = this.logFolderName + "/" + "node_information.csv";
      File myFile = new File(filename);
      FileWriter writer;
      if (!myFile.exists()) {
        // if (nodeInfo.size() < Network.size())
        //    return;
        myFile.createNewFile();
        writer = new FileWriter(myFile, true);
        writer.write(
            "nodeID,topicID,is_evil?,numInConnections,numOutConnections,numEvilOutConnections,numEvilInConnections\n");
      } else {

        writer = new FileWriter(myFile, true);
      }

      for (int i = 0; i < Network.size(); i++) {
        Node node = Network.get(i);
        kadProtocol = node.getKademliaProtocol();
        BigInteger id = kadProtocol.getNode().getId();
        if (writtenNodeIDs.contains(id)) continue;
        int is_evil = kadProtocol.getNode().is_evil ? 1 : 0;
        writer.write(id + "," + nodeInfo.get(id) + "," + is_evil + ",");
        // FIXME
        writer.write("0,");
        writer.write("0,");
        // writer.write(kadProtocol.getNode().getTotalIncomingConnections().size() + ",");
        // writer.write(kadProtocol.getNode().getTotalOutgoingConnections().size() + ",");
        int numEvil = 0;
        /*for(KademliaNode n : kadProtocol.getNode().getTotalIncomingConnections()) {
            if (n.is_evil)
                numEvil++;
        }*/
        writer.write(numEvil + ",");
        numEvil = 0;
        // FIXME
        /*for(KademliaNode n : kadProtocol.getNode().getTotalOutgoingConnections()) {
            if (n.is_evil)
                numEvil++;
        }*/
        writer.write(numEvil + "\n");

        writtenNodeIDs.add(id);
      }
      writer.close();
    } catch (IOException e) {

      e.printStackTrace();
    }
  }

  private void write_registered_topics_timing() {

    try {
      String filename = this.logFolderName + "/" + "registeredTopicsTime.csv";
      File myFile = new File(filename);
      FileWriter writer;
      if (myFile.exists()) myFile.delete();
      myFile.createNewFile();
      writer = new FileWriter(myFile, true);
      writer.write(
          "topic,registrant,times_registered,min_registration_time,average_registration_time,min_discovery_time,times_discovered\n");
      for (String t : registeredTopics.keySet()) {
        for (RegistrationLog reg : registeredTopics.get(t).values()) {
          writer.write(t);
          writer.write(",");
          writer.write(String.valueOf(reg.getRegistrant()));
          writer.write(",");
          writer.write(String.valueOf(reg.getRegistered().size()));
          writer.write(",");
          writer.write(String.valueOf(reg.getMinRegisterTime()));
          writer.write(",");
          writer.write(String.valueOf(reg.getAvgRegisterTime()));
          writer.write(",");
          //            		writer.write(String.valueOf(reg.getMinDiscoveryTime()));
          //            		writer.write(",");
          //            		writer.write(String.valueOf(reg.getAvgDiscoveryTime()));
          //    	        	writer.write("\n");
          writer.write(String.valueOf(reg.getAvgFirstDiscoveryTime()));
          writer.write(",");
          writer.write(String.valueOf(reg.getTimesDiscovered()));
          writer.write("\n");
        }
      }
      writer.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /* private void write_registered_topics_average() {
  	try {
       String filename = this.logFolderName + "/" + "registeredTopics.csv";
       File myFile = new File(filename);
       FileWriter writer;
       if (myFile.exists())myFile.delete();
       myFile.createNewFile();
       writer = new FileWriter(myFile, true);
       writer.write("topic,count\n");
       for(Topic t : regByTopic.keySet()) {
       	writer.write(t.topic);
       	writer.write(",");
       	writer.write(String.valueOf(regByTopic.get(t).intValue()/simpCounter));
       	writer.write("\n");
       }
   	writer.close();

  	}catch(IOException e) {
  		//e.printStackTrace();
  	}


  }*/

  private void write_registered_registrar_average() {
    try {
      String filename = this.logFolderName + "/" + "registeredRegistrar.csv";
      File myFile = new File(filename);
      FileWriter writer;
      if (myFile.exists()) myFile.delete();
      myFile.createNewFile();
      writer = new FileWriter(myFile, true);
      writer.write("topic,nodeId,count,evil\n");

      for (String t : regByRegistrar.keySet()) {
        for (BigInteger n : regByRegistrar.get(t).keySet()) {
          if (avgCounter.get(n) != null) {
            writer.write(String.valueOf(t));
            writer.write(",");
            writer.write(String.valueOf(n));
            writer.write(",");
            /*System.out.println(CommonState.getTime()+" Registrar write "+t+" "+String.valueOf((double)regByRegistrar.get(t).get(n).intValue()/avgCounter.get(n)));
            if((double)regByRegistrar.get(t).get(n).intValue()/avgCounter.get(n)<1&&(double)regByRegistrar.get(t).get(n).intValue()/avgCounter.get(n)>0)
            	writer.write("1");
            else*/
            // writer.write(String.valueOf((double)regByRegistrar.get(t).get(n).intValue()/avgCounter.get(n)));
            writer.write(String.valueOf((double) regByRegistrar.get(t).get(n).intValue()));
            writer.write(",");
            writer.write(String.valueOf((double) regByRegistrarEvil.get(t).get(n).intValue()));
            writer.write("\n");
          }
        }
      }
      writer.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void write_competingtickets() {
    try {
      String filename = this.logFolderName + "/" + "competingtickets.csv";
      File myFile = new File(filename);
      FileWriter writer;
      if (myFile.exists()) myFile.delete();
      myFile.createNewFile();
      writer = new FileWriter(myFile, true);
      writer.write("topic,nodeId,bucket,count\n");

      // System.out.println("Competing tickets");

      for (String t : competingTickets.keySet()) {
        for (BigInteger n : competingTickets.get(t).keySet()) {
          // System.out.println("Competing tickets topic "+t+"
          // "+competingTickets.get(t).get(n).intValue());
          if (avgCounter.get(n) != null) {
            writer.write(String.valueOf(t));
            writer.write(",");
            Topic topic = new Topic(t);
            int dist = Util.logDistance(topic.getTopicID(), n);
            writer.write(String.valueOf(n));
            writer.write(",");
            writer.write(String.valueOf(dist));
            writer.write(",");
            writer.write(String.valueOf(competingTickets.get(t).get(n).intValue()));
            writer.write("\n");
          }
        }
      }
      writer.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void write_registered_registrant_average() {
    try {
      String filename = this.logFolderName + "/" + "registeredRegistrant.csv";
      File myFile = new File(filename);
      FileWriter writer;
      if (myFile.exists()) myFile.delete();
      myFile.createNewFile();
      writer = new FileWriter(myFile, true);
      writer.write("topic,nodeId,count,evil\n");

      for (String t : regByRegistrant.keySet()) {
        for (BigInteger n : regByRegistrant.get(t).keySet()) {
          // System.out.println("Re "+t.getTopic()+" "+regByRegistrar.get(t)/avgCounter);
          if (avgCounter.get(n) != null) {
            writer.write(String.valueOf(t));
            writer.write(",");
            writer.write(String.valueOf(n));
            writer.write(",");
            writer.write(
                String.valueOf(regByRegistrant.get(t).get(n).intValue() / avgCounter.get(n)));
            // writer.write(String.valueOf(regByRegistrant.get(t).get(n).intValue()));
            writer.write(",");
            writer.write(String.valueOf(regByRegistrantEvil.get(t).get(n).intValue()));
            writer.write("\n");
          }
        }
      }

      writer.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void write_eclipsing_results() {

    try {
      String filename = this.logFolderName + "/" + "eclipse_counts.csv";
      File myFile = new File(filename);
      FileWriter writer;
      if (all_topics == null) return;

      if (!myFile.exists()) {
        myFile.createNewFile();
        writer = new FileWriter(myFile, true);
        writer.write("time,");
        for (String t : all_topics) {
          writer.write("PercentEvilDiscovered-" + t + ",");
        }
        for (String t : all_topics) {
          writer.write("PercentEclipsed-" + t + ",");
        }
        // writer.write("EclipsedNodes,UnEclipsedNodes,EvilNodes\n");
        writer.write("PercentEclipsedOperations\n");
      } else {
        writer = new FileWriter(myFile, true);
      }

      writer.write(CommonState.getTime() + ",");
      // Percentage of discovered nodes that are evil
      for (String t : all_topics) {
        Double percent = percentEclipsedDiscoveredInLookupOperations.get(t);
        if (percent != null) {
          Integer numOps = numLookupOperationsPerTopic.get(t);
          if (numOps == 0) percent = 0.0;
          else percent = percent / numOps;
          writer.write(String.valueOf(percent) + ",");
        } else writer.write("0,");
      }
      Integer totalEclipsed = 0;
      Integer totalOperations = 0;
      // Percentage of eclipsed nodes
      for (String t : all_topics) {
        Integer numEclipsed = numEclipsedLookupOperations.get(t);
        if (numEclipsed != null) {
          totalEclipsed += numEclipsed;
          Integer numOps = numLookupOperationsPerTopic.get(t);
          Double percent = (1.0 * numEclipsed) / numOps;
          if (numOps == 0) percent = 0.0;
          writer.write(String.valueOf(percent) + ",");
        } else {
          writer.write("0,");
        }
        Integer numOps = numLookupOperationsPerTopic.get(t);
        if (numOps != null) totalOperations += numOps;
      }
      if (totalOperations == 0) {
        totalOperations = 1;
        totalEclipsed = 0;
      }
      Double percentEclipsed = (1.0 * totalEclipsed) / totalOperations;
      if (totalOperations == 0) percentEclipsed = 0.0;
      writer.write(String.valueOf(percentEclipsed) + "\n");
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    numEclipsedLookupOperations.clear();
    numLookupOperationsPerTopic.clear();
    percentEclipsedDiscoveredInLookupOperations.clear();
  }

  /*private boolean is_eclipsed(KademliaNode node) {
      if (node.is_evil)
          //Don't include malicious nodes in the count
          return false;

      if (node.getOutgoingConnections().size() == 0)
          return false;

      for (KademliaNode outConn : node.getOutgoingConnections())
          if (!outConn.is_evil)
              return false;

      return true;
  }*/

  /** write the snapshot of average storage utilisation in the topic tables for each topic. */
  private void write_average_storage_utilisation_per_topic() {

    if (!(kadProtocol instanceof Discv5Protocol)) return;

    HashMap<String, Double> utilisations = new HashMap<String, Double>();
    HashMap<String, Integer> ticketQLength = new HashMap<String, Integer>();
    double evil_total_utilisations = 0;
    HashMap<Topic, Integer> topics;
    HashMap<Topic, Integer> waitingTickets;

    int numUpGoodNodes = 0;
    int numUpEvilNodes = 0;
    int num_all_registrations = 0;
    for (int i = 0; i < Network.size(); i++) {
      Node node = Network.get(i);
      if (!(node.isUp())) continue;
      kadProtocol = node.getKademliaProtocol();
      if (!kadProtocol.getNode().is_evil) {

        numUpGoodNodes += 1;
        topics = ((Discv5Protocol) kadProtocol).topicTable.getRegbyTopic();

        int total_occupancy_topic_table = 0;
        for (Topic t : topics.keySet()) {
          total_occupancy_topic_table += topics.get(t);
          num_all_registrations += topics.get(t);
        }

        for (Topic t : topics.keySet()) {
          int count = topics.get(t);
          double util;

          util = ((double) count) / KademliaCommonConfig.TOPIC_TABLE_CAP;
          if (utilisations.get(t.getTopic()) != null) {
            double total_util_so_far = utilisations.get(t.getTopic());
            utilisations.put(t.getTopic(), total_util_so_far + util);
          } else utilisations.put(t.getTopic(), util);
        }

      } else { // evil node
        KademliaProtocol evilKadProtocol = node.getKademliaProtocol();
        double topicTableUtil =
            ((Discv5Protocol) evilKadProtocol).topicTable.topicTableUtilisation();
        evil_total_utilisations += topicTableUtil;
        numUpEvilNodes++;
      }
    }

    if (utilisations.size() == 0) return;

    try {
      String filename = this.logFolderName + "/" + "storage_utilisation.csv";
      File myFile = new File(filename);
      FileWriter writer;
      String[] keys = (String[]) utilisations.keySet().toArray(new String[utilisations.size()]);
      Arrays.sort(keys);
      if (!myFile.exists()) {
        myFile.createNewFile();
        writer = new FileWriter(myFile, true);
        String title = "time";
        for (String topic : keys) {
          title += "," + topic;
        }

        title += ",overallUtil";
        title += ",overallEvilUtil";
        title += "\n";
        writer.write(title);
      } else {
        writer = new FileWriter(myFile, true);
      }
      writer.write("" + CommonState.getTime());
      for (String topic : keys) {
        double util = utilisations.get(topic) / numUpGoodNodes;
        writer.write("," + util);
      }
      writer.write(
          ","
              + ((double) num_all_registrations)
                  / (numUpGoodNodes * KademliaCommonConfig.TOPIC_TABLE_CAP));
      if (numUpEvilNodes > 0) writer.write("," + evil_total_utilisations / numUpEvilNodes);
      else writer.write(",0");

      writer.write("\n");
      writer.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * print the statistical snapshot of the current situation
   *
   * @return boolean always false
   */
  public boolean execute() {
    // System.out.println(CommonState.getTime()+" execute");
    System.gc();

    if (CommonState.getIntTime() == 0) {
      return false;
    }
    try {

      topicsList.clear();
      // regByRegistrant = new HashMap<>();
      // regByRegistrantEvil = new HashMap<>();
      // regByRegistrar = new HashMap<>();
      // regByRegistrarEvil = new HashMap<>();

      for (int i = 0; i < Network.size(); i++) {

        Node node = Network.get(i);
        kadProtocol = node.getKademliaProtocol();
        if (!(node.isUp())) {
          for (String t : regByRegistrar.keySet())
            regByRegistrar.get(t).remove(kadProtocol.getNode().getId());

          for (String t : regByRegistrant.keySet())
            regByRegistrant.get(t).remove(kadProtocol.getNode().getId());

          for (String t : regByRegistrantEvil.keySet())
            regByRegistrantEvil.get(t).remove(kadProtocol.getNode().getId());

          for (String t : regByRegistrarEvil.keySet())
            regByRegistrarEvil.get(t).remove(kadProtocol.getNode().getId());

          for (String t : competingTickets.keySet())
            competingTickets.get(t).remove(kadProtocol.getNode().getId());

          continue;
        }
        int c = 0;
        if (avgCounter.get(kadProtocol.getNode().getId()) != null) {
          c = avgCounter.get(kadProtocol.getNode().getId());
        }
        c++;
        avgCounter.put(kadProtocol.getNode().getId(), c);
        HashMap<Topic, Integer> topics = new HashMap<Topic, Integer>();

        /*for(Topic t: topics.keySet()) {
        int count = 0;
        if(regByTopic.get(t)!=null)count=regByTopic.get(t);
        count+=topics.get(t);
        regByTopic.put(t, count);
          }*/

        if (kadProtocol instanceof Discv5Protocol) {

          topics = ((Discv5Protocol) kadProtocol).topicTable.getRegbyTopic();

          List<String> topicsregistering = ((Discv5Protocol) kadProtocol).getRegisteringTopics();
          for (String t : topicsregistering) {
            int n = 1;
            if (topicsList.get(t) != null) {
              n += topicsList.get(t);
            }
            topicsList.put(t, n);
          }

          if (reportReg == 1) {
            String filename =
                this.logFolderName + "/" + CommonState.getTime() + "_registrations.csv";
            File myFile = new File(filename);
            FileWriter writer;
            if (!myFile.exists()) {
              myFile.createNewFile();
              writer = new FileWriter(myFile, true);
              writer.write("host,topic,registrant,timestamp\n");
            } else {
              writer = new FileWriter(myFile, true);
            }

            String registrations = ((Discv5Protocol) kadProtocol).topicTable.dumpRegistrations();
            writer.write(registrations);
            writer.close();
          }

          // topic table registrations by registrar
          int count;

          HashMap<String, Integer> registrars =
              ((Discv5Protocol) kadProtocol).topicTable.getRegbyRegistrar();
          for (String topic : registrars.keySet()) {
            count = 0;
            // System.out.println(CommonState.getTime()+" Registrar topic "+topic+"
            // "+registrars.get(topic)+" regs.");
            /*if(regByRegistrar.get(topic)!=null) {
            	if(regByRegistrar.get(topic).get(kadProtocol.getNode().getId())!=null)
            		count = regByRegistrar.get(topic).get(kadProtocol.getNode().getId());
            }*/
            count += registrars.get(topic);
            if (regByRegistrar.get(topic) == null) {
              HashMap<BigInteger, Integer> tmp = new HashMap<BigInteger, Integer>();
              // tmp.put(kadProtocol.getNode().getId(), count);
              tmp.put(kadProtocol.getNode().getId(), registrars.get(topic));
              regByRegistrar.put(topic, tmp);
            } else regByRegistrar.get(topic).put(kadProtocol.getNode().getId(), count);
            // regByRegistrar.get(topic).put(kadProtocol.getNode().getId(), registrars.get(topic));

          }

          HashMap<String, Integer> registrarsEvil =
              ((Discv5Protocol) kadProtocol).topicTable.getRegEvilbyRegistrar();
          for (String topic : registrarsEvil.keySet()) {
            count = 0;
            count += registrarsEvil.get(topic);
            if (regByRegistrarEvil.get(topic) == null) {
              HashMap<BigInteger, Integer> tmp = new HashMap<BigInteger, Integer>();
              // tmp.put(kadProtocol.getNode().getId(), count);
              tmp.put(kadProtocol.getNode().getId(), registrarsEvil.get(topic));
              regByRegistrarEvil.put(topic, tmp);
            } else regByRegistrarEvil.get(topic).put(kadProtocol.getNode().getId(), count);
            // regByRegistrar.get(topic).put(kadProtocol.getNode().getId(), registrars.get(topic));

          }
          if (kadProtocol instanceof Discv5TicketProtocol
              || kadProtocol instanceof Discv5DHTTicketProtocol) {
            HashMap<String, Integer> competing;
            competing =
                ((Discv5GlobalTopicTable) ((Discv5Protocol) kadProtocol).topicTable)
                    .getCompetingTickets();

            for (String topic : competing.keySet()) {
              if (competingTickets.get(topic) == null) {
                HashMap<BigInteger, Integer> tmp = new HashMap<BigInteger, Integer>();
                tmp.put(kadProtocol.getNode().getId(), competing.get(topic));
                competingTickets.put(topic, tmp);
              } else
                competingTickets
                    .get(topic)
                    .put(kadProtocol.getNode().getId(), competing.get(topic));
            }
          }

          // topic table registrations by registrant
          HashMap<String, HashMap<BigInteger, Integer>> tmpReg =
              ((Discv5Protocol) kadProtocol).topicTable.getRegbyRegistrant();
          for (String topic : tmpReg.keySet()) {
            for (BigInteger id : tmpReg.get(topic).keySet()) {

              // System.out.println(CommonState.getTime()+" Registrant
              // "+kadProtocol.getNode().getId()+" topic "+topic+" node:"+id+"
              // "+tmpReg.get(topic).get(id)+" regs.");
              count = 0;
              if (regByRegistrant.get(topic) != null) {
                if (regByRegistrant.get(topic).get(id) != null) {
                  count = regByRegistrant.get(topic).get(id);
                }
              }
              count += tmpReg.get(topic).get(id);

              // System.out.println(CommonState.getTime()+" Registrant
              // "+kadProtocol.getNode().getId()+" topic "+topic+" node:"+id+" "+count+" regs.");

              if (regByRegistrant.get(topic) == null) {
                HashMap<BigInteger, Integer> tmp = new HashMap<BigInteger, Integer>();
                // tmp.put(id, count);
                tmp.put(id, count);
                regByRegistrant.put(topic, tmp);
              } else {
                // regByRegistrant.get(topic).put(id, tmpReg.get(topic).get(id));
                regByRegistrant.get(topic).put(id, count);
              }
            }
          }

          // topic table registrations by registrant
          HashMap<String, HashMap<BigInteger, Integer>> tmpRegEvil =
              ((Discv5Protocol) kadProtocol).topicTable.getRegEvilbyRegistrant();
          for (String topic : tmpRegEvil.keySet()) {
            for (BigInteger id : tmpRegEvil.get(topic).keySet()) {

              // System.out.println(CommonState.getTime()+" Registrant
              // "+kadProtocol.getNode().getId()+" topic "+topic+" node:"+id+"
              // "+tmpReg.get(topic).get(id)+" regs.");
              count = 0;
              if (regByRegistrantEvil.get(topic) != null) {
                if (regByRegistrantEvil.get(topic).get(id) != null) {
                  count = regByRegistrantEvil.get(topic).get(id);
                }
              }
              count += tmpRegEvil.get(topic).get(id);

              // System.out.println(CommonState.getTime()+" Registrant
              // "+kadProtocol.getNode().getId()+" topic "+topic+" node:"+id+" "+count+" regs.");

              if (regByRegistrantEvil.get(topic) == null) {
                HashMap<BigInteger, Integer> tmp = new HashMap<BigInteger, Integer>();
                // tmp.put(id, count);
                tmp.put(id, count);
                regByRegistrantEvil.put(topic, tmp);
              } else {
                // regByRegistrant.get(topic).put(id, tmpReg.get(topic).get(id));
                regByRegistrantEvil.get(topic).put(id, count);
              }
            }
          }
        }
      }

    } catch (IOException e) {

      e.printStackTrace();
    }

    write_registered_registrant_average();
    // write_registered_topics_average();
    write_registered_registrar_average();
    write_better_waiting_time();
    write_overthreshold_time();
    if (CommonState.getTime() > KademliaCommonConfig.AD_LIFE_TIME) {
      // write these stats once all the register msgs are sent (all_topics list is finalised)
      write_eclipsing_results();
      write_registration_stats();
      write_waiting_times();
      write_average_storage_utilisation_per_topic();
    }
    // if(CommonState.getTime() > 3000000)
    //    write_node_info();
    write_registered_topics_timing();
    write_exchanged_msg_stats_over_time();
    if (CommonState.getEndTime() <= (this.observerStep + CommonState.getTime())) {
      // Last execute cycle of the experiment
      write_msg_received_by_nodes();
      write_register_overhead();
    }
    if (CommonState.getTime() > 100000) write_topics();
    write_competingtickets();
    return false;
  }
}
