package peersim.kademlia.das;

import peersim.edsim.EDProtocol;
import peersim.kademlia.Message;

public interface DAS extends EDProtocol {

  // logger.warning("Send message removed " + m.ackId);
  protected abstract void handleInitNewBlock(Message m, int myPid);

  protected abstract void handleInitGetSample(Message m, int myPid);

  protected abstract void handleGetSample(Message m, int myPid);

  protected abstract void handleSeedSample(Message m, int myPid);

  protected abstract void handleGetSampleResponse(Message m, int myPid);
}
