package peersim.kademlia;

import java.util.Random;

/**
 * @author srene
 */
public class BackoffService {

  private int maxRetries;

  private int numberOfTriesLeft;

  private int retries;

  private int slotTime;

  private int timeToWait;

  private final Random random = new Random();

  public BackoffService(int slotTime, int maxRetries) {
    this.maxRetries = maxRetries;
    this.numberOfTriesLeft = maxRetries;
    this.retries = 0;
    this.slotTime = slotTime;
    this.timeToWait = 0;
  }

  public boolean shouldRetry() {
    return numberOfTriesLeft > 0;
  }

  public void registrationFailed() {

    numberOfTriesLeft--;
    retries++;
    int rand = (int) (slotTime * ((Math.pow(2, retries) - 1)));
    if (rand == 0) rand = 1;
    if (rand < 0) rand = slotTime;
    timeToWait = random.nextInt(rand);
    // System.out.println("Backoff time "+rand+" "+timeToWait);

  }

  public int getTimesFailed() {
    return retries;
  }

  public long getTimeToWait() {
    return this.timeToWait;
  }

  public void reset() {
    this.numberOfTriesLeft = maxRetries;
    this.timeToWait = 0;
  }
}
