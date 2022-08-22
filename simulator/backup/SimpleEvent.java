package peersim.kademlia;

import peersim.core.CommonState;

/**
 * This class defines a simple event. A simple event is characterized only by its type.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class SimpleEvent {

  /** The identifier of the type of the event. */
  protected int type;

  public long timestamp;

  public SimpleEvent() {
    this.timestamp = CommonState.getTime();
  }

  /**
   * Initializes the type of the event.
   *
   * @param type The identifier of the type of the event
   */
  public SimpleEvent(int type) {
    this();
    this.type = type;
  }

  /**
   * Gets the type of the event.
   *
   * @return The type of the current event.
   */
  public int getType() {
    return this.type;
  }
}
