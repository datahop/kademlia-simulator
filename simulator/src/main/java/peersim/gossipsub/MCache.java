package peersim.gossipsub;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Timeout functionnality for the memory store
 *
 * @author Deisss (LGPLv3)
 * @version 0.1
 */
class TimeoutMemoryStore extends TimerTask {
  private BigInteger id;
  private MCache kv;

  /**
   * Constructor
   *
   * @param key The key to use & store
   */
  public TimeoutMemoryStore(BigInteger id, MCache kv) {
    this.id = id;
    this.kv = kv;
  }

  @Override
  public void run() {
    if (this.id != null) {
      kv.delete(this.id);
    }
  }
}

/**
 * The memory store will keep data inside the class, for a specific amout of time
 *
 * @author Deisss (LGPLv3)
 * @version 0.1
 */
public class MCache {
  // This will store any kind of object, related to a specific key value in string
  private HashMap<BigInteger, Object> mem;

  public MCache() {
    mem = new HashMap<>();
  }
  /**
   * Add an object into the memory store
   *
   * @param id The key of the object to store
   * @param obj The object to store
   */
  public void put(BigInteger id, Object obj) {
    put(id, obj, 0);
  }
  /**
   * Add an object into the memory store
   *
   * @param obj The object to store
   * @param timeout The delay in ms
   */
  public void put(BigInteger id, Object obj, long timeout) {
    // If the system is not functionnal
    if (mem == null) {
      erase();
    }

    mem.put(id, obj);

    // Create a delete operation after timeout
    if (timeout > 0) {
      new Timer().schedule(new TimeoutMemoryStore(id, this), timeout);
    }
  }

  /**
   * Get a specific object regarding key
   *
   * @param key The key to search
   * @return The object retrieve, or null if nothing found
   */
  public Object get(BigInteger key) {
    if (mem.containsKey(key)) {
      return mem.get(key);
    } else {
      return null;
    }
  }

  /**
   * Get all objectrs
   *
   * @return All objects in the store
   */
  public Collection<Object> getAll() {
    return mem.values();
  }

  /**
   * Get all objectrs
   *
   * @return All objects in the store
   */
  public Collection<BigInteger> window() {
    return mem.keySet();
  }

  /**
   * Delete an entry from the memory store
   *
   * @param key The key to delete
   * @return The delete value result (true if the object has been found, false in other case)
   */
  public boolean delete(BigInteger key) {
    if (mem.containsKey(key)) {
      mem.remove(key);
      return true;
    }
    return false;
  }

  /** Empty the memory store */
  public void erase() {
    mem = new HashMap<BigInteger, Object>();
  }

  /** Get occupancy */
  public int occupancy() {
    return mem.size();
  }
}
