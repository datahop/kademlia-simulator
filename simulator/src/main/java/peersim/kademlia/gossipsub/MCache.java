package peersim.kademlia.gossipsub;

import java.math.BigInteger;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Limited size memory store with LRU eviction policy.
 *
 * @author Modified
 * @version 0.2
 */
public class MCache {
  // Maximum capacity of the store
  private final int maxCapacity;

  // LinkedHashMap to maintain insertion order and enable LRU eviction
  private final LinkedHashMap<BigInteger, Object> mem;

  /**
   * Constructor for MCache with a specified maximum capacity.
   *
   * @param maxCapacity The maximum number of entries allowed in the store.
   */
  public MCache(int maxCapacity) {
    this.maxCapacity = maxCapacity;

    // Initialize LinkedHashMap with access-order enabled for LRU eviction
    this.mem =
        new LinkedHashMap<BigInteger, Object>(16, 0.75f, true) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<BigInteger, Object> eldest) {
            // Evict the eldest entry if size exceeds maxCapacity
            return size() > MCache.this.maxCapacity;
          }
        };
  }

  /**
   * Add an object into the memory store.
   *
   * @param id The key of the object to store.
   * @param obj The object to store.
   */
  public void put(BigInteger id, Object obj) {
    synchronized (mem) {
      mem.put(id, obj);
    }
  }

  /**
   * Get a specific object by key.
   *
   * @param key The key to search for.
   * @return The object retrieved, or null if not found.
   */
  public Object get(BigInteger key) {
    synchronized (mem) {
      return mem.get(key);
    }
  }

  /**
   * Get all objects stored in the memory cache.
   *
   * @return A collection of all objects in the store.
   */
  public Collection<Object> getAll() {
    synchronized (mem) {
      return mem.values();
    }
  }

  /**
   * Get all keys stored in the memory cache.
   *
   * @return A collection of all keys in the store.
   */
  public Collection<BigInteger> window() {
    synchronized (mem) {
      return mem.keySet();
    }
  }
}
