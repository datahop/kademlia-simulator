package peersim.transport;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.util.List;
import peersim.core.*;
import peersim.edsim.*;

class NodeLatency {
  public int node;
  public int[] latency; // Latency array
}

public class DASTransport implements Transport {

  // A 2D array for storing specific latencies between each pair of nodes
  private final long[][] latencies;

  // Constructor for initializing the latencies
  public DASTransport(String prefix) {

    int size = Network.size();
    latencies = new long[size][size];

    // Read latencies JSON file
    Gson gson = new Gson();

    try {
      // Define the type for List<NodeLatency>
      Type nodeLatencyListType = new TypeToken<List<NodeLatency>>() {}.getType();

      // Parse the JSON file using Gson
      List<NodeLatency> nodes =
          gson.fromJson(
              new FileReader(
                  Paths.get("/root/DAS-simulator/kademlia-simulator/simulator/config/latency.json")
                      .toFile()),
              nodeLatencyListType);

      // Assign latencies from the parsed JSON data
      for (int i = 0; i < size; i++) {
        NodeLatency entry = nodes.get(i % nodes.size());
        for (int j = 0; j < size; j++) {
          int latency = entry.latency[j % entry.latency.length];
          if (latency >= 10) {

            latencies[i][j] = latency;
          } else {
            latencies[i][j] = 10; // No latency to itself
          }
        }
      }

    } catch (IOException e) {
      System.err.println("Error reading the JSON file!");
      e.printStackTrace();
    }
  }

  // Method to send a message between two nodes with custom latency
  public long getLatency(Node src, Node dest) {
    // Get latency from the 2D array
    return latencies[(int) src.getID()][(int) dest.getID()];
  }

  public Object clone() {
    return this;
  }

  public void send(Node src, Node dest, Object msg, int pid) {
    long delay = getLatency(src, dest);
    EDSimulator.add(delay, msg, dest, pid);
  }
}
