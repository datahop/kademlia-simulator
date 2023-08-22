package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.KademliaCommonConfigDas;
import peersim.kademlia.das.MissingNode;
import peersim.kademlia.das.Parcel;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.SearchTable;

public class RandomSamplingOperationDHT extends RandomSamplingOperation {

  protected HashMap<BigInteger, Boolean> parcels;

  public RandomSamplingOperationDHT(
      BigInteger srcNode,
      BigInteger destNode,
      long timestamp,
      Block currentBlock,
      SearchTable searchTable,
      boolean isValidator,
      int numValidators,
      MissingNode callback) {
    super(
        srcNode,
        destNode,
        timestamp,
        currentBlock,
        searchTable,
        isValidator,
        numValidators,
        callback);

    this.parcels = new HashMap<>();

    Sample[] randomSamples = currentBlock.getNRandomSamples(KademliaCommonConfigDas.N_SAMPLES);
    samples.clear();
    for (Sample rs : randomSamples) {
      // samples.put(rs.getId(), false);
      samples.put(rs.getId(), new FetchingSample(rs.getId()));
      Parcel parcel = currentBlock.getParcel(rs.getId());
      parcels.put(parcel.getId(), false);
    }
  }

  public BigInteger[] getParcels() {
    List<BigInteger> result = new ArrayList<>();

    for (BigInteger parcelId : parcels.keySet()) {
      if (!parcels.get(parcelId)) result.add(parcelId);
    }

    return result.toArray(new BigInteger[0]);
  }

  public void receivedParcel(BigInteger id) {
    parcels.remove(id);
    parcels.put(id, true);
  }

  public void elaborateResponse(Sample[] sam) {

    this.available_requests++;
    for (Sample s : sam) {
      if (samples.containsKey(s.getId())) {
        FetchingSample fs = samples.get(s.getId());
        if (!fs.isDownloaded()) {
          fs.setDownloaded();
          samplesCount++;
        }
      }
    }
    // System.out.println("Samples received " + samples.size());
  }
}
