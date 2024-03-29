package peersim.kademlia.das.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.MissingNode;
import peersim.kademlia.das.Parcel;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.SearchTable;

public class ValidatorSamplingOperationDHT extends ValidatorSamplingOperation {

  protected HashMap<BigInteger, Boolean> parcels;

  public ValidatorSamplingOperationDHT(
      BigInteger srcNode,
      long timestamp,
      Block block,
      SearchTable searchTable,
      int row,
      int column,
      boolean isValidator,
      int numValidators,
      MissingNode callback) {
    super(
        srcNode, timestamp, block, searchTable, row, column, isValidator, numValidators, callback);

    // System.out.println("Row " + row + " column " + column);
    assert (row == 0 || column == 0) : "Either row or column should be set";
    assert (!(row == 0 && column == 0)) : "Both row or column are set";

    this.parcels = new HashMap<>();
    this.row = row;
    this.column = column;
    if (row > 0) {
      for (Sample sample : block.getSamplesByRow(row)) {
        samples.put(sample.getId(), new FetchingSample(sample));
        // System.out.println(srcNode + " " + sample);
      }
      List<Parcel> list = block.getParcelByRow(row);
      for (Parcel p : list) {
        parcels.put(p.getId(), false);
      }
    } else if (column > 0) {
      for (Sample sample : block.getSamplesByColumn(column)) {
        samples.put(sample.getIdByColumn(), new FetchingSample(sample));
        // System.out.println(srcNode + " " + sample);
      }
      List<Parcel> list = block.getParcelByColumn(column);
      for (Parcel p : list) {
        parcels.put(p.getId(), false);
      }
    }
    this.searchTable = searchTable;
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
    /*for (BigInteger s : samples.keySet()) {
      if (samples.get(s)) {
        System.out.println(srcNode + " " + s);
      }
    }*/
    if (samplesCount >= samples.size() / 2) completed = true;

    // System.out.println("Samples received " + samples.size());
  }
}
