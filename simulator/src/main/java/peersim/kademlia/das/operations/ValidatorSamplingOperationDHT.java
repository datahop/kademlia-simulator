package peersim.kademlia.das.operations;

import java.math.BigInteger;
import peersim.kademlia.das.Block;
import peersim.kademlia.das.KademliaCommonConfigDas;
import peersim.kademlia.das.MissingNode;
import peersim.kademlia.das.Sample;
import peersim.kademlia.das.SearchTable;

public class ValidatorSamplingOperationDHT extends ValidatorSamplingOperation {

  public ValidatorSamplingOperationDHT(
      BigInteger srcNode,
      long timestamp,
      Block block,
      SearchTable searchTable,
      int row,
      int column,
      boolean isValidator,
      MissingNode callback) {
    super(srcNode, timestamp, block, searchTable, row, column, isValidator, callback);

    // System.out.println("Row " + row + " column " + column);
    assert (row == 0 || column == 0) : "Either row or column should be set";
    assert (!(row == 0 && column == 0)) : "Both row or column are set";

    this.row = row;
    this.column = column;
    if (row > 0) {
      for (BigInteger sample : block.getSamplesIdsByRow(row)) {
        samples.put(sample, false);
        // System.out.println(srcNode + " " + sample);
      }
    } else if (column > 0) {
      for (BigInteger sample : block.getSamplesIdsByRow(column)) {
        samples.put(sample, false);
        // System.out.println(srcNode + " " + sample);
      }
    }
    this.searchTable = searchTable;
    setAvailableRequests(KademliaCommonConfigDas.ALPHA);
  }

  public void elaborateResponse(Sample[] sam) {

    this.available_requests++;
    for (Sample s : sam) {
      if (samples.containsKey(s.getId())) {
        if (!samples.get(s.getId())) {
          samples.remove(s.getId());
          samples.put(s.getId(), true);
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
