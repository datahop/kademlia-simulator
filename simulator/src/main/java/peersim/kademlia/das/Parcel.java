package peersim.kademlia.das;

import java.util.HashMap;
import java.math.BigInteger;

public class Parcel{

    HashMap<BigInteger,Sample> samples;
    BigInteger parcelId;

    public Parcel(BigInteger id, int size){
        samples = new HashMap<>();
        parcelId = id;
    }

    public void addSample(Sample s){
        samples.put(s.getId(),s);
    }

    public Sample getSample(BigInteger id){
        return samples.get(id);
    }
}