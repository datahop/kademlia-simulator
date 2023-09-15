package peersim.kademlia.das;

import java.math.BigInteger;

public class Neighbour implements Comparable<Neighbour> {

    BigInteger id;
    int ttl;

    Neighbour(BigInteger id){
        this.id = id;
        this.ttl=0;
    }

    @Override
    public int compareTo(Neighbour arg0) {
        if(this.ttl<arg0.ttl)
            return 1;
        else if(this.ttl==arg0.ttl){
            return 0;
        } else {
            return -1;
        }
    }


    
}
