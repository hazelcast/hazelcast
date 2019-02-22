import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.HashUtil;

public class Main {

    public static void main(String[] args){
        int[] partitionThreadBuckets = new int[12];
        SerializationService ss = new DefaultSerializationServiceBuilder().build();
        for(int k=0;k<20_000;k++){
            String s = ""+k;
            int hash = ss.toData(s).getPartitionHash();
            partitionThreadBuckets[HashUtil.hashToIndex(hash,partitionThreadBuckets.length)]++;
        }

        int total = 0;
        for(int k=0;k<partitionThreadBuckets.length;k++){
            total+=partitionThreadBuckets[k];
        }

        for(int k=0;k<partitionThreadBuckets.length;k++){
            int value = partitionThreadBuckets[k];
            System.out.println("count:"+ value +" percentage: "+((value*100f)/total)+"%");
        }
    }
}
