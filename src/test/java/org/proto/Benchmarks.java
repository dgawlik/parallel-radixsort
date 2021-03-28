package org.proto;


import java.util.Random;
import org.junit.jupiter.api.Test;

public class Benchmarks {


  @Test
  void test_split_time(){
    int SIZE = 20_000_000;
    Random rnd = new Random(1);

    long[] arr = new long[SIZE];

    for(int i=0;i<SIZE;i++){
      long val = rnd.nextLong() >>> 1;
      arr[i] = val;
    }

    var start = System.currentTimeMillis();
    int numProc = Runtime.getRuntime().availableProcessors();
    int partL = (int)Math.ceil(arr.length/(double)numProc);
    long[][] work = new long[numProc][];
    for(int i=0;i<numProc;i++){
      if(i<numProc-1){
        work[i] = new long[partL];
      }
      else {
        work[i] = new long[arr.length % partL == 0 ? partL : arr.length % partL ];
      }
    }

    for(int i=0;i<numProc;i++){
//      for(int j=i*partL;j<i*partL+work[i].length;j++){
//        work[i][j-i*partL] = arr[j];
//      }
      System.arraycopy(arr,i*partL, work[i], 0, work[i].length);
    }
    var end = System.currentTimeMillis();
    System.out.println((end-start) + " ms");
  }
}
