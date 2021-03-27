package org.radix.perf;

import java.util.Arrays;
import java.util.Random;
import org.radix.RadixSortPrototype;

public class RadixSortPerformanceTest {


  public static void main(String[] args) {
    int SIZE = 20_000_000;
    Random rnd = new Random(1);

    long[] q = new long[SIZE];
    long[] r = new long[SIZE];

    for (int i = 0; i < SIZE; i++) {
      long val = rnd.nextLong() >>> 1;
      q[i] = val;
      r[i] = val;
    }

    if(args[0].equals("java")){
      long qStart = System.currentTimeMillis();
      Arrays.parallelSort(q);
      long qEnd = System.currentTimeMillis();
      System.out.println("Java: " + (qEnd - qStart) + " ms");
    }
    else if(args[0].equals("serial")){
      long rStart = System.currentTimeMillis();
      RadixSortPrototype.sort(r);
      long rEnd = System.currentTimeMillis();
      System.out.println("Radixsort serial: " + (rEnd - rStart) + " ms");
    }
    else if(args[0].equals("parallel")){
      long rStart = System.currentTimeMillis();
      RadixSortPrototype.parallelSort(r);
      long rEnd = System.currentTimeMillis();
      System.out.println("Radixsort parallel: " + (rEnd - rStart) + " ms");
    }
  }

}
