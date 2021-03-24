package org.radix;

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class RadixSortPerformanceTest {

  @Test
  void vs_java() {
    int SIZE = 20_000_000;
    Random rnd = new Random(1);

    long[] q = new long[SIZE];
    long[] r = new long[SIZE];

    for (int i = 0; i < SIZE; i++) {
      long val = rnd.nextLong() >>> 1;
      q[i] = val;
      r[i] = val;
    }

    long qStart = System.currentTimeMillis();
    Arrays.parallelSort(q);
    long qEnd = System.currentTimeMillis();

    long rStart = System.currentTimeMillis();
    RadixSort.parallelSort(r);
    long rEnd = System.currentTimeMillis();

    System.out.println("Java: " + (qEnd - qStart) + " ms");
    System.out.println("Radixsort: " + (rEnd - rStart) + " ms");
  }

}
