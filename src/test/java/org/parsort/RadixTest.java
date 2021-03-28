package org.parsort;

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.radix.RadixSortPrototype;


class RadixTest {

  @Test
  void chaos_test(){
    Random rnd = new Random(1);
    int SIZE = 100;
    long[] ref = new long[SIZE];
    long[] sut = new long[SIZE];

    for(int i=0;i<SIZE;i++){
      long num = (rnd.nextLong() >>> 1) % 10000;
      ref[i] = num;
      sut[i] = num;
    }

    Arrays.sort(ref);
    RadixSortPrototype.parallelSort(sut);

    Assertions.assertArrayEquals(ref, sut);
  }

  @Test
  public void performance(){
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
    System.out.println("Java Parallel: " + (qEnd - qStart) + " ms");

    long rStart = System.currentTimeMillis();
    Radix.parallelSort(r);
    long rEnd = System.currentTimeMillis();
    System.out.println("Radixsort Parallel: " + (rEnd - rStart) + " ms");
  }
}