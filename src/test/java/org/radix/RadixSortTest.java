package org.radix;

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RadixSortTest {


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
    RadixSort.parallelSort(sut);

    RadixSort.printArray(sut);

    Assertions.assertArrayEquals(ref, sut);
  }

}