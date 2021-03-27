package org.radix;

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RadixSortPrototypeTest {


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

    RadixSortPrototype.printArray(sut);

    Assertions.assertArrayEquals(ref, sut);
  }

}