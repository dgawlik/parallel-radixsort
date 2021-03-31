package org.parsort;

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SampleTest {

  @Test
  void chaos_test(){
    Random rnd = new Random(1);
    int SIZE = 100;
    long[] ref = new long[SIZE];
    long[] sut = new long[SIZE];

    for(int i=0;i<SIZE;i++){
      long num = rnd.nextLong() % 10000;
      ref[i] = num;
      sut[i] = num;
    }

    Arrays.sort(ref);
    Sample.parallelSort(sut);

    for(int i=0;i<sut.length;i++){
      System.out.print(sut[i] + ", ");
    }

    Assertions.assertArrayEquals(ref, sut);
  }

  @Test
  public void performance(){
    int SIZE = 20_000_000;
    Random rnd = new Random(1);

    long[] q = new long[SIZE];
    long[] r = new long[SIZE];

    for (int i = 0; i < SIZE; i++) {
      long val = rnd.nextLong();
      q[i] = val;
      r[i] = val;
    }

    long qStart = System.currentTimeMillis();
    Arrays.sort(q);
    long qEnd = System.currentTimeMillis();
    System.out.println("Java Serial: " + (qEnd - qStart) + " ms");

    long rStart = System.currentTimeMillis();
    Sample.parallelSort(r);
    long rEnd = System.currentTimeMillis();
    System.out.println("Samplesort Parallel: " + (rEnd - rStart) + " ms");
  }
}