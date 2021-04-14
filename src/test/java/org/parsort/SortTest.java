package org.parsort;

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SortTest {

  @Test
  void chaos_test(){
    Random rnd = new Random(1);
    int SIZE = 20_000_000;
    int[] ref = new int[SIZE];
    int[] sut = new int[SIZE];

    for(int i=0;i<SIZE;i++){
      int num = rnd.nextInt();
      ref[i] = num;
      sut[i] = num;
    }


    Arrays.sort(ref);
    Sort.sort(sut);

    Assertions.assertArrayEquals(ref, sut);
  }

  @Test
  public void performance(){
    int SIZE = 20_000_000;
    Random rnd = new Random(1);

    int[] q = new int[SIZE];
    int[] r = new int[SIZE];

    for (int i = 0; i < SIZE; i++) {
      int val = rnd.nextInt();
      q[i] = val;
      r[i] = val;
    }

    long rStart = System.currentTimeMillis();
    Sort.sort(r);
    long rEnd = System.currentTimeMillis();
    System.out.println("Custom sort: " + (rEnd - rStart) + " ms");

    long qStart = System.currentTimeMillis();
    Arrays.parallelSort(q);
    long qEnd = System.currentTimeMillis();
    System.out.println("Java Parallel: " + (qEnd - qStart) + " ms");

  }



}