package org.proto.perf;

import java.util.Arrays;
import java.util.Random;
import org.parsort.Radix;
import org.parsort.Sample;
import org.proto.RadixSortPrototype;

public class SampleSortPerformanceTest {


  public static void main(String[] args) {
    testParallel();
  }

  public static void testJavaParallel() {
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
  }

  public static void testSerial() {
    int SIZE = 20_000_000;
    Random rnd = new Random(1);

    long[] q = new long[SIZE];
    long[] r = new long[SIZE];

    for (int i = 0; i < SIZE; i++) {
      long val = rnd.nextLong() >>> 1;
      q[i] = val;
      r[i] = val;
    }

    long rStart = System.currentTimeMillis();
    RadixSortPrototype.sort(r);
    long rEnd = System.currentTimeMillis();
    System.out.println("Radixsort Serial: " + (rEnd - rStart) + " ms");
  }

  public static void testParallel() {
    int SIZE = 20_000_000;
    Random rnd = new Random(1);

    long[] q = new long[SIZE];
    long[] r = new long[SIZE];

    for (int i = 0; i < SIZE; i++) {
      long val = rnd.nextLong() >>> 1;
      q[i] = val;
      r[i] = val;
    }
    long rStart = System.currentTimeMillis();
    Sample.parallelSort(r);
    long rEnd = System.currentTimeMillis();
    System.out.println("SampleSort Parallel: " + (rEnd - rStart) + " ms");
  }

}
