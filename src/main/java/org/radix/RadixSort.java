package org.radix;


import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RadixSort {


  public static void parallelSort(long[] arr) {
    int MAX_PART = 4_000_000;
    int numProc = Runtime.getRuntime().availableProcessors();
    int partL = (int) Math.ceil(arr.length / (double) numProc);

    partL = Math.min(partL, MAX_PART);
    int parts = (int) Math.ceil(arr.length / (double) partL);

    long[] work = new long[arr.length];
    ExecutorService worker = Executors.newFixedThreadPool(numProc);
    Future[] threads = new Future[parts];

    System.arraycopy(arr, 0, work, 0, arr.length);

    for (int i = 0; i < parts; i++) {
      int start = i * partL;
      int end = Math.min((i + 1) * partL, arr.length);
      threads[i] = worker.submit(() -> sortRange(work, start, end));
    }

    for (int i = 0; i < parts; i++) {
      try {
        threads[i].get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }

    int index = 0;
    int[] workIndices = new int[parts];
    int[] limits = new int[parts];

    for (int i = 0; i < parts - 1; i++) {
      limits[i] = partL;
    }
    limits[parts - 1] = arr.length % partL == 0 ? partL : arr.length % partL;

    while (index < arr.length) {
      long minValue = Long.MAX_VALUE;
      int chosen = -1;
      for (int i = 0; i < parts; i++) {
        if (workIndices[i] < limits[i]
            && work[i * partL + workIndices[i]] < minValue) {
          minValue = work[i * partL + workIndices[i]];
          chosen = i;
        }
      }
      arr[index++] = work[chosen * partL + workIndices[chosen]++];
    }
    worker.shutdownNow();
  }

  public static void sortRange(long[] arr, int start, int end) {
    long[] output = new long[end - start + 1];

    for (int i = 0; i < 8; i++) {
      int[] counts = new int[256];

      int shift = i * 8;
      for (int j = start; j < end; j++) {
        int chunk = (int) ((arr[j] >> shift) & 255);
        counts[chunk]++;
      }

      for (int j = 1; j <= 255; j++) {
        counts[j] += counts[j - 1];
      }

      for (int j = end - 1; j >= start; j--) {
        int chunk = (int) ((arr[j] >> shift) & 255);
        output[counts[chunk] - 1] = arr[j];
        counts[chunk]--;
      }

      int index = 0;
      for (int j = start; j < end; j++) {
        arr[j] = output[index++];
      }

    }
  }

  public static void sort(long[] arr) {
    sortRange(arr, 0, arr.length);
  }

  public static void printArray(long[] arr) {
    for (int i = 0; i < arr.length; i++) {
      System.out.print(arr[i] + ", ");
    }
    System.out.println();
  }
}
