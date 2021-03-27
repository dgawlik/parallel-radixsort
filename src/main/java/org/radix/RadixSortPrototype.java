package org.radix;


import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

public class RadixSortPrototype {


  public static void parallelSort(long[] arr) {
    long[] output = new long[arr.length];

    int MAX_PART = 1_000_000;
    int numProc = Runtime.getRuntime().availableProcessors();
    int partL = Math
        .min((int) Math.ceil(arr.length / (double) numProc), MAX_PART);
    int parts = (int) Math.ceil(arr.length / (double) partL);

    Future[] threads = new Future[parts];
    ExecutorService worker = Executors.newFixedThreadPool(numProc);

    for (int i = 0; i < 8; i++) {
      int[][] counts = new int[parts][256];
      int radix = i;

      for (int j = 0; j < parts; j++) {
        int part = j;
        threads[j] = worker.submit(() -> {
          for (int k = part * partL; k < (part + 1) * partL && k < arr.length;
              k++) {
            int chunk = (int) ((arr[k] >> (radix * 8)) & 255);
            counts[part][chunk]++;
          }
        });
      }
      barrier(parts, threads);

      int base = 0;
      for (int k = 0; k <= 255; k++) {
        for (int j = 0; j < parts; j++) {
          int t = counts[j][k];
          counts[j][k] = base;
          base += t;
        }
      }

      for (int j = 0; j < parts; j++) {
        int part = j;
        threads[j] = worker.submit(() -> {
          for (int k = part * partL;
              k < (part + 1) * partL && k < arr.length;
              k++) {

            int chunk = (int) ((arr[k] >> (radix * 8)) & 255);
            output[counts[part][chunk]] = arr[k];
            counts[part][chunk]++;
          }
        });
      }
      barrier(parts, threads);

      for (int j = 0; j < parts; j++) {
        int part = j;
        threads[j] = worker.submit(() -> {
          for (int k = part * partL;
              k < (part + 1) * partL && k < arr.length;
              k++) {

            arr[k] = output[k];
          }
        });
      }
      barrier(parts, threads);
    }
    worker.shutdownNow();
  }

  private static void barrier(int parts, Future[] threads) {
    for (int j = 0; j < parts; j++) {
      try {
        threads[j].get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
  }


  public static void parallelSortWithMerge(long[] arr) {
    int MAX_PART = 1_000_000;
    int numProc = Runtime.getRuntime().availableProcessors();
    int partL = (int) Math.ceil(arr.length / (double) numProc);

    partL = Math.min(partL, MAX_PART);
    int parts = (int) Math.ceil(arr.length / (double) partL);

    long start1 = System.currentTimeMillis();
    long[] work = new long[arr.length];
    ExecutorService worker = Executors.newFixedThreadPool(parts);
    Future[] threads = new Future[parts];

    System.arraycopy(arr, 0, work, 0, arr.length);

    for (int i = 0; i < parts; i++) {
      int start = i * partL;
      int end = Math.min((i + 1) * partL, arr.length);
      threads[i] = worker.submit(() -> sortRange(work, start, end));
    }

    barrier(parts, threads);
    long end1 = System.currentTimeMillis();
    System.out.println("partition " + (end1 - start1) + "ms");

    long start2 = System.currentTimeMillis();
    int index = 0;
    int[] workIndices = new int[parts];
    int[] limits = new int[parts];
    long[] heap = new long[parts];
    int[] heapWhich = new int[parts];

    for (int i = 0; i < parts - 1; i++) {
      limits[i] = partL;
      workIndices[i] = 1;
    }
    limits[parts - 1] = arr.length % partL == 0 ? partL : arr.length % partL;
    workIndices[parts - 1] = 1;

    int heapTop = 0;
    for (int i = 0; i < parts; i++) {
      heap[heapTop] = work[i * partL];
      heapWhich[heapTop] = i;

      int parent = heapTop / 2;
      int j = heapTop;
      while (j > 0 && heap[j] < heap[parent]) {
        long temp = heap[j];
        int temp2 = heapWhich[j];
        heap[j] = heap[parent];
        heapWhich[j] = heapWhich[parent];
        heap[parent] = temp;
        heapWhich[parent] = temp2;

        j = parent;
        parent = parent / 2;
      }

      heapTop++;
    }

    while (index < arr.length) {
      arr[index++] = heap[0];
      int which = heapWhich[0];
      if (workIndices[which] < limits[which]) {
        heap[0] = work[which * partL + workIndices[which]];
        workIndices[which]++;
      } else {
        heap[0] = Long.MAX_VALUE;
        heapWhich[0] = -1;
      }

      int i = 0;
      int backI;
      int smallest;
      do {
        int l = 2 * i;
        int r = 2 * i + 1;

        smallest = i;

        if (l < heapTop && heap[i] > heap[l]) {
          smallest = l;
        }

        if (r < heapTop && heap[smallest] > heap[r]) {
          smallest = r;
        }

        if (smallest != i) {
          long temp = heap[i];
          int temp2 = heapWhich[i];

          heap[i] = heap[smallest];
          heapWhich[i] = heapWhich[smallest];

          heap[smallest] = temp;
          heapWhich[smallest] = temp2;
        }
        backI = i;
        i = smallest;
      } while (backI != smallest);
    }

    long end2 = System.currentTimeMillis();
    System.out.println("merge " + (end2 - start2));
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
