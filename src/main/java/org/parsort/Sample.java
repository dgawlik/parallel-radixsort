package org.parsort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class Sample {

  private static class PartMergeSort extends RecursiveAction {

    private final long[] arr;
    private final long[] buffer;
    private final int start;
    private final int end;

    public PartMergeSort(long[] arr, long[] buffer, int start, int end) {
      this.arr = arr;
      this.buffer = buffer;
      this.start = start;
      this.end = end;
    }

    @Override
    protected void compute() {
      mergesort(start, end);
    }

    private void mergesort(int start, int end) {
      if (end - start < 32) {
        if(start < end) {
          Arrays.sort(arr, start, end);
        }
        return;
      }

      int m = (start + end) / 2;

      mergesort(start, m);
      mergesort(m + 1, end);

      int left = start;
      int right = m + 1;
      int index = 0;
      while (left <= m && right <= end) {
        if (arr[left] <= arr[right]) {
          buffer[index++] = arr[left++];
        } else {
          buffer[index++] = arr[right++];
        }
      }

      System.arraycopy(arr, left, buffer, index, m - left + 1);
      System.arraycopy(arr, right, buffer, index, end - right + 1);
      System.arraycopy(buffer, 0, arr, start, end - start + 1);
    }
  }

  private static final Random rng = new Random(1);

  public static void parallelSort(long[] arr) {
    long[] buffer = new long[arr.length];

    int numProcessors = Runtime.getRuntime().availableProcessors();
//    int MAX_PART = 500_000;
//    int partSize = Math.min(arr.length / numProcessors, MAX_PART);
//    int parts = (int) Math.ceil(arr.length / (double) partSize);
    int parts = numProcessors;

    long[] splitters = sampleSplitters(arr, parts);

    Arrays.parallelSort(splitters);

    int[] counts = calculateBucketCounts(arr, parts, splitters);

    int[] offsets = new int[parts];
    int offset = 0;
    for (int i = 0; i < parts; i++) {
      offsets[i] = offset;
      offset += counts[i];
    }

    partitionToBuckets(arr, buffer, parts, splitters, offsets);

    ForkJoinPool pool = new ForkJoinPool(numProcessors);
    List<PartMergeSort> tasks = new ArrayList<>();
    offset = 0;
    for (int i = 0; i < parts; i++) {
      int start = offset;
      int end = Math.min(start + counts[i] - 1, arr.length - 1);
      offset += counts[i];
      var task = new PartMergeSort(buffer, arr, start, end);
      tasks.add(task);
      pool.submit(task);
    }

    tasks.forEach(RecursiveAction::join);
    System.arraycopy(buffer, 0, arr, 0, buffer.length);
    pool.shutdownNow();
  }

  private static void partitionToBuckets(long[] arr, long[] buffer, int parts,
      long[] splitters, int[] offsets) {
    for (int i = 0; i < arr.length; i++) {
      int k = findBucket(splitters, arr[i], 0, parts - 1);
      buffer[offsets[k]++] = arr[i];
    }
  }

  private static int[] calculateBucketCounts(long[] arr, int parts, long[] splitters) {
    int[] counts = new int[parts];
    for (int i = 0; i < arr.length; i++) {
      counts[findBucket(splitters, arr[i], 0, parts - 1)]++;
    }
    return counts;
  }

  private static long[] sampleSplitters(long[] arr, int parts) {
    long[] splitters = new long[parts];
    splitters[0] = Long.MIN_VALUE;
    for (int i = 1; i < parts; i++) {
      splitters[i] = arr[rng.nextInt(arr.length)];
    }
    return splitters;
  }

  private static int findBucket(long[] splitters, long val, int start,
      int end) {
    if (start == end) {
      return start;
    }
    if (end - start == 1) {
      return val < end ? start : end;
    }

    int m = (start + end) / 2;
    if (val <= splitters[m]) {
      return findBucket(splitters, val, start, m);
    } else {
      return findBucket(splitters, val, m + 1, end);
    }
  }

}
