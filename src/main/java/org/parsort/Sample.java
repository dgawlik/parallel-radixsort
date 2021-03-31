package org.parsort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

public class Sample {

  private static class PartMergeSort extends RecursiveAction {

    private final long[] arr;
    private final long[] buffer;
    private final int start;
    private final int end;
    private final int parts;
    private final Random rng = new Random(1);

    public PartMergeSort(long[] arr, long[] buffer, int start, int end,
        int parts) {
      this.arr = arr;
      this.start = start;
      this.end = end;
      this.buffer = buffer;
      this.parts = parts;
    }

    @Override
    protected void compute() {
      if (end - start < 10_000) {
        if (start < end) {
          Arrays.sort(arr, start, end + 1);
        }
      } else {
        long[] splitters = sampleSplitters(arr, parts);

        Arrays.sort(splitters);

        int[] counts = calculateBucketCounts(arr, parts, splitters, start, end);

        int[] offsets = new int[parts];
        int offset = 0;
        for (int i = 0; i < parts; i++) {
          offsets[i] = offset;
          offset += counts[i];
        }

        partitionToBuckets(arr, buffer, parts, splitters, offsets, start, end);

        List<PartMergeSort> tasks = new ArrayList<>();
        offset = 0;
        for (int i = 0; i < parts; i++) {
          int begin = this.start + offset;
          int limit = Math.min(begin + counts[i] - 1, arr.length - 1);
          offset += counts[i];
          var task = new PartMergeSort(buffer,arr, begin, limit, parts);
          tasks.add(task);
        }

        ForkJoinTask.invokeAll(tasks).forEach(ForkJoinTask::join);
        System.arraycopy(buffer, 0, arr, start, end - start + 1);
      }
    }

    private void partitionToBuckets(long[] arr, long[] buffer, int parts,
        long[] splitters, int[] offsets, int start, int end) {
      for (int i = start; i <= end; i++) {
        int k = findBucket(splitters, arr[i]);
        buffer[this.start+offsets[k]++] = arr[i];
      }
    }

    private int[] calculateBucketCounts(long[] arr, int parts,
        long[] splitters, int start, int end) {
      int[] counts = new int[parts];
      for (int i = start; i <= end; i++) {
        counts[findBucket(splitters, arr[i])]++;
      }
      return counts;
    }

    private long[] sampleSplitters(long[] arr, int parts) {
      long[] splitters = new long[parts];
      splitters[0] = Long.MIN_VALUE;
      for (int i = 1; i < parts; i++) {
        splitters[i] = arr[start + rng.nextInt(end - start + 1)];
      }
      return splitters;
    }

    private int findBucket(long[] splitters, long val) {
      int index = 0;
      while (index < splitters.length && val >= splitters[index]) {
        index++;
      }
      return index - 1;
    }
  }

    public static void parallelSort(long[] arr) {
      int numProcessors = Runtime.getRuntime().availableProcessors();
      long buffer[] = new long[arr.length];

      PartMergeSort main = new PartMergeSort(arr, buffer,0, arr.length - 1,
          numProcessors);
      main.fork().join();
    }


  }
