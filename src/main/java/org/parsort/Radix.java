package org.parsort;


import java.util.ArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

public class Radix {

  private abstract static class BaseAction extends RecursiveAction {

    protected final int[][] counts;
    protected final long[] arr;
    protected final int start;
    protected final int end;
    protected final int radix;
    protected final int partNo;
    protected final long[] buffer;

    public BaseAction(int[][] counts, long[] arr, long[] buffer, int start,
        int end,
        int radix, int partNo) {
      this.counts = counts;
      this.arr = arr;
      this.start = start;
      this.end = end;
      this.radix = radix;
      this.partNo = partNo;
      this.buffer = buffer;
    }
  }

  private static class CountBuckets extends BaseAction {

    public CountBuckets(int[][] counts, long[] arr, long[] buffer, int start,
        int end, int radix, int partNo) {
      super(counts, arr, buffer, start, end, radix, partNo);
    }

    @Override
    protected void compute() {
      for (int i = start; i < end; i++) {
        int chunk = calculateBucket(arr[i], radix);
        counts[partNo][chunk]++;
      }
    }
  }

  private static class AssignBuffer extends BaseAction {

    public AssignBuffer(int[][] counts, long[] arr, long[] buffer, int start,
        int end, int radix, int partNo) {
      super(counts, arr, buffer, start, end, radix, partNo);
    }

    @Override
    protected void compute() {
      for (int i = start; i < end; i++) {
        int chunk = calculateBucket(arr[i], radix);
        buffer[counts[partNo][chunk]] = arr[i];
        counts[partNo][chunk]++;
      }
    }
  }

  private static class CopyBackToArray extends BaseAction {

    public CopyBackToArray(int[][] counts, long[] arr, long[] buffer, int start,
        int end, int radix, int partNo) {
      super(counts, arr, buffer, start, end, radix, partNo);
    }

    @Override
    protected void compute() {
      if (end - start >= 0) {
        System.arraycopy(buffer, start, arr, start, end - start);
      }
    }
  }

  public static void parallelSort(long[] arr) {
    long[] output = new long[arr.length];

    int MAX_PART = 500_000;
    int numProc = Runtime.getRuntime().availableProcessors();
    int partL = Math
        .min((int) Math.ceil(arr.length / (double) numProc), MAX_PART);
    int parts = (int) Math.ceil(arr.length / (double) partL);

    ForkJoinPool pool = new ForkJoinPool(4);

    for (int radix = 0; radix < 8; radix++) {
      int[][] counts = new int[parts][512];

      ArrayList<ForkJoinTask<?>> tasks = new ArrayList<>();
      for (int partNo = 0; partNo < parts; partNo++) {

        int start = partNo * partL;
        int end = Math.min((partNo + 1) * partL, arr.length);

        CountBuckets task = new CountBuckets(counts, arr, output, start, end,
            radix, partNo);
        tasks.add(task);
        pool.execute(task);
      }
      tasks.forEach(ForkJoinTask::join);

      int base = 0;
      for (int k = 0; k < 512; k++) {
        for (int j = 0; j < parts; j++) {
          int t = counts[j][k];
          counts[j][k] = base;
          base += t;
        }
      }

      tasks.clear();
      for (int partNo = 0; partNo < parts; partNo++) {

        int start = partNo * partL;
        int end = Math.min((partNo + 1) * partL, arr.length);

        AssignBuffer task = new AssignBuffer(counts, arr, output, start, end,
            radix, partNo);
        tasks.add(task);
        pool.execute(task);
      }
      tasks.forEach(ForkJoinTask::join);

      tasks.clear();
      for (int partNo = 0; partNo < parts; partNo++) {

        int start = partNo * partL;
        int end = Math.min((partNo + 1) * partL, arr.length);

        CopyBackToArray task = new CopyBackToArray(counts, arr, output, start,
            end, radix, partNo);
        tasks.add(task);
        pool.execute(task);
      }
      tasks.forEach(ForkJoinTask::join);
    }
    pool.shutdownNow();
  }

  private static int calculateBucket(long val, int radix) {
    if (val < 0L) {
      return (int) (256 - ((((~val >> (radix * 8)) + 1) & 255)));
    } else {
      return (int) (((val >> (radix * 8)) & 255) + 256);
    }
  }
}
