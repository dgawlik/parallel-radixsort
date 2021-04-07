package org.parsort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinTask;

public class Sort {

  private static class Stat implements Comparable<Stat> {

    int min;
    int max;
    int index;
    int start;
    int end;

    public Stat(int min, int max, int index, int start, int end) {
      this.min = min;
      this.max = max;
      this.index = index;
      this.start = start;
      this.end = end;
    }

    @Override
    public int compareTo(Stat o) {
      int c1 = Integer.compare(this.max, o.max);
      return c1 == 0
          ? Integer.compare(this.min, o.min) : c1;
    }

    public Stat copy() {
      return new Stat(this.min, this.max, this.index, this.start, this.end);
    }
  }

  public static void sort(int[] arr) {
    int L1 = 2*1024;
    int parts = (int) Math.ceil(arr.length / (double) L1);
    ArrayList<Stat> stats = sortSegments2(arr, L1, parts);
    Collections.sort(stats);
    regularize(arr, parts, stats);
    mergeSegments2(arr, L1, parts, stats);
  }

  private static void regularize(int[] arr, int parts, ArrayList<Stat> stats) {
    long start1 = System.currentTimeMillis();
    int[] buffer = new int[arr.length];
    int index = 0;
    for (int i = 0; i < parts; i++) {
      int len = stats.get(i).end - stats.get(i).start;
      System.arraycopy(arr, stats.get(i).start, buffer, index, len);
      stats.get(i).start = index;
      stats.get(i).end = index + len;
      index += len;
    }
    System.arraycopy(buffer, 0, arr, 0, buffer.length);
    long end1 = System.currentTimeMillis();
    System.out.println("Regularize: " + (end1 - start1) + " ms");
  }

  private static void mergeSegments2(int[] arr, int L1, int parts,
      ArrayList<Stat> stats) {
    long start2 = System.currentTimeMillis();

    int[] buffer = new int[arr.length];
    while (stats.size() > 1) {
      List<ForkJoinTask<Stat>> tasks = new ArrayList<>();
      Stat last = null;
      for (int i = 0; i < stats.size(); i += 2) {
        if (i + 1 == stats.size()) {
          last = stats.get(i);
          break;
        }

        Stat left = stats.get(i).copy();
        Stat right = stats.get(i + 1).copy();
        tasks.add(ForkJoinTask.adapt(() -> merge2(arr, buffer, left, right)));

      }
      ArrayList<Stat> newStat = new ArrayList<>();
      ForkJoinTask.invokeAll(tasks).forEach(t -> {
        Stat st = t.join();
        newStat.add(st);
      });
      if (last != null) {
        newStat.add(last);
      }
      stats = newStat;
    }
    long end2 = System.currentTimeMillis();
    System.out.println("Merge: " + (end2 - start2) + " ms");
  }

  private static ArrayList<Stat> sortSegments2(int[] arr, int L1, int parts) {
    long start1 = System.currentTimeMillis();
    List<ForkJoinTask> tasks = new ArrayList<>();
    for (int i = 0; i < parts; i++) {
      int start = i * L1;
      int end = Math.min((i + 1) * L1, arr.length);
      tasks.add(
          ForkJoinTask.adapt(() -> Arrays.sort(arr, start, end)));
    }
    ForkJoinTask.invokeAll(tasks).forEach(ForkJoinTask::join);
    ArrayList<Stat> stats = new ArrayList<>();
    for (int i = 0; i < parts; i++) {
      int start = i * L1;
      int end = Math.min((i + 1) * L1, arr.length);
      stats.add(new Stat(arr[start], arr[end - 1], i, start, end));
    }
    long end1 = System.currentTimeMillis();
    System.out.println("Sort: " + (end1 - start1) + " ms");
    return stats;
  }

  public static Stat merge2(int[] arr, int[] buffer, Stat statLeft,
      Stat statRight) {

    int start = statLeft.start;
    int middle = statLeft.end;
    int end = statRight.end;

    int step = (int) Math.sqrt(end - start);

    int leftCopy = start;
    while (leftCopy + step < middle
        && arr[leftCopy + step] < statRight.min) {
      leftCopy += step;
    }

    int rightCopy = end;
    while (rightCopy - step >= middle
        && arr[rightCopy - step] > statLeft.max) {
      rightCopy -= step;
    }

    System.arraycopy(arr, start, buffer, start, leftCopy - start);

    int left = leftCopy;
    int index = leftCopy;
    int right = middle;
    while (left < middle && right < rightCopy) {
      if (arr[left] <= arr[right]) {
        buffer[index++] = arr[left++];
      } else {
        buffer[index++] = arr[right++];
      }
    }

    while (left < middle) {
      buffer[index++] = arr[left++];
    }

    while (right < rightCopy) {
      buffer[index++] = arr[right++];
    }

    System.arraycopy(arr, rightCopy, buffer, index, end - rightCopy);
    System.arraycopy(buffer, start, arr, start, end - start);
    return new Stat(Math.min(statLeft.min, statRight.min),
        Math.max(statLeft.max, statRight.max),
        0, statLeft.start, statRight.end);
  }
}
