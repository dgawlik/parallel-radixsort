package org.parsort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinTask;

public class Sort {

  public static void sort(int[] arr) {
    int L1 = 2 * 1024;
    int parts = (int) Math.ceil(arr.length / (double) L1);
    sortSegments(arr, L1, parts);
    mergeSegments(arr, L1, parts);
  }

  private static void sortSegments(int[] arr, int L1, int parts) {
    long start1 = System.currentTimeMillis();
    List<ForkJoinTask> tasks = new ArrayList<>();
    for (int i = 0; i < parts; i++) {
      int start = i * L1;
      int end = Math.min((i + 1) * L1, arr.length);
      tasks.add(
          ForkJoinTask.adapt(() -> sortSegment(arr, start, end)));
    }
    ForkJoinTask.invokeAll(tasks).forEach(ForkJoinTask::join);
    long end1 = System.currentTimeMillis();
    System.out.println("Sort: " + (end1 - start1) + " ms");
  }

  private static void sortSegment(int[] arr, int start, int end) {
    int low = start;
    int high = end-1;

    if (low < high) {

      int pivot = arr[high];
      int i = (low - 1);
      for (int j = low; j <= high - 1; j++) {
        if (arr[j] < pivot) {
          i++;
          int t = arr[i];
          arr[i] = arr[j];
          arr[j] = t;
        }
      }
      int t = arr[i + 1];
      arr[i + 1] = arr[high];
      arr[high] = t;

      int limit = i + 1;

      sortSegment(arr, low, limit - 1);
      sortSegment(arr, limit + 1, high);
    }
  }

  private static void mergeSegments(int[] arr, int L1, int parts) {
    long start2 = System.currentTimeMillis();

    int[] buffer = new int[arr.length];
    int range = 2;
    while (range / parts <= 1) {
      List<ForkJoinTask<?>> tasks = new ArrayList<>();

      for (int i = 0; i < parts; i += range) {
        if (i + (range / 2) >= parts) {
          break;
        }

        int left = i * L1;
        int middle = (i + range / 2) * L1;
        int right = Math.min((i + range) * L1, arr.length);

        tasks.add(
            ForkJoinTask.adapt(() -> merge(arr, buffer, left, middle, right)));
      }

      ForkJoinTask.invokeAll(tasks).forEach(ForkJoinTask::join);
      range <<= 1;
    }

    long end2 = System.currentTimeMillis();
    System.out.println("Merge: " + (end2 - start2) + " ms");
  }


  public static void merge(int[] arr, int[] buffer, int start, int middle,
      int end) {

    int left = start;
    int index = start;
    int right = middle;
    while (left < middle && right < end) {
      if (arr[left] <= arr[right]) {
        buffer[index++] = arr[left++];
      } else {
        buffer[index++] = arr[right++];
      }
    }

    while (left < middle) {
      buffer[index++] = arr[left++];
    }

    while (right < end) {
      buffer[index++] = arr[right++];
    }

    System.arraycopy(buffer, start, arr, start, end - start);
  }
}
