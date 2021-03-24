package org.radix;


import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

public class RadixSort {


  public static void parallelSort(long[] arr){
    int numProc = Runtime.getRuntime().availableProcessors();
    int partL = (int)Math.ceil(arr.length/(double)numProc);
    long[][] work = new long[numProc][];
    for(int i=0;i<numProc;i++){
      if(i<numProc-1){
        work[i] = new long[partL];
      }
      else {
        work[i] = new long[arr.length % partL == 0 ? partL : arr.length % partL ];
      }
    }
    ExecutorService worker = ForkJoinPool.commonPool();
    Future[] await = new Future[numProc];

    for(int i=0;i<numProc;i++){
      for(int j=i*partL;j<i*partL+work[i].length;j++){
        work[i][j-i*partL] = arr[j];
      }

      int partIndex = i;
      await[i] = worker.submit(() -> sort(work[partIndex]));
    }

    for(int i=0;i<numProc;i++){
      try {
        await[i].get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }

    worker.shutdownNow();

    int index = 0;
    int[] workIndices = new int[numProc];
    while(index < arr.length){
      long minValue = Long.MAX_VALUE;
      int chosen = -1;
      for(int i=0;i<work.length;i++){
        if(workIndices[i] < work[i].length && work[i][workIndices[i]] < minValue){
          minValue = work[i][workIndices[i]];
          chosen = i;
        }
      }
      arr[index++] = work[chosen][workIndices[chosen]++];
    }
  }

  public static void sort(long[] arr) {
    long[] output = new long[arr.length];

    for(int i=0;i<8;i++){
      int[] counts = new int[256];

      int shift = i*8;
      for(int j=0;j<arr.length;j++){
        int chunk = (int)((arr[j] >> shift) & 255);
        counts[chunk]++;
      }

      for(int j=1;j<= 255;j++){
        counts[j] += counts[j-1];
      }

      for(int j=arr.length-1;j>=0;j--){
        int chunk = (int)((arr[j] >> shift) & 255);
        output[counts[chunk]-1] = arr[j];
        counts[chunk]--;
      }

      for(int j=0;j<arr.length;j++){
        arr[j] = output[j];
      }

    }
  }

  public static void printArray(long[] arr) {
    for (int i = 0; i < arr.length; i++) {
      System.out.print(arr[i] + ", ");
    }
    System.out.println();
  }
}
