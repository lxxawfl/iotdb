package org.apache.iotdb.session;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Random;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.junit.Assert;

public class MyBasicOperationTest3 {

  public static void main(String[] args) throws IOException {
    // op1: long v = BytesUtils.bytesToLong(deltaBuf, packWidth * i, packWidth);
    // op2: put bytes as a whole into long, i.e., BytesUtils.bytesToLong2(deltaBuf, packWidth * i, packWidth);

    int repeat = 1000000;
    int packNum = 128;
    int packWidth = 9;

    int[] fallWithinMasks;
    if (packWidth < 8) {
      fallWithinMasks = TsFileConstant.generateFallWithinMasks(packWidth);
    } else {
      fallWithinMasks = null;
    }

    DescriptiveStatistics op1 = new DescriptiveStatistics();
    DescriptiveStatistics op2 = new DescriptiveStatistics();
    for (int k = 0; k < repeat; k++) {
      // prepare test data
      Random r = new Random();
      int low = 0; // inclusive
      int high = 256; // exclusive
      byte[] buf = new byte[packNum * 8];
      for (int i = 0; i < packNum; i++) {
        int v = r.nextInt(high - low) + low;
//        int v = 1;
        BytesUtils.longToBytes(v, buf, i * packWidth, packWidth);
      }

      // test op1
      long[] value1 = new long[packNum];
      long start = System.nanoTime();
      for (int i = 0; i < packNum; i++) {
        value1[i] = BytesUtils.bytesToLong(buf, packWidth * i, packWidth);
//        System.out.println(BytesUtils.bytesToLong(buf, packWidth * i, packWidth));
      }
      long elapsedTime = System.nanoTime() - start;
//      System.out.println(elapsedTime / 1000.0 + "us");
//      System.out.println(sum);
      op1.addValue(elapsedTime / 1000.0);

      // test op2
      long[] value2 = new long[packNum];
      start = System.nanoTime();
      for (int i = 0; i < packNum; i++) {
        value2[i] = BytesUtils.bytesToLong2(buf, packWidth * i, packWidth, fallWithinMasks);
//        System.out.println(BytesUtils.bytesToLong2(buf, packWidth * i, packWidth));
//        Assert.assertEquals(value1[i], value2[i]);
      }
      elapsedTime = System.nanoTime() - start;
//      System.out.println(elapsedTime / 1000.0 + "us");
//      System.out.println(sum2);
      op2.addValue(elapsedTime / 1000.0);

      System.out.println("---------------");
      for (int i = 0; i < packNum; i++) {
        Assert.assertEquals(value1[i], value2[i]);
      }
    }
    printStat(op1, "op1-bytesToLong");
    printStat(op2, "op2-bytesToLong2");
    System.out.println("op1/op2=" + op1.getMean() / op2.getMean());
    System.out.println("op2/op1=" + op2.getMean() / op1.getMean());
    System.out.println("repeat=" + repeat);
    System.out.println("packNum=" + packNum);
    System.out.println("packWidth=" + packWidth);
    TsFileConstant.printByteToLongStatistics();
  }

  private static String printStat(DescriptiveStatistics statistics, String name) {
    DecimalFormat df = new DecimalFormat("#,###.00");
    double max = statistics.getMax();
    double min = statistics.getMin();
    double mean = statistics.getMean();
    double std = statistics.getStandardDeviation();
    double p25 = statistics.getPercentile(25);
    double p50 = statistics.getPercentile(50);
    double p75 = statistics.getPercentile(75);
    double p90 = statistics.getPercentile(90);
    double p95 = statistics.getPercentile(95);
    String res =
        name
            + "_stats"
            + ": "
            + "num="
            + statistics.getN()
            + ", "
            + "sum="
            + df.format(statistics.getSum())
            + "us,"
            + "mean="
            + df.format(mean)
            + ", "
            + "min="
            + df.format(min)
            + ", "
            + "max="
            + df.format(max)
            + ", "
            + "std="
            + df.format(std)
            + ", "
            + "p25="
            + df.format(p25)
            + ", "
            + "p50="
            + df.format(p50)
            + ", "
            + "p75="
            + df.format(p75)
            + ", "
            + "p90="
            + df.format(p90)
            + ", "
            + "p95="
            + df.format(p95);
    System.out.println(res);
    return res;
  }
}
