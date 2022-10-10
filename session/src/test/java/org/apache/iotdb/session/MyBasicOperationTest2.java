package org.apache.iotdb.session;

import java.text.DecimalFormat;
import java.util.Random;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.iotdb.tsfile.utils.BytesUtils;

public class MyBasicOperationTest2 {

  public static void main(String[] args) {
    // op1: long v = BytesUtils.bytesToLong(deltaBuf, packWidth * i, packWidth);
    // op2: put bytes as a whole into long

    int repeat = 1000000;
    DescriptiveStatistics op1 = new DescriptiveStatistics();
    DescriptiveStatistics op2 = new DescriptiveStatistics();
    for (int k = 0; k < repeat; k++) {
      // prepare test data
      Random r = new Random();
      int low = 0; // inclusive
      int high = 256; // exclusive
      int packNum = 128;
      int packWidth = 8; // equal to one byte length
      byte[] buf = new byte[packNum];
      for (int i = 0; i < packNum; i++) {
        int v = r.nextInt(high - low) + low;
        BytesUtils.longToBytes(v, buf, i * packWidth, packWidth);
      }

      // test op1
      long sum = 0;
      long start = System.nanoTime();
      for (int i = 0; i < packNum; i++) {
        sum += BytesUtils.bytesToLong(buf, packWidth * i, packWidth);
//        System.out.println(BytesUtils.bytesToLong(buf, packWidth * i, packWidth));
      }
      long elapsedTime = System.nanoTime() - start;
      System.out.println(elapsedTime / 1000.0 + "us");
      System.out.println(sum);
      op1.addValue(elapsedTime / 1000.0);

      // test op2
      sum = 0;
      start = System.nanoTime();
      for (int i = 0; i < packNum; i++) {
        // op2_a: 把一个byte装到一个long里从低位到高位偏移offset，又即byte的最低位在long中的pos（从低到高从0开始数）
        sum += (buf[i] & 0xff) << 8; // &0xff is to convert bytes to unsigned bytes
        // op2_b: 把一个byte的高位x个比特装到一个long的低位x个比特
        // TODO 如何把一个byte一次分成高位x个比特和低位y个比特
//        System.out.println("---");
//        System.out.println(buf[i]);
//        System.out.println((buf[i] & 0xff & 0b11100000) >> 5);
        sum += (buf[i] & 0xff & 0b11100000) >> 5;

        // op2_c: 把一个byte的低位y个比特装到一个long的从低位到高位pos=packWidth-1的开始
//        System.out.println((buf[i] & 0xff & ~0b11100000) << 3);
        sum += (buf[i] & 0xff & ~0b11100000) << 3;
      }
      elapsedTime = System.nanoTime() - start;
      System.out.println(elapsedTime / 1000.0 + "us");
      System.out.println(sum);
      op2.addValue(elapsedTime / 1000.0);
    }
    printStat(op1, "op1-convertBitToLong");
    printStat(op2, "op2-compareByte");
    System.out.println("op1/op2=" + op1.getMean() / op2.getMean());
    System.out.println("op2/op1=" + op2.getMean() / op1.getMean());
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
