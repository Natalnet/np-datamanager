package com.np.datamanager.stats;
import java.util.*;
  
public class SimpleMovingAverage {
      
    // queue used to store list so that we get the average
    private final Queue<Double> series = new LinkedList<Double>();
    private final int period;
    private double sum;
  
    // constructor to initialize period
    public SimpleMovingAverage(int period)
    {
        this.period = period;
    }
  
    // function to add new data in the
    // list and update the sum so that
    // we get the new mean
    public void addData(double num)
    {
        sum += num;
        series.add(num);
  
        // Updating size so that length
        // of data set should be equal
        // to period as a normal mean has
        if (series.size() > period)
        {
            sum -= series.remove();
        }
    }
  
    // function to calculate mean
    public double getMean()
    {
        return sum / period;
    }
  
    public static void main(String[] args)
    {
        double[] input_data = { 1, 3, 5, 6, 8, 12, 18, 21, 22, 25 };
        int per = 3;
        SimpleMovingAverage obj = new SimpleMovingAverage(per);
        for (double x : input_data) 
        {
            obj.addData(x);
            System.out.println("New number added is " +
                                x + ", SMA = " + obj.getMean());
        }
    }
}