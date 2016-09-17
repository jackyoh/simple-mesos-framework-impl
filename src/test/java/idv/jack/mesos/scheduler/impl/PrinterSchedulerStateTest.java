package idv.jack.mesos.scheduler.impl;

import org.junit.Test;

public class PrinterSchedulerStateTest {
	@Test
	public void test1(){
		int startValue = 1;
		int endValue = 1000;
		int numTasks = 10;
		
		int avgValue = endValue / numTasks;
		int count = 1;
		for(int i = startValue ; i < endValue ; i += avgValue){
			int stopValue = avgValue * count;
			System.out.println(i + "," + stopValue);
			count++;
		}
	}
	@Test
	public void test2(){
		int total = 0;
		for(int i = 1 ; i <= 1000 ; i++){
			total += i;
		}
		System.out.println(total);
	}

}
