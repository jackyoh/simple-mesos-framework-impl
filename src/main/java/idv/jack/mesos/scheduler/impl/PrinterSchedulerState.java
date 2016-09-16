package idv.jack.mesos.scheduler.impl;

import java.util.LinkedList;
import java.util.List;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

public class PrinterSchedulerState implements Scheduler{

	private LinkedList<String> tasks;
	private int numTasks;
	private int startValue = 1;
	private int endValue = 1000;
	
	public PrinterSchedulerState(String[] args, int numTasks){
		this.numTasks = numTasks;
		tasks = new LinkedList<String>();
		//計算1到1000分為不同task
		int avgValue = endValue / numTasks;
		int count = 1;
		for(int i = startValue ; i < endValue ; i += avgValue){
			int stopValue = avgValue * count;
			tasks.add(i + "," + stopValue);
			count++;
		}
		
	}
	@Override
	public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
		System.out.println("Scheduler registered with id " + frameworkId.getValue());
		
	}

	@Override
	public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
		System.out.println("Scheduler re-registered");
		
	}

	@Override
	public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
		System.out.println("Scheduler received offers " + offers.size());
		for(Offer offer : offers){
			if(offers.size() > 0){
				
			}
			
		}

		
		
	}

	@Override
	public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
	
		
	}

	@Override
	public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
		System.out.println("Status update: task " + status.getTaskId().getValue() + " state is " + status.getState());
		
	}

	@Override
	public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) {
		
	}

	@Override
	public void disconnected(SchedulerDriver driver) {
		
		
	}

	@Override
	public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
		
		
	}

	@Override
	public void executorLost(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, int status) {
		
		
	}

	@Override
	public void error(SchedulerDriver driver, String message) {
		
		
	}

}
