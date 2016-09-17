package idv.jack.mesos.scheduler.impl;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

public class PrinterSchedulerState implements Scheduler{

	private LinkedList<String> tasks;
	private int numTasks;
	private int tasksSubmitted;
	private int tasksCompleted;
	private int startValue = 1;
	private int endValue = 1000;
	private int totalResult = 0;
	public PrinterSchedulerState(String[] args, int numTasks){
		this.numTasks = numTasks;
		tasks = new LinkedList<String>();
		//計算1到1000分為不同task
		int avgValue = endValue / numTasks;
		int count = 1;
		for(int i = startValue ; i < endValue ; i += avgValue){
			int stopValue = avgValue * count;
			tasks.add(i + " " + stopValue);
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
			if(tasks.size() > 0){
				tasksSubmitted++;
				String task = tasks.remove();
				Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue(String.valueOf(tasksSubmitted)).build();
				System.out.println("LAUNCHING TASK " + taskID.getValue() + " on slave " + offer.getSlaveId().getValue() + " with " + task);
				Protos.ExecutorInfo executor = Protos.ExecutorInfo.newBuilder()
											.setExecutorId(Protos.ExecutorID.newBuilder().setValue(String.valueOf(tasksSubmitted)))
											.setCommand(createCommand(task))
											.setName("Number total").build();

				
				Protos.TaskInfo taskInfo = Protos.TaskInfo.newBuilder()
						.setName("Add Number Task-" + taskID.getValue())
						.setTaskId(taskID)
						.setExecutor(Protos.ExecutorInfo.newBuilder(executor))
						.addResources(Protos.Resource.newBuilder().setName("cpus").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(1)))
						.addResources(Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(128)))
						.setSlaveId(offer.getSlaveId())
						.build();

				driver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(taskInfo));
			}
			
		}

		
		
	}

	@Override
	public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
	
		
	}

	@Override
	public void statusUpdate(SchedulerDriver driver, TaskStatus taskStatus) {
		System.out.println("Status update: task " + taskStatus.getTaskId().getValue() + " state is " + taskStatus.getState());
		if(taskStatus.getState().equals(Protos.TaskState.TASK_FINISHED)){
			tasksCompleted++;
			//int taskResult = Integer.parseInt(taskStatus.getData().toStringUtf8());
			///this.totalResult += taskResult;
			System.out.println("Task " + taskStatus.getTaskId().getValue() + " finished with area " + taskStatus.getMessage());
		}else{
			System.out.println("Task " + taskStatus.getTaskId().getValue() + " has message " + taskStatus.getMessage());
		}
		
		
		if(this.tasksCompleted == this.numTasks){
			System.out.println("Result is " + this.totalResult);
			driver.stop();
		}
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
	
	private Protos.CommandInfo.Builder createCommand(String args) {
		//
		return Protos.CommandInfo.newBuilder().setValue("/jdk1.7.0_80/bin/java -cp /mesos-0.28.2-shaded-protobuf.jar:/simple-mesos-framework-impl.jar:/protobuf-java-2.5.0.jar idv.jack.mesos.executor.impl.NumberAdderExecutor " + args);//TODO
	}

}
