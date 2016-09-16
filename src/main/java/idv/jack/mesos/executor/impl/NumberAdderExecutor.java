package idv.jack.mesos.executor.impl;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

import com.google.protobuf.ByteString;

public class NumberAdderExecutor implements Executor {
	private int startValue;
	private int stopValue;
	
	public NumberAdderExecutor(int startValue, int stopValue){
		this.startValue = startValue;
		this.stopValue = stopValue;
	}
	@Override
	public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo,
			SlaveInfo slaveInfo) {

		System.out.println("Registered an executor on slave " + slaveInfo.getHostname());
	}

	@Override
	public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {

		System.out.println("Re-Registered an executor on slave " + slaveInfo.getHostname());
	}

	@Override
	public void disconnected(ExecutorDriver driver) {
		System.out.println("Re-Disconnected the executor on slave");
		
	}

	@Override
	public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
		System.out.println("Launching task " + task.getTaskId().getValue());
		System.out.println("StartValue is :" + this.startValue + " StopValue is:" + this.stopValue);
		
		Thread thread = new Thread(){
			
			@Override
			public void run(){
				Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
														   .setTaskId(task.getTaskId())
														   .setState(Protos.TaskState.TASK_RUNNING).build();
				driver.sendStatusUpdate(status);
				
				System.out.println("Running task " + task.getTaskId().getValue());
				
				int total = 0;
				for(int i = startValue ; i <= stopValue ; i++){
					total += i;
				}

				status = Protos.TaskStatus.newBuilder()
									.setTaskId(task.getTaskId())
									.setState(Protos.TaskState.TASK_FINISHED)
									.setData(ByteString.copyFrom(Integer.toString(total).getBytes()))
									.build();
				driver.sendStatusUpdate(status);
				System.out.println("Finished task " + task.getTaskId().getValue() + " " + " Result:" + total);
			}
			
		};
		thread.start();
	}

	@Override
	public void killTask(ExecutorDriver driver, TaskID taskId) {
		System.out.println("Killing task " + taskId);
		
	}

	@Override
	public void frameworkMessage(ExecutorDriver driver, byte[] data) {

		
	}

	@Override
	public void shutdown(ExecutorDriver driver) {
		System.out.println("Shutting down the executor");
		
	}

	@Override
	public void error(ExecutorDriver driver, String message) {
		
		
	}
	
	public static void main(String args[]){
		
		MesosExecutorDriver driver = new MesosExecutorDriver(new NumberAdderExecutor(Integer.parseInt(args[0]), Integer.parseInt(args[1])));
		Protos.Status status = driver.run();
		System.out.println("Driver exited with status " + status);
	}
}
