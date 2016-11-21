package idv.jack.mesos.scheduler.impl;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

public class UselessRemoteBASH implements Scheduler{
	private boolean submitted = false;
	private String shellScriptPath;
	
	public UselessRemoteBASH(String shellScriptPath){
		this.shellScriptPath = shellScriptPath;
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
		synchronized (this) {
			if (submitted) {
				for(Offer o : offers) {
					driver.declineOffer(o.getId());
				}
				return;
			}
			submitted = true;
			Offer offer = offers.get(0);
			TaskInfo ti = makeTask(offer.getSlaveId());
			driver.launchTasks(
					Collections.singletonList(offer.getId()), 
					Collections.singletonList(ti)
			);
			System.out.println("Launched offer: " + ti);
		}
		
	}

	@Override
	public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
		
		
	}

	@Override
	public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
	
		
	}

	@Override
	public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId,
			SlaveID slaveId, byte[] data) {
		
		
	}

	@Override
	public void disconnected(SchedulerDriver driver) {
		
		
	}

	@Override
	public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
		
		
	}

	@Override
	public void executorLost(SchedulerDriver driver, ExecutorID executorId,
			SlaveID slaveId, int status) {
		
		
	}

	@Override
	public void error(SchedulerDriver driver, String message) {
		
		
	}
	public TaskInfo makeTask(SlaveID targetSlave){
		double cpus = 20;
		double mem = 1000;
		//String command = "echo hello world";
		String command = "bash " + this.shellScriptPath;
		
		UUID uuid = UUID.randomUUID();
		TaskID id = TaskID.newBuilder().setValue(uuid.toString()).build();
		
		return TaskInfo.newBuilder()
				.setName("useless_remote_bash.task " + id.getValue())
				.setTaskId(id)
				.addResources(Resource.newBuilder()
											 .setName("cpus")
											 .setType(Value.Type.SCALAR)
											 .setScalar(Value.Scalar.newBuilder().setValue(cpus))		)
				.addResources(Resource.newBuilder()
											 .setName("mem")
											 .setType(Value.Type.SCALAR)
											 .setScalar(Value.Scalar.newBuilder().setValue(mem)))
				.setCommand(CommandInfo.newBuilder().setValue(command))
				.setSlaveId(targetSlave)				   
				.build();
	}
}
