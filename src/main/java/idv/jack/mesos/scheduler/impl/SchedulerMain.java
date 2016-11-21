package idv.jack.mesos.scheduler.impl;

import static org.apache.mesos.Protos.FrameworkInfo.Capability.Type.REVOCABLE_RESOURCES;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

public class SchedulerMain {

	public static void main(String args[]){
		Protos.FrameworkInfo.Builder frameworkInfoBuilder = Protos.FrameworkInfo.newBuilder();
		frameworkInfoBuilder.addCapabilitiesBuilder().setType(REVOCABLE_RESOURCES);
		frameworkInfoBuilder.setName("Mesos Framework Test");
		frameworkInfoBuilder.setRole("role2");
		frameworkInfoBuilder.setUser("root");
		Protos.FrameworkInfo frameworkInfo = frameworkInfoBuilder.build();
		
		
		Scheduler mySched = new UselessRemoteBASH(args[1]); //args[1] is running linux shell script for mesos
		
		SchedulerDriver driver = new MesosSchedulerDriver(mySched, frameworkInfo, args[0]);
		driver.run();
	}
	
}
