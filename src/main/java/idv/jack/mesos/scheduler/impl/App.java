package idv.jack.mesos.scheduler.impl;

import static org.apache.mesos.Protos.FrameworkInfo.Capability.Type.REVOCABLE_RESOURCES;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;

public class App {
	public static void main(String args[]){
		System.out.println("Starting the Test on Mesos with master " + args[1]);

		Protos.FrameworkInfo.Builder frameworkInfoBuilder = Protos.FrameworkInfo.newBuilder();
		frameworkInfoBuilder.addCapabilitiesBuilder().setType(REVOCABLE_RESOURCES);
		frameworkInfoBuilder.setName("Mesos Framework Test");
		frameworkInfoBuilder.setRole("role2");
		
		Protos.FrameworkInfo frameworkInfo = frameworkInfoBuilder.build();
		MesosSchedulerDriver schedulerDriver = new MesosSchedulerDriver(new PrinterSchedulerState(args, Integer.parseInt(args[0])), frameworkInfo, args[1]);
		schedulerDriver.run();
	}

}
