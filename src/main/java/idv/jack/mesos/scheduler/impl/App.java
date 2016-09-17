package idv.jack.mesos.scheduler.impl;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;

public class App {
	public static void main(String args[]){
		System.out.println("Starting the Test on Mesos with master " + args[1]);
		Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder().setName("Mesos Framework Test").setUser("root").build();
		
		MesosSchedulerDriver schedulerDriver = new MesosSchedulerDriver(new PrinterSchedulerState(args, Integer.parseInt(args[0])), frameworkInfo, args[1]);
		schedulerDriver.run();
	}

}
