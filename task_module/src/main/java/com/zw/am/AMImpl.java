package com.zw.am;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class AMImpl {
    public static void main(String[] args) throws InterruptedException, IOException, YarnException {
//        File file = new File(System.getProperty("user.dir"));

        int n = Integer.parseInt(args[0]);

        Configuration conf = new YarnConfiguration();

        AMRMClient rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        System.out.println("register application master");
        rmClient.registerApplicationMaster("", 0, "");
        System.out.println("register application master end");

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        Resource resource = Records.newRecord(Resource.class);
        resource.setMemorySize(128);
        resource.setVirtualCores(1);

        for (int index = 0; index < n; index++) {
            AMRMClient.ContainerRequest containerRequest = new AMRMClient.ContainerRequest(resource, null, null, priority);

            System.out.println("making container request " + index);

            rmClient.addContainerRequest(containerRequest);
        }

        int responseId = 0;

        int completedContainers = 0;

        while (completedContainers < n) {
            AllocateResponse response = rmClient.allocate(responseId++);

            for (Container container : response.getAllocatedContainers()) {
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(Collections.singletonList("echo " + responseId + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));

                System.out.println("Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
            }

            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;

                System.out.println("complete container " + status.getContainerId());
            }

            Thread.sleep(1000L);

        }
    }
}
