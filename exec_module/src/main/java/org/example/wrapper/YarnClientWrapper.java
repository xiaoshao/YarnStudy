package org.example.wrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class YarnClientWrapper {

    private YarnClient yarnClient;

    public YarnClientWrapper(YarnClient yarnClient) {
        this.yarnClient = yarnClient;
    }

    public ApplicationId createApplication(YarnConfiguration conf) throws IOException, YarnException, InterruptedException {

        HdfsClientWrapper hdfsClientWrapper = new HdfsClientWrapper();

        hdfsClientWrapper.upload("/Users/shaozengwei/projects/YarnApplicationStudy/task_module/target/task_module-1.0-SNAPSHOT.jar", "task_module.jar");

        YarnClientApplication application = yarnClient.createApplication();

        GetNewApplicationResponse newApplicationResponse = application.getNewApplicationResponse();

        ApplicationSubmissionContext appsubmitContext = application.getApplicationSubmissionContext();

        appsubmitContext.setApplicationType("datafusion");

        appsubmitContext.setResource(Resource.newInstance(2, 2));

        appsubmitContext.setPriority(Priority.newInstance(0));
        appsubmitContext.setQueue("default");

        ContainerLaunchContext amContainerSpec = createAmContainer(conf);
        appsubmitContext.setAMContainerSpec(amContainerSpec);
        yarnClient.submitApplication(appsubmitContext);

        ApplicationReport applicationReport = yarnClient.getApplicationReport(newApplicationResponse.getApplicationId());
        YarnApplicationState state = applicationReport.getYarnApplicationState();

        while (state != YarnApplicationState.FINISHED && state != YarnApplicationState.KILLED || state != YarnApplicationState.FAILED) {
            Thread.sleep(1000L);
            applicationReport = yarnClient.getApplicationReport(newApplicationResponse.getApplicationId());
            state = applicationReport.getYarnApplicationState();
        }
        System.out.println("application " + newApplicationResponse.getApplicationId() + "  finished with " + state + " at " + applicationReport.getFinishTime());
        return newApplicationResponse.getApplicationId();
    }

    private ContainerLaunchContext createAmContainer(YarnConfiguration conf) throws IOException {

        ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);

        // 设置 classpath 环境变量
        Map<String, String> appMasterEnv = new HashMap<>();
        String[] settings = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);

        for (String setting : settings) {
            Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                    setting.trim());
        }
        Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "*.jar");

        containerLaunchContext.setEnvironment(appMasterEnv);

        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        Configuration conf1 = new Configuration();
        conf1.set("fs.defaultFS", "hdfs://localhost:9000");
        FileStatus fileStatus = FileSystem.get(conf1).getFileStatus(new Path("hdfs://localhost:9000/srv/yarn/application/task_module.jar"));
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(new Path("hdfs://localhost:9000/srv/yarn/application/task_module.jar")));
        appMasterJar.setSize(fileStatus.getLen());
        appMasterJar.setTimestamp(fileStatus.getModificationTime());

        containerLaunchContext.setLocalResources(Collections.singletonMap("task_module.jar", appMasterJar));

        ArrayList<String> commands = new ArrayList<>();
        commands.add("ls -al && java -jar task_module.jar 2");

        containerLaunchContext.setCommands(commands);

        return containerLaunchContext;
    }


}
