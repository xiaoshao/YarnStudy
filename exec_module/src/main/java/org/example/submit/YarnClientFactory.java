package org.example.submit;

import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.nio.file.Path;
import java.nio.file.Paths;

public class YarnClientFactory {

    public static YarnClient createYarnClient() {
        YarnClient yarnClient = YarnClient.createYarnClient();
        getYarnConfiguration();
        yarnClient.init(getYarnConfiguration());

        yarnClient.start();
        return yarnClient;
    }

    public static YarnConfiguration getYarnConfiguration() {
        YarnConfiguration configuration = new YarnConfiguration();

        String hadoopHome = System.getenv("HADOOP_HOME");
        Path yarnSiteConfiguration = Paths.get(hadoopHome, "etc", "hadoop", "yarn-site.xml");
        configuration.addResource(yarnSiteConfiguration.toString());
        return configuration;
    }

}
