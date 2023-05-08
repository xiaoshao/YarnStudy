package org.example;

import java.util.Map;

public class YarnClientAdapter {
    public static void main(String[] args) {

        Map<String, String> getenv = System.getenv();

        for (Map.Entry<String, String> entry : getenv.entrySet()) {
            System.out.println(entry.getKey() + "==" +entry.getValue());
        }

//        YarnClient yarnClient = YarnClientFactory.createYarnClient();
//
//        YarnClientWrapper yarnClientWrapper = new YarnClientWrapper(yarnClient);
//        ApplicationId application1 = yarnClientWrapper.createApplication(YarnClientFactory.getYarnConfiguration());
//
//        System.out.println(application1.getId());
//
//        List<ApplicationReport> applications = yarnClient.getApplications();
//
//        for (ApplicationReport application : applications) {
//            System.out.println(application);
//        }
    }
}