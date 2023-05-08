package org.example.wrapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.file.Paths;

public class HdfsClientWrapper {

    public void upload(String localPath, String hdfsPath) throws IOException {

        Configuration conf = new Configuration();
        conf.addResource("/Users/shaozengwei/project/tools/hadoop-3.3.3/etc/hadoop/core-site.xml");
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        Path yarnApplicationDir = new Path("/srv/yarn/application");

        if (fileSystem.exists(yarnApplicationDir)) {
            if (!fileSystem.isDirectory(yarnApplicationDir)) {
                throw new IllegalStateException("the " + yarnApplicationDir + " is not dir");
            }
        } else {
            fileSystem.mkdirs(yarnApplicationDir);
        }

        fileSystem.copyFromLocalFile(false, true, new Path(localPath), new Path(yarnApplicationDir, hdfsPath));
    }
}
