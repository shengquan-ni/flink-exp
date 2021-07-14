package org.apache.flink.runtime.recovery;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

public class RecoveryUtils {

    public static final int NO_PRINT = 0;
    public static final int PRINT_RECEIVE = 1;
    public static final int PRINT_PROCESS = 2;
    public static final int PRINT_SEND = 4;
    public static final int PRINT_DIRECT_CALL = 8;

    private static String hostAddress;
    public static int printLevel = NO_PRINT;
    public static boolean isEnabled = false;

    public static FileSystem getHDFS(String addr){
        FileSystem hdfs = null;
        hostAddress = addr;
        Configuration hdfsConf = new Configuration();
        hdfsConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false");
        try {
                hdfs = FileSystem.get(new URI(hostAddress), hdfsConf);
            } catch (Exception e) {
                e.printStackTrace();
            }
        return hdfs;
    }

    public static boolean needPrint(int targetLevel){
        return (printLevel & targetLevel) != 0;
    }
}
