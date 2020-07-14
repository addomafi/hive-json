package org.apache.hadoop.hive.json;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class TestDataSchemaDiscovery {

    private final static String INPUT = "src/test/resources";
    private final static String BASEDIR = System.getProperty("test.build.data",
            "target/test-dir");
    private final static String OUTPUT = BASEDIR + "/output";

    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    System.out.println("Could not delete directory after test!");
                    return false;
                }
            }
        }

        // The directory is now empty so delete it
        return dir.delete();
    }

    @Before
    public void setup() throws Exception {
        deleteDir(new File(OUTPUT));
    }

    @Test
    public void testEntityDiscovery() throws Exception {
        String args[] = new String[4];
        args[0] = OUTPUT;
        args[1] = "vector_wallet_sender,vector_sender_withdrawals";
        args[2] = INPUT;
        args[3] = INPUT;

        DataSchemaDiscovery wm = new DataSchemaDiscovery();
        ToolRunner.run(new Configuration(), wm, args);
        HiveType hiveType = wm.getHiveType();

        // outputs MUST match
        assertEquals(hiveType, null);
    }
}
