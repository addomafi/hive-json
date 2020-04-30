package org.apache.hadoop.hive.json;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

public class HiveTypeRecordWriter<K, V> extends RecordWriter<K, V> {
    protected DataOutputStream out;
    private final byte[] recordSeparator;
    private final byte[] fieldSeparator;

    public HiveTypeRecordWriter(DataOutputStream out, String fieldSeprator, String recordSeprator) {
        this.out = out;
        this.fieldSeparator = fieldSeprator.getBytes(StandardCharsets.UTF_8);
        this.recordSeparator = recordSeprator.getBytes(StandardCharsets.UTF_8);
    }

    public HiveTypeRecordWriter(DataOutputStream out) {
        this(out, "\t", "\n");
    }

    private void writeObject(Object o, K key) throws IOException {
        if (o instanceof Text) {
            Text to = (Text) o;
            this.out.write(to.getBytes(), 0, to.getLength());
        } else if (o instanceof HiveTypeWrapper) {
            HiveTypeWrapper val = (HiveTypeWrapper) o;
            val.getInstance().printFlat(new PrintStream(this.out), key.toString());
        }

    }

    private void writeObject(Object o) throws IOException {
        if (o instanceof Text) {
            Text to = (Text) o;
            this.out.write(to.getBytes(), 0, to.getLength());
        } else if (o instanceof HiveTypeWrapper) {
            HiveTypeWrapper val = (HiveTypeWrapper) o;
            val.getInstance().printFlat(new PrintStream(this.out), "root");
        }

    }

    public synchronized void write(K key, V value) throws IOException {
        boolean nullKey = key == null || key instanceof NullWritable;
        boolean nullValue = value == null || value instanceof NullWritable;
        if (!nullKey || !nullValue) {
            if (!nullValue) {
                if (!nullKey) {
//                    this.writeObject(key);
//                    this.out.write(this.fieldSeparator);
                    this.writeObject(value, key);
                } else {
                    this.writeObject(value);
                }
            }

            this.out.write(recordSeparator);//write custom record separator instead of NEW_LINE
        }
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
        this.out.close();
    }
}