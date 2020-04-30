package org.apache.hadoop.hive.json;

import com.google.common.annotations.VisibleForTesting;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class HiveTypeWrapper implements Writable {
    private AtomicReference<Configuration> conf = new AtomicReference();

    private HiveType instance;

    @VisibleForTesting
    Map<Class<?>, Byte> classToIdMap = new ConcurrentHashMap();

    @VisibleForTesting
    Map<Byte, Class<?>> idToClassMap = new ConcurrentHashMap();

    private void init() {
        this.addToMap(StructType.class, (byte)-127);
        this.addToMap(UnionType.class, (byte)-126);
        this.addToMap(ListType.class, (byte)-125);
        this.addToMap(NumericType.class, (byte)-124);
        this.addToMap(StringType.class, (byte)-123);
        this.addToMap(NullType.class, (byte)-122);
        this.addToMap(BooleanType.class, (byte)-121);
    }

    protected HiveTypeWrapper() {
        init();
    }

    public HiveTypeWrapper(HiveType instance) {
        init();
        this.instance = instance;
    }

    private synchronized void addToMap(Class<?> clazz, byte id) {
        if (this.classToIdMap.containsKey(clazz)) {
            byte b = (Byte)this.classToIdMap.get(clazz);
            if (b != id) {
                throw new IllegalArgumentException("Class " + clazz.getName() + " already registered but maps to " + b + " and not " + id);
            }
        }

        if (this.idToClassMap.containsKey(id)) {
            Class<?> c = (Class)this.idToClassMap.get(id);
            if (!c.equals(clazz)) {
                throw new IllegalArgumentException("Id " + id + " exists but maps to " + c.getName() + " and not " + clazz.getName());
            }
        }

        this.classToIdMap.put(clazz, id);
        this.idToClassMap.put(id, clazz);
    }

    protected Class<?> getClass(byte id) {
        return (Class)this.idToClassMap.get(id);
    }

    protected byte getId(Class<?> clazz) {
        return this.classToIdMap.containsKey(clazz) ? (Byte)this.classToIdMap.get(clazz) : -1;
    }

    public Configuration getConf() {
        return (Configuration)this.conf.get();
    }

    public void setConf(Configuration conf) {
        this.conf.set(conf);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(this.getId(this.instance.getClass()));
        this.instance.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Writable value = (Writable) ReflectionUtils.newInstance(this.getClass(in.readByte()), this.getConf());
        value.readFields(in);
        this.instance = (HiveType) value;
    }

    public HiveType getInstance() {
        return this.instance;
    }
}
