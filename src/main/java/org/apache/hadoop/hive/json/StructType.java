/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.json;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;

/**
 * Model structs.
 */
class StructType extends HiveType {
  final SortedMapWritable<Text> fields = new SortedMapWritable<>();

  StructType() {
    super(Kind.STRUCT);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder("struct<");
    boolean first = true;
    for (Map.Entry<Text, Writable> field : fields.entrySet()) {
      if (!first) {
        buf.append(',');
      } else {
        first = false;
      }
      buf.append(field.getKey());
      buf.append(':');
      buf.append(field.getValue().toString());
    }
    buf.append(">");
    return buf.toString();
  }

  public StructType addField(String name, HiveType fieldType) {
    fields.put(new Text(name), fieldType);
    return this;
  }

  @Override
  public boolean equals(Object other) {
    return super.equals(other) && fields.equals(((StructType) other).fields);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode() * 3;
    for (Map.Entry<Text, Writable> pair : fields.entrySet()) {
      result += pair.getKey().hashCode() * 17 + pair.getValue().hashCode();
    }
    return result;
  }

  @Override
  public boolean subsumes(HiveType other) {
    return other.kind == Kind.NULL || other.kind == Kind.STRUCT;
  }

  @Override
  public void merge(HiveType other) {
    if (other.getClass() == StructType.class) {
      StructType otherStruct = (StructType) other;
      for (Map.Entry<Text, Writable> pair : otherStruct.fields.entrySet()) {
        HiveType ourField = (HiveType) fields.get(pair.getKey());
        HiveType value = (HiveType) pair.getValue();
        if (ourField == null) {
          fields.put(pair.getKey(), value);
        } else if (ourField.subsumes(value)) {
          ourField.merge(value);
        } else if (value.subsumes(ourField)) {
          value.merge(ourField);
          fields.put(pair.getKey(), value);
        } else {
          fields.put(pair.getKey(), new UnionType(ourField, value));
        }
      }
    }
  }

  public void printFlat(PrintStream out, String prefix) {
    prefix = prefix + ".";
    for (Map.Entry<Text, Writable> field : fields.entrySet()) {
      ((HiveType) field.getValue()).printFlat(out, prefix + field.getKey());
    }
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    super.write(dataOutput);
    final SortedMapWritable<Text> toWritable = new SortedMapWritable<>();
    Iterator<Text> it = this.fields.keySet().iterator();
    while (it.hasNext()) {
      Text key = it.next();
      HiveType value = (HiveType) this.fields.get(key);
      toWritable.put(key, new HiveTypeWrapper(value));
    }
    toWritable.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    super.readFields(dataInput);
    final SortedMapWritable<Text> toPlain = new SortedMapWritable<>();
    toPlain.readFields(dataInput);

    Iterator<Text> it = toPlain.keySet().iterator();
    while (it.hasNext()) {
      Text key = it.next();
      HiveTypeWrapper value = (HiveTypeWrapper) toPlain.get(key);
      this.fields.put(key, value.getInstance());
    }
  }
}
