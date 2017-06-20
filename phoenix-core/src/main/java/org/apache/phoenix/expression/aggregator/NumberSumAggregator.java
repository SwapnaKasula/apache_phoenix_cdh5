/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression.aggregator;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.SizedUtil;

/**
 * 
 * Aggregator that sums integral number values
 * 
 * 
 * @since 0.1
 */
abstract public class NumberSumAggregator extends BaseAggregator {
    private long sum = 0;
    private byte[] buffer;

    public NumberSumAggregator(SortOrder sortOrder) {
        super(sortOrder);
    }

    public NumberSumAggregator(SortOrder sortOrder,
            ImmutableBytesWritable ptr) {
        this(sortOrder);
        if (ptr != null) {
            //System.out.println("*******************   ptr is not null ****************");
            initBuffer();
            sum = PLong.INSTANCE.getCodec().decodeLong(ptr, sortOrder);
            //System.out.println("*******************   sum HERE **************** "+sum);
        }else{
            //System.out.println("**************Step 3:   ptr is NULL ****************");
        }
    }

    public long getSum() {
        return sum;
    }

    abstract protected PDataType getInputDataType();

    private int getBufferLength() {
        return getDataType().getByteSize();
    }

    private void initBuffer() {
        //System.out.println("*******************   initBuffer ****************");
        buffer = new byte[getBufferLength()];
    }

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        System.out.println("*******************  aggregate HERE");
        if (ptr == null) {
            //System.out.println("*******************  PTR is NULL HERE");
        }else{
            //System.out.println("*******************  PTR is NOT NULL HERE: "+ptr.getLength());
            //System.out.println("******************* ptr.toString(): "+ptr.toString());
        }
        // Get either IntNative or LongNative depending on input type
        long value = getInputDataType().getCodec().decodeLong(ptr, sortOrder);
        sum += value;
        //System.out.println("*******************  value HERE :"+value);
        //System.out.println("*******************  sum HERE :"+sum);
        if (buffer == null) {
            initBuffer();
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        //System.out.println("*******************  INSIDE EVALUATE");
        if (buffer == null) {
            //System.out.println("******************* NOT EXEC");
            if (isNullable()) {
                //System.out.println("******************* NOT EXEC1");
                return false;
            }
            initBuffer();
        }

        if (ptr == null) {
            //System.out.println("*************NOT EXEC:  PTR is NULL HERE1");
        }else{
            //System.out.println("*******************  PTR is NOT NULL HERE1: ");
        }


        //System.out.println("************ SUM here: "+sum);
        //System.out.println("************ buffer.length: "+buffer.length);
        //System.out.println("************ buffer.toString: " + buffer.toString());
        getDataType().getCodec().encodeLong(sum, buffer, 0);
        ptr.set(buffer);
        return true;
    }

    @Override
    public final PDataType getDataType() {
        return PLong.INSTANCE;
    }

    @Override
    public void reset() {
        sum = 0;
        buffer = null;
        super.reset();
    }

    @Override
    public String toString() {
        return "SUM [sum=" + sum + "]";
    }

    @Override
    public int getSize() {
        return super.getSize() + SizedUtil.LONG_SIZE + SizedUtil.ARRAY_SIZE
                + getBufferLength();
    }

}
