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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SizedUtil;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus.Builder;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 
 * Aggregator that sums integral number values
 * 
 * 
 * @since 0.1
 */
abstract public class NumberSumhAggregator extends BaseAggregator {
    private long sum = 0;
    private byte[] buffer;
    private byte[] aggbuffer;

    protected final ImmutableBytesWritable valueByteArray = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);

    private HyperLogLogPlus mergedHLL = new HyperLogLogPlus(16, 25);

    public NumberSumhAggregator(SortOrder sortOrder) {
        super(sortOrder);
    }

    public NumberSumhAggregator(SortOrder sortOrder,
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

    abstract protected PDataType getInputDataType();

    private int getBufferLength() {
        //return getDataType().getByteSize();
        return 64 * 1024;
    }

    private void initBuffer() {
        //System.out.println("*******************   initBuffer ****************");
        buffer = new byte[getBufferLength()];
    }


    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        //System.out.println("*******************  aggregate HERE");
        if (ptr == null) {
            //System.out.println("*******************  PTR is NULL HERE");
        }else{
            //System.out.println("*******************  PTR is NOT NULL HERE: "+ptr.getLength()); //NOT NULL
            //System.out.println("******************* ptr.toString(): "+ptr.toString());
        }
        long value=0;

        try {
            byte[] byteArray=ptr.copyBytes();
            //System.out.println("********** byteArray size  :"+byteArray.length);
            //System.out.println("********** byteArray String  :"+byteArray.toString());

            HyperLogLogPlus hll2 = new HyperLogLogPlus(16,25);
            hll2 =  Builder.build(byteArray);
            long value1=hll2.cardinality();
            System.out.println("*******************  HLL_Cardinality: " + value1);

            mergedHLL.addAll(hll2);
            value = mergedHLL.cardinality();
            System.out.println("*******************  mergedHLL_Cardinality: " + value);

            aggbuffer=mergedHLL.getBytes();
            //System.out.println("********** aggbuffer size  :" + aggbuffer.length);
            System.out.println("********** aggbuffer String  :" + aggbuffer.toString());

        }catch (Exception e){
            e.printStackTrace();
        }
        sum = value;
        if (buffer == null) {
            initBuffer();
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        //System.out.println("*******************  INSIDE EVALUATE");
        if (buffer == null) {        //NOT NULL
            //System.out.println("*********NOT EXEC");
            if (isNullable()) {
                return false;
            }
            initBuffer();
        }
        if (ptr == null) {
            //System.out.println("*************NOT EXEC:  PTR is NULL HERE1");
        } else {
            //System.out.println("*******************  PTR is NOT NULL HERE1");
        }

        //System.out.println("************ buffer.length: " + buffer.length);
        //System.out.println("************ buffer.toString: " + buffer.toString());

        try {
            //buffer = mergedHLL.getBytes();
            valueByteArray.set(mergedHLL.getBytes(), 0, mergedHLL.getBytes().length);

        } catch (Exception e) {
            e.printStackTrace();
        }
        //System.out.println("********** buffer size AFTER  :" + buffer.length);
        //System.out.println("********** buffer String  AFTER :" + buffer.toString());

        //ptr.set(buffer);

        ptr.set(valueByteArray.copyBytes(), valueByteArray.getOffset(), valueByteArray.getLength());

        byte[] byteArray1 = ptr.copyBytes();
        System.out.println("********** byteArray1 size  :" + byteArray1.length);
        System.out.println("********** byteArray1 String  :" + byteArray1.toString());

//        byte[] byteGArray1 = ptr.get();
//        System.out.println("********** byteGArray1 size  :" + byteGArray1.length);
//        System.out.println("********** byteGArray1 String  :" + byteGArray1.toString());


        try {
            HyperLogLogPlus hllFinal = new HyperLogLogPlus(16, 25);
            hllFinal = Builder.build(byteArray1);
            long value1 = hllFinal.cardinality();
            System.out.println("*******************  hllFinal_Cardinality: " + value1);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

//    @Override
//    public final PDataType getDataType() {
//        return PLong.INSTANCE;
//    }


    @Override
    public final PDataType getDataType() {
        return PVarbinary.INSTANCE;
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

//    @Override
//    public int getSize() {
//        return super.getSize() + SizedUtil.LONG_SIZE + SizedUtil.ARRAY_SIZE
//                + getBufferLength();
//    }

    @Override
    public int getSize() {
        return super.getSize() + SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE;
    }

}
