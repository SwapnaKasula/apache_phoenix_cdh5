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
package org.apache.phoenix.expression.function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.DecimalSumAggregator;
import org.apache.phoenix.expression.aggregator.NumberSumhAggregator;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.SumhAggregateParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.*;

import java.math.BigDecimal;
import java.util.List;
import java.sql.Timestamp;


/**
 * 
 * Built-in function for SUM aggregation function.
 *
 * 
 * @since 0.1
 */
@BuiltInFunction(name= SumhAggregateFunction.NAME, nodeClass=SumhAggregateParseNode.class, args= {@Argument(allowedTypes={PVarchar.class})} )
public class SumhAggregateFunction extends DelegateConstantToCountAggregateFunction {
    public static final String NAME = "SUMH2";

    public SumhAggregateFunction() {
        //System.out.println("*******************  SumhAggregateFunction CONST1");
    }

    // TODO: remove when not required at built-in func register time
    public SumhAggregateFunction(List<Expression> childExpressions){
        super(childExpressions, null);
        //System.out.println("*******************  SumhAggregateFunction CONST2");
    }

    public SumhAggregateFunction(List<Expression> childExpressions, CountAggregateFunction delegate){
        super(childExpressions, delegate);
        //System.out.println("*******************  SumhAggregateFunction CONST3");
    }
    
    private Aggregator newAggregator(final PDataType type, SortOrder sortOrder, ImmutableBytesWritable ptr) {
        //System.out.println("*******************  newAggregator NumberSumAggregator :" + type.toString());
        return new NumberSumhAggregator(sortOrder, ptr) {
          //return new NumberSumAggregator(sortOrder, ptr) {
            @Override
            protected PDataType getInputDataType() {
                //System.out.println("*******************   inside my getInputDataType : "+type);
              return type;
            }
        };

    }

    @Override
    public Aggregator newClientAggregator() {
        //System.out.println("*******************  newClientAggregator CALLED :"+getDataType().toString()+", timestamp: "+ new Timestamp(new java.util.Date().getTime()) );
        return newAggregator(getDataType(), SortOrder.getDefault(), null);
    }
    
    @Override
    public Aggregator newServerAggregator(Configuration conf) {
        //System.out.println("*******************  newServerAggregator CALLED1 : timestamp: "+ new Timestamp(new java.util.Date().getTime()) );
        Expression child = getAggregatorExpression();
        //System.out.println("*******************  child.getDataType() CALLED1 :"+child.getDataType().toString());
        return newAggregator(child.getDataType(), child.getSortOrder(), null);
    }
    
    @Override
    public Aggregator newServerAggregator(Configuration conf, ImmutableBytesWritable ptr) {
        //System.out.println("*******************  newServerAggregator CALLED2 with PTR " + new Timestamp(new java.util.Date().getTime()) );
        Expression child = getAggregatorExpression();
        //System.out.println("*******************  child.getDataType() with PTR :"+child.getDataType().toString());
        return newAggregator(child.getDataType(), child.getSortOrder(), ptr);
    }
    
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        //System.out.println("*******************   inside my evaluate ****************");

        if (!super.evaluate(tuple, ptr)) {
            return false;
        }
        if (isConstantExpression()) {
            //System.out.println("*******************   inside my isConstantExpression ****************");
            PDataType type = getDataType();
            Object constantValue = ((LiteralExpression)children.get(0)).getValue();
            if (type == PDecimal.INSTANCE) {
                BigDecimal value = ((BigDecimal)constantValue).multiply((BigDecimal) PDecimal.INSTANCE.toObject(ptr, PLong.INSTANCE));
                ptr.set(PDecimal.INSTANCE.toBytes(value));
            } else {
                long constantLongValue = ((Number)constantValue).longValue();
                long value = constantLongValue * type.getCodec().decodeLong(ptr, SortOrder.getDefault());
                byte[] resultPtr = new byte[type.getByteSize()];
                type.getCodec().encodeLong(value, resultPtr, 0);
                ptr.set(resultPtr);
            }
        }
        return true;
    }

//    @Override
//    public PDataType getDataType() {
//        System.out.println("*******************   inside getDataTypegetDataTypegetDataType ****************");
//        if (super.getDataType() == PDecimal.INSTANCE) {
//          return PDecimal.INSTANCE;
//        } else if (PDataType.equalsAny(super.getDataType(), PUnsignedFloat.INSTANCE, PUnsignedDouble.INSTANCE,
//                PFloat.INSTANCE, PDouble.INSTANCE)) {
//          return PDouble.INSTANCE;
//        } else {
//          return PLong.INSTANCE;
//        }
//    }

    @Override
    public PDataType getDataType() {
        //System.out.println("*******************   inside getDataTypegetDataTypegetDataType ****************");
        PDataType baseType = getAggregatorExpression().getDataType().isArrayType() ? PVarbinary.INSTANCE : getAggregatorExpression().getDataType();
        //System.out.println("*******************   inside getDataTypegetDataTypegetDataType baseType: "+baseType.toString());
        //System.out.println("*******************   inside getDataTypegetDataTypegetDataType super.getDataType(): "+super.getDataType().toString());
        return baseType;
        //return PDataType.fromTypeId(baseType.getSqlType() + PDataType.ARRAY_TYPE_BASE);
    }

//    @Override
//    public PDataType getDataType() {
//        return type;
//    }

    @Override
    public String getName() {
        return NAME;
    }
}
