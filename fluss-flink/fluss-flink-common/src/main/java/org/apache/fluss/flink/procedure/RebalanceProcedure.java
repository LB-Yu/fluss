/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.procedure;

import org.apache.fluss.cluster.rebalance.GoalType;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Procedure to rebalance. */
public class RebalanceProcedure extends ProcedureBase {

    /**
     * As flink call don't support input a nested type like 'ARRAY'. So priorityGoals is defined as
     * a String type, and different goals are split by ';'.
     */
    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "priorityGoals", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "dryRun", type = @DataTypeHint("BOOLEAN"), isOptional = true)
            })
    public String[] call(ProcedureContext context, String priorityGoals, @Nullable Boolean dryRun)
            throws Exception {
        String[] split = priorityGoals.split(";");
        if (split.length == 0) {
            return new String[] {};
        }

        List<GoalType> goalTypes = new ArrayList<>();
        for (String goal : split) {
            goalTypes.add(GoalType.valueOf(goal.toUpperCase()));
        }
        Map<TableBucket, RebalancePlanForBucket> planForBucketMap =
                admin.rebalance(goalTypes, dryRun != null && dryRun).get();
        return planForBucketMapToString(planForBucketMap);
    }

    private static String[] planForBucketMapToString(
            Map<TableBucket, RebalancePlanForBucket> planForBucketMap) {
        if (planForBucketMap == null || planForBucketMap.isEmpty()) {
            return new String[] {};
        }

        return planForBucketMap.values().stream()
                .map(RebalancePlanForBucket::toString)
                .toArray(String[]::new);
    }
}
