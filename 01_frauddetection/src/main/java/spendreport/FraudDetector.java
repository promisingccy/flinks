/*
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

package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(FraudDetector.class);

	/*
	 * 欺诈检测器条件：
	 * @出现小于 $1 美元的交易后紧跟着一个大于 $500 的交易
	 * @设置了一分钟的超时
	 * -> 输出报警信息
	 */

	//最小金额
	private static final double SMALL_AMOUNT = 1.00;
	//最大金额
	private static final double LARGE_AMOUNT = 500.00;
	//持续时间
	private static final long ONE_MINUTE = 60 * 1000;

	private transient ValueState<Boolean> flagState;
	private transient ValueState<Long> timerState;

	@Override
	//初始化程序
	public void open(Configuration parameters) {
		LOG.info("========== open");
		//设置金额检测状态变量 flagState
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN);
		// 可能存在的值： null/true/false
		flagState = getRuntimeContext().getState(flagDescriptor);

		//设置时间检测状态变量 timerState
		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
				"timer-state",
				Types.LONG);
		timerState = getRuntimeContext().getState(timerDescriptor);
	}

	@Override
	//处理每一个元素
	public void processElement(
			Transaction transaction,//当前这条数据
			Context context,//上下文获取，如当前时间戳
			Collector<Alert> collector) throws Exception {
		LOG.info("========== processElement");
		LOG.info(transaction.toString());

		// 获取当前的 金额检测状态变量 flagState
		Boolean lastTransactionWasSmall = flagState.value();

		// 金额检测状态变量是否被设置
		if (lastTransactionWasSmall != null) {
			//当前的消费量 > 500
			if (transaction.getAmount() > LARGE_AMOUNT) {
				LOG.info("========== LARGE_AMOUNT alert");
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());
				//输出当前这一条数据
				collector.collect(alert);
			}
			// 清除已经被设置的告警状态变量，为了下次检测过滤
			cleanUp(context);
		}

		// 当前的消费量 < 1.00 ，满足告警前置条件，更新告警状态变量-标记/时间
		if (transaction.getAmount() < SMALL_AMOUNT) {
			LOG.info("========== SMALL_AMOUNT update");
			// 更新金额状态
			flagState.update(true);

			// 更新时间状态
			long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
			// 增加定时器
			context.timerService().registerProcessingTimeTimer(timer);
			timerState.update(timer);
		}
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
		LOG.info("========== onTimer");
		// 清除状态变量
		timerState.clear();
		flagState.clear();
	}

	private void cleanUp(Context ctx) throws Exception {
		LOG.info("========== cleanUp");
		// 删除定时器
		Long timer = timerState.value();
		ctx.timerService().deleteProcessingTimeTimer(timer);

		// 清除状态变量
		timerState.clear();
		flagState.clear();
	}
}
