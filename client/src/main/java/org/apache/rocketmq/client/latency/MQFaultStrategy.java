/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息队列的故障策略
 *
 * 根据currentLatency本次消息发送的延迟时间，从latencyMax尾部向前找到一个比currentLatency小的索引index，如果没有找到，则返回0。
 * 然后根据这个索引从notAvailableDuration 数组中取出对应的时间，在这个时长内，Broker将设置为不可用。
 *
 * 开启了故障延迟机制，即设置了sendLatencyFaultEnable为true，其实是一种悲观的做法。
 * 当消息发送者遇到一次消息发送失败后，就会悲观地认为Broker不可用，在接下来地一段时间就不再向其发送消息，直接避免该Broker。
 * 而不开启延迟规避机制，只会在本次消息发送地重试过程中规避该Broker，下一次消息发送还是会继续尝试。
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;
    /**发送延迟时间*/
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    /**Broker故障规避时间*/
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // sendLatencyFaultEnable: 是否开启Broker故障延迟机制。默认为false，不开启。
        if (this.sendLatencyFaultEnable) {
            // 故障延迟机制
            try {
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                // 1、轮询获取一个消息队列
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0) {
                        pos = 0;
                    }
                    // 2、验证消息队列是否可用
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        // 找到可用的MessageQueue，直接返回
                        return mq;
                    }
                }
                // 获取最近故障的Broker
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                // 获取写队列的数量
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    // 获取消息队列
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    // 从故障列表中移除Broker
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            // 选择一个消息队列
            return tpInfo.selectOneMessageQueue();
        }
        // 选择一个消息队列，选择
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        // 如果在发送消息的时候发生了异常，会调用 updateFaultItem 方法，isolation为true
        if (this.sendLatencyFaultEnable) {
            // isolation为true时，使用默认30秒作为 computeNotAvailableDuration 的参数。
            // 如果为false，则使用本次消息发送延迟作为 computeNotAvailableDuration 的参数
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 计算因本次消息发送故障需要规避Broker的时长，也就是接下来多长时间内，该Broker将不参与消息发送队列的载体。
     *
     * @param currentLatency
     * @return
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
