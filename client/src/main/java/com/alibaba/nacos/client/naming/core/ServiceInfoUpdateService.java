/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.client.naming.core;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.remote.NamingClientProxy;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Service information update service.
 *
 * @author xiweng.yy
 */
public class ServiceInfoUpdateService implements Closeable {

    private static final long DEFAULT_DELAY = 1000L;

    private static final int DEFAULT_UPDATE_CACHE_TIME_MULTIPLE = 6;

    private final Map<String, ScheduledFuture<?>> futureMap = new HashMap<String, ScheduledFuture<?>>();

    private final ServiceInfoHolder serviceInfoHolder;

    private final ScheduledExecutorService executor;

    private final NamingClientProxy namingClientProxy;

    private final InstancesChangeNotifier changeNotifier;

    public ServiceInfoUpdateService(Properties properties, ServiceInfoHolder serviceInfoHolder,
            NamingClientProxy namingClientProxy, InstancesChangeNotifier changeNotifier) {
        // 定时任务调度执行器，线程池大小为处理器核数的一半，可以通过参数"namingPollingThreadCount”指定;
        // 职责：调度器用于执行UpdateTask，延迟1秒执行。
        this.executor = new ScheduledThreadPoolExecutor(initPollingThreadCount(properties),
                new NameThreadFactory("com.alibaba.nacos.client.naming.updater"));
        this.serviceInfoHolder = serviceInfoHolder;
        this.namingClientProxy = namingClientProxy;
        this.changeNotifier = changeNotifier;
    }

    private int initPollingThreadCount(Properties properties) {
        if (properties == null) {
            return UtilAndComs.DEFAULT_POLLING_THREAD_COUNT;
        }
        return ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.NAMING_POLLING_THREAD_COUNT),
                UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }

    /**
     * Schedule update if absent.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters
     */
    public void scheduleUpdateIfAbsent(String serviceName, String groupName, String clusters) {
        String serviceKey = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        if (futureMap.get(serviceKey) != null) {
            return;
        }
        synchronized (futureMap) {
            if (futureMap.get(serviceKey) != null) {
                return;
            }
            // 构建UpdateTask
            ScheduledFuture<?> future = addTask(new UpdateTask(serviceName, groupName, clusters));
            futureMap.put(serviceKey, future);
        }
    }

    private synchronized ScheduledFuture<?> addTask(UpdateTask task) {
        return executor.schedule(task, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
    }

    /**
     * Stop to schedule update if contain task.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param clusters    clusters
     */
    public void stopUpdateIfContain(String serviceName, String groupName, String clusters) {
        String serviceKey = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        if (!futureMap.containsKey(serviceKey)) {
            return;
        }
        synchronized (futureMap) {
            if (!futureMap.containsKey(serviceKey)) {
                return;
            }
            futureMap.remove(serviceKey);
        }
    }

    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executor, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }

    /**
     * 当我们开启订阅时subscribe时，会通过调度器生成一个UpdateTask；
     * UpdateTask每个6秒钟（最长为1分钟）会从注册中心获取实例Instance列表，
     * 当检测到实例Instance列表有变更时会通过NotifyCenter.publishEvent发布实例变更事件;
     *
     *
     * UpdateTask主要逻辑为如果服务缓存刷新时间过期，
     * 则会从注册中心查询最新服务信息，同时刷新缓存更新时间。
     * 并定时调度去更新服务注册信息，更新的频率最小为6秒，最长为1分钟。
     * 当更新无异常时更新频率为6秒，当发生异常时最长频率为1分钟。
     */
    public class UpdateTask implements Runnable {

        long lastRefTime = Long.MAX_VALUE;

        private final String serviceName;

        private final String groupName;

        private final String clusters;

        private final String groupedServiceName;

        private final String serviceKey;

        /**
         * the fail situation. 1:can't connect to server 2:serviceInfo's hosts is empty
         */
        private int failCount = 0;

        public UpdateTask(String serviceName, String groupName, String clusters) {
            this.serviceName = serviceName;
            this.groupName = groupName;
            this.clusters = clusters;
            this.groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
            this.serviceKey = ServiceInfo.getKey(groupedServiceName, clusters);
        }

        @Override
        public void run() {
            long delayTime = DEFAULT_DELAY;

            try {
                // 判断该注册的Service是否被订阅，如果没有订阅则不再执行
                if (!changeNotifier.isSubscribed(groupName, serviceName, clusters) && !futureMap.containsKey(serviceKey)) {
                    NAMING_LOGGER
                            .info("update task is stopped, service:" + groupedServiceName + ", clusters:" + clusters);
                    return;
                }

                // 获取缓存的service信息
                ServiceInfo serviceObj = serviceInfoHolder.getServiceInfoMap().get(serviceKey);
                if (serviceObj == null) {
                    // 根据serviceName从注册中心服务端获取Service信息
                    serviceObj = namingClientProxy.queryInstancesOfService(serviceName, groupName, clusters, 0, false);
                    serviceInfoHolder.processServiceInfo(serviceObj);
                    lastRefTime = serviceObj.getLastRefTime();
                    return;
                }

                // 过期服务（服务的最新更新时间小于等于缓存刷新时间），从注册中心重新查询
                if (serviceObj.getLastRefTime() <= lastRefTime) {
                    serviceObj = namingClientProxy.queryInstancesOfService(serviceName, groupName, clusters, 0, false);
                    // 处理Service消息
                    serviceInfoHolder.processServiceInfo(serviceObj);
                }
                // 刷新更新时间
                lastRefTime = serviceObj.getLastRefTime();
                if (CollectionUtils.isEmpty(serviceObj.getHosts())) {
                    incFailCount();
                    return;
                }
                // 下次更新缓存时间设置，默认为6秒
                // TODO multiple time can be configured.
                delayTime = serviceObj.getCacheMillis() * DEFAULT_UPDATE_CACHE_TIME_MULTIPLE;
                // 重置失败数量为0
                resetFailCount();
            } catch (Throwable e) {
                incFailCount();
                NAMING_LOGGER.warn("[NA] failed to update serviceName: " + groupedServiceName, e);
            } finally {
                // 下次调度刷新时间，下次执行的时间与failCount有关
                // failCount=0，则下次调度时间为6秒，最长为1分钟
                // 即当无异常情况下缓存实例的刷新时间是6秒
                executor.schedule(this, Math.min(delayTime << failCount, DEFAULT_DELAY * 60), TimeUnit.MILLISECONDS);
            }
        }

        private void incFailCount() {
            int limit = 6;
            if (failCount == limit) {
                return;
            }
            failCount++;
        }

        private void resetFailCount() {
            failCount = 0;
        }
    }
}
