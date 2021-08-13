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

package com.alibaba.nacos.client.naming.cache;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.backups.FailoverReactor;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Naming client service information holder.
 *
 * @author xiweng.yy
 */
public class ServiceInfoHolder implements Closeable {

    private static final String JM_SNAPSHOT_PATH_PROPERTY = "JM.SNAPSHOT.PATH";

    private static final String FILE_PATH_NACOS = "nacos";

    private static final String FILE_PATH_NAMING = "naming";

    private static final String USER_HOME_PROPERTY = "user.home";

    private final ConcurrentMap<String, ServiceInfo> serviceInfoMap;

    private final FailoverReactor failoverReactor;

    private final boolean pushEmptyProtection;

    private String cacheDir;

    public ServiceInfoHolder(String namespace, Properties properties) {
        // 生成缓存目录：默认为${user.home}/nacos/naming/public，
        // 可以通过System.setProperty("JM.SNAPSHOT.PATH")自定义根目录
        initCacheDir(namespace, properties);
        // 启动时是否从缓存目录读取信息，默认false。设置为true会读取缓存文件
        if (isLoadCacheAtStart(properties)) {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(DiskCache.read(this.cacheDir));
        } else {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(16);
        }
        // 故障转移相关，故障转移目录：${user.home}/nacos/naming/public/failover
        // 故障转移开关文件：${user.home}/nacos/naming/public/failover/00-00---000-VIPSRV_FAILOVER_SWITCH-000---00-00
        // 故障转移关闭：当故障转移开关文件不存在时或者文件的值为0
        // 故障转移开启：当故障转移开关文件存在时或者文件的值为1
        // 故障转移检查：延迟5秒将缓存文件ServiceInfo信息读入缓存（由FailoverReactor#SwitchRefresher负责）
        // 当故障转移开关开启，更新缓存switchParams.put("failover-mode", "true")，
        // 同时启动FailoverFileReader线程读取目录failover文件ServiceInfo内容。
        // 例如：DEFAULT_GROUP%40%40nacos.test.3，这些信息被读入到内存Map<String, ServiceInfo> serviceMap中。
        // 故障数据备份：每10秒钟备份一次（FailoverReactor#DiskFileWriter），会把ServiceInfo即上面json内容备份到文件中。
        this.failoverReactor = new FailoverReactor(this, cacheDir);
        this.pushEmptyProtection = isPushEmptyProtect(properties);
    }

    private void initCacheDir(String namespace, Properties properties) {
        String jmSnapshotPath = System.getProperty(JM_SNAPSHOT_PATH_PROPERTY);

        String namingCacheRegistryDir = "";
        if (properties.getProperty(PropertyKeyConst.NAMING_CACHE_REGISTRY_DIR) != null) {
            namingCacheRegistryDir = File.separator + properties.getProperty(PropertyKeyConst.NAMING_CACHE_REGISTRY_DIR);
        }

        if (!StringUtils.isBlank(jmSnapshotPath)) {
            cacheDir = jmSnapshotPath + File.separator + FILE_PATH_NACOS + namingCacheRegistryDir
                    + File.separator + FILE_PATH_NAMING + File.separator + namespace;
        } else {
            cacheDir = System.getProperty(USER_HOME_PROPERTY) + File.separator + FILE_PATH_NACOS + namingCacheRegistryDir
                    + File.separator + FILE_PATH_NAMING + File.separator + namespace;
        }
    }

    private boolean isLoadCacheAtStart(Properties properties) {
        boolean loadCacheAtStart = false;
        if (properties != null && StringUtils
                .isNotEmpty(properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START))) {
            loadCacheAtStart = ConvertUtils
                    .toBoolean(properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START));
        }
        return loadCacheAtStart;
    }

    private boolean isPushEmptyProtect(Properties properties) {
        boolean pushEmptyProtection = false;
        if (properties != null && StringUtils
                .isNotEmpty(properties.getProperty(PropertyKeyConst.NAMING_PUSH_EMPTY_PROTECTION))) {
            pushEmptyProtection = ConvertUtils
                    .toBoolean(properties.getProperty(PropertyKeyConst.NAMING_PUSH_EMPTY_PROTECTION));
        }
        return pushEmptyProtection;
    }

    public Map<String, ServiceInfo> getServiceInfoMap() {
        return serviceInfoMap;
    }

    public ServiceInfo getServiceInfo(final String serviceName, final String groupName, final String clusters) {
        NAMING_LOGGER.debug("failover-mode: " + failoverReactor.isFailoverSwitch());
        String groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
        String key = ServiceInfo.getKey(groupedServiceName, clusters);
        if (failoverReactor.isFailoverSwitch()) {
            return failoverReactor.getService(key);
        }
        return serviceInfoMap.get(key);
    }

    /**
     * Process service json.
     *
     * @param json service json
     * @return service info
     */
    public ServiceInfo processServiceInfo(String json) {
        ServiceInfo serviceInfo = JacksonUtils.toObj(json, ServiceInfo.class);
        serviceInfo.setJsonFromServer(json);
        return processServiceInfo(serviceInfo);
    }

    /**
     * Process service info.
     *
     * 服务实例信息会被缓存在serviceInfoMap中，key为「goupName@@ServiceName」
     * 例如：DEFAULT_GROUP@@nacos.test.3；
     * serviceInfoMap的大小会通过prometheus simpleclient统计监控；
     * 如果服务信息有更新，会通过 NotifyCenter.publishEvent发布实例变更事件，订阅该服务的的订阅者Subscribes将会处理该事件；
     * 将缓存服务信息保存到本地文件容灾。
     *
     * @param serviceInfo new service info
     * @return service info
     */
    public ServiceInfo processServiceInfo(ServiceInfo serviceInfo) {
        String serviceKey = serviceInfo.getKey();
        if (serviceKey == null) {
            return null;
        }
        ServiceInfo oldService = serviceInfoMap.get(serviceInfo.getKey());
        if (isEmptyOrErrorPush(serviceInfo)) {
            //empty or error push, just ignore
            return oldService;
        }
        // 缓存服务信息
        serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);
        // 判断注册的实例信息是否已变更
        boolean changed = isChangedServiceInfo(oldService, serviceInfo);
        if (StringUtils.isBlank(serviceInfo.getJsonFromServer())) {
            serviceInfo.setJsonFromServer(JacksonUtils.toJson(serviceInfo));
        }
        // 通过prometheus-simpleclient监控服务缓存Map的大小
        MetricsMonitor.getServiceInfoMapSizeMonitor().set(serviceInfoMap.size());
        // 服务实例已变更
        if (changed) {
            NAMING_LOGGER.info("current ips:(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getKey() + " -> "
                    + JacksonUtils.toJson(serviceInfo.getHosts()));
            // 添加实例变更事件，会被推动到订阅者执行
            NotifyCenter.publishEvent(new InstancesChangeEvent(serviceInfo.getName(), serviceInfo.getGroupName(),
                    serviceInfo.getClusters(), serviceInfo.getHosts()));
            // 记录Service本地文件
            DiskCache.write(serviceInfo, cacheDir);
        }
        return serviceInfo;
    }

    private boolean isEmptyOrErrorPush(ServiceInfo serviceInfo) {
        return null == serviceInfo.getHosts() || (pushEmptyProtection && !serviceInfo.validate());
    }
    
    /**
     * 判断是否需要变更本地ServiceInfo
     */
    private boolean isChangedServiceInfo(ServiceInfo oldService, ServiceInfo newService) {
        // 如果旧数据为null，则需要变更
        if (null == oldService) {
            NAMING_LOGGER.info("init new ips(" + newService.ipCount() + ") service: " + newService.getKey() + " -> "
                    + JacksonUtils.toJson(newService.getHosts()));
            return true;
        }
        // 如果旧数据更新时间大于新数据更新时间，则打印警告日志
        if (oldService.getLastRefTime() > newService.getLastRefTime()) {
            NAMING_LOGGER
                    .warn("out of date data received, old-t: " + oldService.getLastRefTime() + ", new-t: " + newService
                            .getLastRefTime());
        }
        boolean changed = false;
        Map<String, Instance> oldHostMap = new HashMap<String, Instance>(oldService.getHosts().size());
        for (Instance host : oldService.getHosts()) {
            oldHostMap.put(host.toInetAddr(), host);
        }
        Map<String, Instance> newHostMap = new HashMap<String, Instance>(newService.getHosts().size());
        for (Instance host : newService.getHosts()) {
            newHostMap.put(host.toInetAddr(), host);
        }

        // 变更的实例集合
        Set<Instance> modHosts = new HashSet<Instance>();
        // 新增的实例集合
        Set<Instance> newHosts = new HashSet<Instance>();
        // 删除的实例集合
        Set<Instance> remvHosts = new HashSet<Instance>();

        List<Map.Entry<String, Instance>> newServiceHosts = new ArrayList<Map.Entry<String, Instance>>(
                newHostMap.entrySet());
        for (Map.Entry<String, Instance> entry : newServiceHosts) {
            Instance host = entry.getValue();
            String key = entry.getKey();
            if (oldHostMap.containsKey(key) && !StringUtils.equals(host.toString(), oldHostMap.get(key).toString())) {
                modHosts.add(host);
                continue;
            }

            if (!oldHostMap.containsKey(key)) {
                newHosts.add(host);
            }
        }

        for (Map.Entry<String, Instance> entry : oldHostMap.entrySet()) {
            Instance host = entry.getValue();
            String key = entry.getKey();
            if (newHostMap.containsKey(key)) {
                continue;
            }

            if (!newHostMap.containsKey(key)) {
                remvHosts.add(host);
            }

        }

        if (newHosts.size() > 0) {
            changed = true;
            NAMING_LOGGER
                    .info("new ips(" + newHosts.size() + ") service: " + newService.getKey() + " -> " + JacksonUtils
                            .toJson(newHosts));
        }

        if (remvHosts.size() > 0) {
            changed = true;
            NAMING_LOGGER.info("removed ips(" + remvHosts.size() + ") service: " + newService.getKey() + " -> "
                    + JacksonUtils.toJson(remvHosts));
        }

        if (modHosts.size() > 0) {
            changed = true;
            NAMING_LOGGER.info("modified ips(" + modHosts.size() + ") service: " + newService.getKey() + " -> "
                    + JacksonUtils.toJson(modHosts));
        }
        return changed;
    }

    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        failoverReactor.shutdown();
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
}
