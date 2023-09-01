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

package org.apache.streampark.console.core.service.impl;

import org.apache.streampark.common.conf.CommonConfig;
import org.apache.streampark.common.conf.InternalConfigHolder;
import org.apache.streampark.console.core.bean.SenderEmail;
import org.apache.streampark.console.core.entity.Setting;
import org.apache.streampark.console.core.mapper.SettingMapper;
import org.apache.streampark.console.core.service.SettingService;

import org.apache.commons.lang3.StringUtils;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@Transactional(propagation = Propagation.SUPPORTS, readOnly = true, rollbackFor = Exception.class)
public class SettingServiceImpl extends ServiceImpl<SettingMapper, Setting>
    implements SettingService, ApplicationListener<ContextRefreshedEvent> {

    @Override
    public Setting get(String key) {
        /**
         * 根据 setting_key 查询 t_setting 表记录
         */
        LambdaQueryWrapper<Setting> queryWrapper =
            new LambdaQueryWrapper<Setting>().eq(Setting::getSettingKey, key);
        return this.getOne(queryWrapper);
    }

    private final Setting emptySetting = new Setting();

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        List<Setting> settingList = super.list();
        /**
         * 程序启动全量查询 t_setting 表
         */
        /**
         * mysql> select * from t_setting order by order_num asc \G;
         * *************************** 1. row ***************************
         *     order_num: 0
         *   setting_key: alert.email.password
         * setting_value: NULL
         *  setting_name: Alert Email Password
         *   description: 用来发送告警邮箱的认证密码
         *          type: 1
         * *************************** 2. row ***************************
         *     order_num: 1
         *   setting_key: streampark.maven.settings
         * setting_value: /opt/app/maven/maven-3.8.5/conf/settings.xml
         *  setting_name: Maven Settings File Path
         *   description: Maven Settings.xml 完整路径
         *          type: 1
         * *************************** 3. row ***************************
         *     order_num: 2
         *   setting_key: streampark.maven.central.repository
         * setting_value: NULL
         *  setting_name: Maven Central Repository
         *   description: Maven 私服地址
         *          type: 1
         * *************************** 4. row ***************************
         *     order_num: 3
         *   setting_key: streampark.maven.auth.user
         * setting_value: NULL
         *  setting_name: Maven Central Repository Auth User
         *   description: Maven 私服认证用户名
         *          type: 1
         * *************************** 5. row ***************************
         *     order_num: 4
         *   setting_key: streampark.maven.auth.password
         * setting_value: NULL
         *  setting_name: Maven Central Repository Auth Password
         *   description: Maven 私服认证密码
         *          type: 1
         * *************************** 6. row ***************************
         *     order_num: 5
         *   setting_key: alert.email.host
         * setting_value: NULL
         *  setting_name: Alert Email Smtp Host
         *   description: 告警邮箱Smtp Host
         *          type: 1
         * *************************** 7. row ***************************
         *     order_num: 6
         *   setting_key: alert.email.port
         * setting_value: NULL
         *  setting_name: Alert Email Smtp Port
         *   description: 告警邮箱的Smtp Port
         *          type: 1
         * *************************** 8. row ***************************
         *     order_num: 7
         *   setting_key: alert.email.from
         * setting_value: NULL
         *  setting_name: Alert  Email From
         *   description: 发送告警的邮箱
         *          type: 1
         * *************************** 9. row ***************************
         *     order_num: 8
         *   setting_key: alert.email.userName
         * setting_value: NULL
         *  setting_name: Alert  Email User
         *   description: 用来发送告警邮箱的认证用户名
         *          type: 1
         * *************************** 10. row ***************************
         *     order_num: 10
         *   setting_key: alert.email.ssl
         * setting_value: false
         *  setting_name: Alert Email Is SSL
         *   description: 发送告警的邮箱是否开启SSL
         *          type: 2
         * *************************** 11. row ***************************
         *     order_num: 11
         *   setting_key: docker.register.address
         * setting_value: registry.cn-guangzhou.aliyuncs.com
         *  setting_name: Docker Register Address
         *   description: Docker容器服务地址
         *          type: 1
         * *************************** 12. row ***************************
         *     order_num: 12
         *   setting_key: docker.register.user
         * setting_value: tanbingshi
         *  setting_name: Docker Register User
         *   description: Docker容器服务认证用户名
         *          type: 1
         * *************************** 13. row ***************************
         *     order_num: 13
         *   setting_key: docker.register.password
         * setting_value: 128505tan
         *  setting_name: Docker Register Password
         *   description: Docker容器服务认证密码
         *          type: 1
         * *************************** 14. row ***************************
         *     order_num: 14
         *   setting_key: docker.register.namespace
         * setting_value: tanbingshi
         *  setting_name: Namespace for docker image used in docker building env and target image register
         *   description: Docker命名空间
         *          type: 1
         * *************************** 15. row ***************************
         *     order_num: 15
         *   setting_key: ingress.mode.default
         * setting_value: NULL
         *  setting_name: Automatically generate an nginx-based ingress by passing in a domain name
         *   description: Ingress域名地址
         *          type: 1
         * 15 rows in set (0.00 sec)
         */
        settingList.forEach(x -> SETTINGS.put(x.getSettingKey(), x));
    }

    /**
     * 更新 System Setting 界面的内容
     */
    @Override
    public boolean update(Setting setting) {
        try {
            /**
             * 比如更新 maven settings 文件绝对路径
             * {
             * settingKey: streampark.maven.settings,
             * settingValue: /opt/app/maven/maven-3.8.5/conf/settings.xml,
             * teamId: 10000
             * }
             */
            String value = StringUtils.trimToNull(setting.getSettingValue());
            setting.setSettingValue(value);

            Setting entity = new Setting();
            entity.setSettingValue(setting.getSettingValue());
            /**
             * 根据 setting_key 更新 setting_value
             */
            LambdaQueryWrapper<Setting> queryWrapper =
                new LambdaQueryWrapper<Setting>().eq(Setting::getSettingKey, setting.getSettingKey());
            this.update(entity, queryWrapper);

            /**
             * 将
             * streampark.maven.settings
             * streampark.maven.central.repository
             * streampark.maven.auth.user
             * streampark.maven.auth.password
             * 缓存在内存以及设置在系统环境变量
             */
            String settingKey = setting.getSettingKey();
            if (CommonConfig.MAVEN_SETTINGS_PATH().key().equals(settingKey)) {
                InternalConfigHolder.set(CommonConfig.MAVEN_SETTINGS_PATH(), value);
            }

            if (CommonConfig.MAVEN_REMOTE_URL().key().equals(settingKey)) {
                InternalConfigHolder.set(CommonConfig.MAVEN_REMOTE_URL(), value);
            }

            if (CommonConfig.MAVEN_AUTH_USER().key().equals(settingKey)) {
                InternalConfigHolder.set(CommonConfig.MAVEN_AUTH_USER(), value);
            }
            if (CommonConfig.MAVEN_AUTH_PASSWORD().key().equals(settingKey)) {
                InternalConfigHolder.set(CommonConfig.MAVEN_AUTH_PASSWORD(), value);
            }

            /**
             * 更新缓存
             */
            Optional<Setting> optional = Optional.ofNullable(SETTINGS.get(setting.getSettingKey()));
            optional.ifPresent(x -> x.setSettingValue(value));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public SenderEmail getSenderEmail() {
        try {
            String host = SETTINGS.get(SettingService.KEY_ALERT_EMAIL_HOST).getSettingValue();
            String port = SETTINGS.get(SettingService.KEY_ALERT_EMAIL_PORT).getSettingValue();
            String from = SETTINGS.get(SettingService.KEY_ALERT_EMAIL_FROM).getSettingValue();
            String userName = SETTINGS.get(SettingService.KEY_ALERT_EMAIL_USERNAME).getSettingValue();
            String password = SETTINGS.get(SettingService.KEY_ALERT_EMAIL_PASSWORD).getSettingValue();
            String ssl = SETTINGS.get(SettingService.KEY_ALERT_EMAIL_SSL).getSettingValue();

            SenderEmail senderEmail = new SenderEmail();
            senderEmail.setSmtpHost(host);
            senderEmail.setSmtpPort(Integer.parseInt(port));
            senderEmail.setFrom(from);
            senderEmail.setUserName(userName);
            senderEmail.setPassword(password);
            senderEmail.setSsl(Boolean.parseBoolean(ssl));
            return senderEmail;
        } catch (Exception e) {
            log.warn("Fault Alert Email is not set.");
        }
        return null;
    }

    @Override
    public String getDockerRegisterAddress() {
        return SETTINGS
            .getOrDefault(SettingService.KEY_DOCKER_REGISTER_ADDRESS, emptySetting)
            .getSettingValue();
    }

    @Override
    public String getDockerRegisterUser() {
        return SETTINGS
            .getOrDefault(SettingService.KEY_DOCKER_REGISTER_USER, emptySetting)
            .getSettingValue();
    }

    @Override
    public String getDockerRegisterPassword() {
        return SETTINGS
            .getOrDefault(SettingService.KEY_DOCKER_REGISTER_PASSWORD, emptySetting)
            .getSettingValue();
    }

    @Override
    public String getDockerRegisterNamespace() {
        return SETTINGS
            .getOrDefault(SettingService.KEY_DOCKER_REGISTER_NAMESPACE, emptySetting)
            .getSettingValue();
    }

    @Override
    public String getStreamParkAddress() {
        return SETTINGS
            .getOrDefault(SettingService.KEY_STREAMPARK_ADDRESS, emptySetting)
            .getSettingValue();
    }

    @Override
    public String getMavenSettings() {
        return SETTINGS.getOrDefault(SettingService.KEY_MAVEN_SETTINGS, emptySetting).getSettingValue();
    }

    @Override
    public String getMavenRepository() {
        return SETTINGS
            .getOrDefault(SettingService.KEY_MAVEN_REPOSITORY, emptySetting)
            .getSettingValue();
    }

    @Override
    public String getMavenAuthUser() {
        return SETTINGS
            .getOrDefault(SettingService.KEY_MAVEN_AUTH_USER, emptySetting)
            .getSettingValue();
    }

    @Override
    public String getMavenAuthPassword() {
        return SETTINGS
            .getOrDefault(SettingService.KEY_MAVEN_AUTH_PASSWORD, emptySetting)
            .getSettingValue();
    }

    @Override
    public String getIngressModeDefault() {
        return SETTINGS
            .getOrDefault(SettingService.KEY_INGRESS_MODE_DEFAULT, emptySetting)
            .getSettingValue();
    }
}
