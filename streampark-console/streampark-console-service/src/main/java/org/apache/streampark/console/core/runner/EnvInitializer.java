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

package org.apache.streampark.console.core.runner;

import org.apache.streampark.common.conf.CommonConfig;
import org.apache.streampark.common.conf.ConfigConst;
import org.apache.streampark.common.conf.InternalConfigHolder;
import org.apache.streampark.common.conf.InternalOption;
import org.apache.streampark.common.conf.Workspace;
import org.apache.streampark.common.enums.StorageType;
import org.apache.streampark.common.fs.FsOperator;
import org.apache.streampark.common.util.SystemPropertyUtils;
import org.apache.streampark.common.util.Utils;
import org.apache.streampark.console.base.util.WebUtils;
import org.apache.streampark.console.core.entity.FlinkEnv;
import org.apache.streampark.console.core.service.SettingService;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.streampark.common.enums.StorageType.LFS;

@Order
@Slf4j
@Component
public class EnvInitializer implements ApplicationRunner {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private SettingService settingService;

    private final Set<StorageType> initialized = new HashSet<>(2);

    private final FileFilter fileFilter = p -> !".gitkeep".equals(p.getName());

    private static final Pattern PATTERN_FLINK_SHIMS_JAR =
        Pattern.compile(
            "^streampark-flink-shims_flink-(1.1[2-7])_(2.11|2.12)-(.*).jar$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Optional<String> profile =
            Arrays.stream(context.getEnvironment().getActiveProfiles()).findFirst();
        if ("test".equals(profile.orElse(null))) {
            return;
        }

        String appHome = WebUtils.getAppHome();
        if (appHome == null) {
            throw new ExceptionInInitializerError(
                String.format(
                    "[StreamPark] System initialization check failed,"
                        + " The system initialization check failed. If started local for development and debugging,"
                        + " please ensure the -D%s parameter is clearly specified,"
                        + " more detail: https://streampark.apache.org/docs/user-guide/development",
                    ConfigConst.KEY_APP_HOME()));
        }

        // init InternalConfig
        initInternalConfig(context.getEnvironment());
        // overwrite system variable HADOOP_USER_NAME
        String hadoopUserName = InternalConfigHolder.get(CommonConfig.STREAMPARK_HADOOP_USER_NAME());
        overrideSystemProp(ConfigConst.KEY_HADOOP_USER_NAME(), hadoopUserName);
        // initialize local file system resources
        storageInitialize(LFS);
    }

    private void initInternalConfig(Environment springEnv) {
        // override config from spring application.yaml
        InternalConfigHolder.keys().stream()
            .filter(springEnv::containsProperty)
            .forEach(
                key -> {
                    InternalOption config = InternalConfigHolder.getConfig(key);
                    Utils.notNull(config);
                    InternalConfigHolder.set(config, springEnv.getProperty(key, config.classType()));
                });

        String mvnSettings = settingService.getMavenSettings();
        if (StringUtils.isNotEmpty(mvnSettings)) {
            InternalConfigHolder.set(CommonConfig.MAVEN_SETTINGS_PATH(), mvnSettings);
        }

        String mvnRepository = settingService.getMavenRepository();
        if (StringUtils.isNotEmpty(mvnRepository)) {
            InternalConfigHolder.set(CommonConfig.MAVEN_REMOTE_URL(), mvnRepository);
        }

        String mvnAuthUser = settingService.getMavenAuthUser();
        if (StringUtils.isNotEmpty(mvnAuthUser)) {
            InternalConfigHolder.set(CommonConfig.MAVEN_AUTH_USER(), mvnAuthUser);
        }

        String mvnAuthPassword = settingService.getMavenAuthPassword();
        if (StringUtils.isNotEmpty(mvnAuthPassword)) {
            InternalConfigHolder.set(CommonConfig.MAVEN_AUTH_PASSWORD(), mvnAuthPassword);
        }

        InternalConfigHolder.log();
    }

    private void overrideSystemProp(String key, String defaultValue) {
        String value = context.getEnvironment().getProperty(key, defaultValue);
        log.info("initialize system properties: key:{}, value:{}", key, value);
        SystemPropertyUtils.set(key, value);
    }

    public synchronized void storageInitialize(StorageType storageType) {

        if (initialized.contains(storageType)) {
            return;
        }

        final String mkdirLog = "storage initialize, now mkdir [{}] starting ...";

        FsOperator fsOperator = FsOperator.of(storageType);
        Workspace workspace = Workspace.of(storageType);

        // 1. prepare workspace dir
        // 获取本地目录 eg: /opt/app/streampark/streampark-2.1.0/workspace/dist
        if (storageType.equals(LFS)) {
            String localDist = Workspace.APP_LOCAL_DIST();
            if (!fsOperator.exists(localDist)) {
                log.info(mkdirLog, localDist);
                fsOperator.mkdirs(localDist);
            }
        }

        // 获取上传目录 /opt/app/streampark/streampark-2.1.0/workspace/uploads
        // 如果不存在 则创建
        String appUploads = workspace.APP_UPLOADS();
        if (!fsOperator.exists(appUploads)) {
            log.info(mkdirLog, appUploads);
            fsOperator.mkdirs(appUploads);
        }

        // 获取工作目录 /opt/app/streampark/streampark-2.1.0/workspace/workspace
        // 如果不存在 则创建
        String appWorkspace = workspace.APP_WORKSPACE();
        if (!fsOperator.exists(appWorkspace)) {
            log.info(mkdirLog, appWorkspace);
            fsOperator.mkdirs(appWorkspace);
        }

        // 获取 app backup 目录 /opt/app/streampark/streampark-2.1.0/workspace/backups
        // 如果不存在 则创建
        String appBackups = workspace.APP_BACKUPS();
        if (!fsOperator.exists(appBackups)) {
            log.info(mkdirLog, appBackups);
            fsOperator.mkdirs(appBackups);
        }

        // 获取 savepoint 目录 /opt/app/streampark/streampark-2.1.0/workspace/savepoints
        // 如果不存在 则创建
        String appSavePoints = workspace.APP_SAVEPOINTS();
        if (!fsOperator.exists(appSavePoints)) {
            log.info(mkdirLog, appSavePoints);
            fsOperator.mkdirs(appSavePoints);
        }

        // 获取 jars 目录 /opt/app/streampark/streampark-2.1.0/workspace/jars
        // 如果不存在 则创建
        String appJars = workspace.APP_JARS();
        if (!fsOperator.exists(appJars)) {
            log.info(mkdirLog, appJars);
            fsOperator.mkdirs(appJars);
        }

        // 2. upload jar.
        // 2.1) upload client jar
        // 获取文件目录 /opt/app/streampark/streampark-2.1.0/workspace/client
        // 一般这个 client 只有一个 streampark-flink-sql-client-jar streampark-flink-sqlclient_2.12-2.1.0.jar
        File client = WebUtils.getAppClientDir();
        Utils.required(
            client.exists() && client.listFiles().length > 0,
            client.getAbsolutePath().concat(" is not exists or empty directory "));

        // 获取 app-client 目录 /opt/app/streampark/streampark-2.1.0/workspace/client
        String appClient = workspace.APP_CLIENT();
        // 先删除 app-client 目录 然后再创建该目录
        fsOperator.mkCleanDirs(appClient);

        for (File file : client.listFiles(fileFilter)) {
            log.info("load client:{} to {}", file.getName(), appClient);
            // 将 streampark-flink-sqlclient_2.12-2.1.0.jar 上传到 app-client 目录
            fsOperator.upload(file.getAbsolutePath(), appClient);
        }

        // 2.2) upload plugin jar.
        // 创建 plugins 目录 /opt/app/streampark/streampark-2.1.0/workspace/plugins
        // 一般这个目录下只有一个文件 也即 Dockerfile
        String appPlugins = workspace.APP_PLUGINS();
        fsOperator.mkCleanDirs(appPlugins);

        File plugins = WebUtils.getAppPluginsDir();
        for (File file : plugins.listFiles(fileFilter)) {
            log.info("load plugin:{} to {}", file.getName(), appPlugins);
            // 上传 Dockerfile 到 plugins 目录上
            fsOperator.upload(file.getAbsolutePath(), appPlugins);
        }

        // 2.3) upload shims jar
        // 获取 shims 目录 /opt/app/streampark/streampark-2.1.0/lib
        // 并过滤出 streampark-flink-shims_flink*.jar
        File[] shims =
            WebUtils.getAppLibDir()
                .listFiles(pathname -> pathname.getName().matches(PATTERN_FLINK_SHIMS_JAR.pattern()));

        Utils.required(shims != null && shims.length > 0, "streampark-flink-shims jar not exist");

        String appShims = workspace.APP_SHIMS();
        fsOperator.delete(appShims);

        // 将 streampark-flink-shims_flink*.jar 上传到指定目录
        for (File file : shims) {
            Matcher matcher = PATTERN_FLINK_SHIMS_JAR.matcher(file.getName());
            if (matcher.matches()) {
                String version = matcher.group(1);
                String shimsPath = appShims.concat("/flink-").concat(version);
                fsOperator.mkdirs(shimsPath);
                log.info("load shims:{} to {}", file.getName(), shimsPath);
                fsOperator.upload(file.getAbsolutePath(), shimsPath);
            }
        }

        // 2.4) create maven local repository dir
        // todo 这个是不是写的有问题呀 ???
        String localMavenRepo = Workspace.MAVEN_LOCAL_PATH();
        if (FsOperator.lfs().exists(localMavenRepo)) {
            FsOperator.lfs().mkdirs(localMavenRepo);
        }

        initialized.add(storageType);
    }

    public void checkFlinkEnv(StorageType storageType, FlinkEnv flinkEnv) throws IOException {
        // 获取 flink home 比如 /opt/app/flink/flink-1.16.0
        String flinkLocalHome = flinkEnv.getFlinkHome();
        if (flinkLocalHome == null) {
            throw new ExceptionInInitializerError(
                "[StreamPark] FLINK_HOME is undefined,Make sure that Flink is installed.");
        }
        // 创建 Workspace
        Workspace workspace = Workspace.of(storageType);
        // 获取 app 目录
        /**
         * StorageType 可能是 HDFS 也可能是 LFS
         * application.yml 配置如下:
         * streampark:
         *   proxy:
         *     workspace:
         *       local: /opt/app/streampark/streampark-2.1.0/workspace
         *       remote:   # support hdfs:///streampark/ 、 /streampark 、hdfs://host:ip/streampark/
         *
         * 比如
         * LFS -> /opt/app/streampark/streampark-2.1.0/workspace/flink
         * HDFS -> hdfs:///streampark/flink
         */
        String appFlink = workspace.APP_FLINK();
        // 获取 FsOperator
        /**
         * LFS  -> LfsOperator
         * HDFS -> HdfsOperator
         */
        FsOperator fsOperator = FsOperator.of(storageType);
        if (!fsOperator.exists(appFlink)) {
            log.info("checkFlinkEnv, now mkdir [{}] starting ...", appFlink);
            fsOperator.mkdirs(appFlink);
        }
        // 获取 flink 目录名字
        File flinkLocalDir = new File(flinkLocalHome);
        if (Files.isSymbolicLink(flinkLocalDir.toPath())) {
            flinkLocalDir = flinkLocalDir.getCanonicalFile();
        }
        String flinkName = flinkLocalDir.getName();
        // 拼接路径
        // 比如 /opt/app/streampark/streampark-2.1.0/workspace/flink/flink-1.16.0
        String flinkHome = appFlink.concat("/").concat(flinkName);
        if (!fsOperator.exists(flinkHome)) {
            log.info("{} is not exists,upload beginning....", flinkHome);
            // 将 ${FLINK_HOME} 目录下的所有文件拷贝到 flinkHome 目录下
            fsOperator.upload(flinkLocalHome, flinkHome, false, true);
        }
    }
}
