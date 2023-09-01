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

package org.apache.streampark.console.core.controller;

import org.apache.streampark.common.util.Utils;
import org.apache.streampark.common.util.YarnUtils;
import org.apache.streampark.console.base.domain.ApiDocConstant;
import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.base.exception.InternalException;
import org.apache.streampark.console.core.annotation.ApiAccess;
import org.apache.streampark.console.core.annotation.AppUpdated;
import org.apache.streampark.console.core.annotation.PermissionAction;
import org.apache.streampark.console.core.bean.AppControl;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.ApplicationBackUp;
import org.apache.streampark.console.core.entity.ApplicationLog;
import org.apache.streampark.console.core.enums.AppExistsState;
import org.apache.streampark.console.core.enums.PermissionType;
import org.apache.streampark.console.core.service.AppBuildPipeService;
import org.apache.streampark.console.core.service.ApplicationBackUpService;
import org.apache.streampark.console.core.service.ApplicationLogService;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.flink.packer.pipeline.PipelineStatus;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import com.baomidou.mybatisplus.core.metadata.IPage;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Tag(name = "FLINK_APPLICATION_TAG")
@Slf4j
@Validated
@RestController
@RequestMapping("flink/app")
public class ApplicationController {

    @Autowired
    private ApplicationService applicationService;

    @Autowired
    private ApplicationBackUpService backUpService;

    @Autowired
    private ApplicationLogService applicationLogService;

    @Autowired
    private AppBuildPipeService appBuildPipeService;

    @Operation(summary = "Get application")
    @ApiAccess
    @PostMapping("get")
    @RequiresPermissions("app:detail")
    public RestResponse get(Application app) {
        Application application = applicationService.getApp(app);
        return RestResponse.success(application);
    }

    @Operation(summary = "Create application")
    @ApiAccess
    @PermissionAction(id = "#app.teamId", type = PermissionType.TEAM)
    @PostMapping("create")
    @RequiresPermissions("app:create")
    public RestResponse create(Application app) throws IOException {
        /**
         * 创建一个 Application
         * jobType: 2
         * flinkSql: CREATE TABLE datagen (
         *    f_sequence INT,
         *    f_random INT,
         *    f_random_str STRING,
         *    ts AS localtimestamp,
         *    WATERMARK FOR ts AS ts
         *    ) WITH (
         *     'connector' = 'datagen',
         *     'rows-per-second' = '5',
         *     'fields.f_sequence.kind' = 'sequence',
         *     'fields.f_sequence.start' = '1',
         *     'fields.f_sequence.end' = '1000',
         *     'fields.f_random.min' = '1',
         *     'fields.f_random.max' = '1000',
         *     'fields.f_random_str.length' = '10'
         *    );
         *    CREATE TABLE sink_minio (f_sequence INT, f_random INT, f_random_str STRING) WITH (
         *     'connector' = 'filesystem',
         *     'path' = 's3a://flink/data',
         *     'sink.rolling-policy.file-size' = '1MB',
         *     'sink.rolling-policy.rollover-interval' = '2 min',
         *     'sink.rolling-policy.check-interval' = '1 min',
         *     'format' = 'json'
         *    );
         *    insert into
         *      sink_minio
         *    select
         *      f_sequence,
         *      f_random,
         *      f_random_str
         *    from
         *      datagen;
         * appType: 1
         * executionMode: 6
         * versionId: 100000
         * jobName: flink-test-debug
         * options: {}
         * resolveOrder: 0
         * k8sRestExposedType: 2
         * k8sNamespace: flink-dev
         * clusterId: flink-test
         * flinkImage: registry.cn-guangzhou.aliyuncs.com/tanbingshi/flink:flink-1.16.0-s3_v2
         * k8sPodTemplate: apiVersion: v1
         *    kind: Pod
         *    metadata:
         *      name: streampark-pod
         *    spec:
         *      serviceAccount: default
         *      imagePullSecrets:
         *       - name: streamparksecret
         * k8sJmPodTemplate:
         * k8sTmPodTemplate:
         * k8sHadoopIntegration: false
         * socketId: f1596f8dd71346e194bafc817c31fda3
         * teamId: 100000
         */
        /**
         * *************************** 4. row ***************************
         *                       id: 100004
         *                  team_id: 100000
         *                 job_type: 2
         *           execution_mode: 6
         *            resource_from: NULL
         *               project_id: NULL
         *                 job_name: flink-test-debug
         *                   module: NULL
         *                      jar: NULL
         *            jar_check_sum: NULL
         *               main_class: NULL
         *                     args: NULL
         *                  options: {}
         *               hot_params: NULL
         *                  user_id: 100000
         *                   app_id: NULL
         *                 app_type: 1
         *                 duration: NULL
         *                   job_id: NULL
         *          job_manager_url: NULL
         *               version_id: 100000
         *               cluster_id: flink-test
         *            k8s_namespace: flink-dev
         *              flink_image: registry.cn-guangzhou.aliyuncs.com/tanbingshi/flink:flink-1.16.0-s3_v2
         *                    state: 0
         *             restart_size: NULL
         *            restart_count: NULL
         *             cp_threshold: NULL
         *  cp_max_failure_interval: NULL
         * cp_failure_rate_interval: NULL
         *        cp_failure_action: NULL
         *       dynamic_properties: NULL
         *              description: NULL
         *            resolve_order: 0
         *    k8s_rest_exposed_type: 2
         *                jm_memory: NULL
         *                tm_memory: NULL
         *               total_task: NULL
         *                 total_tm: NULL
         *               total_slot: NULL
         *           available_slot: NULL
         *             option_state: 0
         *                 tracking: 0
         *              create_time: 2023-09-01 09:36:01
         *              modify_time: 2023-09-01 09:36:01
         *              option_time: NULL
         *                  release: 1
         *                    build: 1
         *               start_time: NULL
         *                 end_time: NULL
         *                 alert_id: NULL
         *         k8s_pod_template: apiVersion: v1
         * kind: Pod
         * metadata:
         *   name: streampark-pod
         * spec:
         *   serviceAccount: default
         *   imagePullSecrets:
         *   - name: streamparksecret
         *      k8s_jm_pod_template:
         *      k8s_tm_pod_template:
         *   k8s_hadoop_integration: 0
         *         flink_cluster_id: NULL
         *         ingress_template: NULL
         *     default_mode_ingress: NULL
         *                     tags: NULL
         */
        /**
         * 涉及到 t_flink_app t_flink_sql 两种表
         */
        boolean saved = applicationService.create(app);
        return RestResponse.success(saved);
    }

    @Operation(
        summary = "Copy application",
        tags = {ApiDocConstant.FLINK_APP_OP_TAG})
    @Parameters({
        @Parameter(
            name = "id",
            description = "copied target app id",
            in = ParameterIn.QUERY,
            required = true,
            example = "100000"),
        @Parameter(
            name = "jobName",
            description = "new application name",
            in = ParameterIn.QUERY,
            example = "copy-app"),
        @Parameter(name = "args", description = "new application args", in = ParameterIn.QUERY)
    })
    @ApiAccess
    @PermissionAction(id = "#app.id", type = PermissionType.APP)
    @PostMapping(value = "copy")
    @RequiresPermissions("app:copy")
    public RestResponse copy(@Parameter(hidden = true) Application app) throws IOException {
        Long id = applicationService.copy(app);
        Map<String, String> data = new HashMap<>();
        data.put("id", Long.toString(id));
        return id.equals(0L)
            ? RestResponse.success(false).data(data)
            : RestResponse.success(true).data(data);
    }

    @Operation(summary = "Update application")
    @AppUpdated
    @PermissionAction(id = "#app.id", type = PermissionType.APP)
    @PostMapping("update")
    @RequiresPermissions("app:update")
    public RestResponse update(Application app) {
        applicationService.update(app);
        return RestResponse.success(true);
    }

    @Operation(summary = "Get applications dashboard data")
    @PostMapping("dashboard")
    public RestResponse dashboard(Long teamId) {
        Map<String, Serializable> map = applicationService.dashboard(teamId);
        return RestResponse.success(map);
    }

    @Operation(summary = "List applications")
    @ApiAccess
    @PostMapping("list")
    @RequiresPermissions("app:view")
    public RestResponse list(Application app, RestRequest request) {
        IPage<Application> applicationList = applicationService.page(app, request);
        List<Application> appRecords = applicationList.getRecords();
        List<Long> appIds = appRecords.stream().map(Application::getId).collect(Collectors.toList());
        Map<Long, PipelineStatus> pipeStates = appBuildPipeService.listPipelineStatus(appIds);

        // add building pipeline status info and app control info
        appRecords =
            appRecords.stream()
                .peek(
                    e -> {
                        if (pipeStates.containsKey(e.getId())) {
                            e.setBuildStatus(pipeStates.get(e.getId()).getCode());
                        }
                    })
                .peek(
                    e -> {
                        AppControl appControl =
                            new AppControl()
                                .setAllowBuild(
                                    e.getBuildStatus() == null
                                        || !PipelineStatus.running.getCode().equals(e.getBuildStatus()))
                                .setAllowStart(
                                    !e.shouldBeTrack()
                                        && PipelineStatus.success.getCode().equals(e.getBuildStatus()))
                                .setAllowStop(e.isRunning());
                        e.setAppControl(appControl);
                    })
                .collect(Collectors.toList());
        applicationList.setRecords(appRecords);
        return RestResponse.success(applicationList);
    }

    @Operation(summary = "Mapping application")
    @AppUpdated
    @PostMapping("mapping")
    @RequiresPermissions("app:mapping")
    public RestResponse mapping(Application app) {
        boolean flag = applicationService.mapping(app);
        return RestResponse.success(flag);
    }

    @Operation(summary = "Revoke application")
    @AppUpdated
    @PermissionAction(id = "#app.id", type = PermissionType.APP)
    @PostMapping("revoke")
    @RequiresPermissions("app:release")
    public RestResponse revoke(Application app) {
        applicationService.revoke(app);
        return RestResponse.success();
    }

    @Operation(
        summary = "Start application",
        tags = {ApiDocConstant.FLINK_APP_OP_TAG})
    @Parameters({
        @Parameter(
            name = "id",
            description = "start app id",
            in = ParameterIn.QUERY,
            required = true,
            example = "100000",
            schema = @Schema(implementation = Long.class)),
        @Parameter(
            name = "savePointed",
            description = "restored app from the savepoint or latest checkpoint",
            in = ParameterIn.QUERY,
            required = true,
            example = "false",
            schema = @Schema(implementation = boolean.class, defaultValue = "false")),
        @Parameter(
            name = "savePoint",
            description = "savepoint or checkpoint path",
            in = ParameterIn.QUERY,
            schema = @Schema(implementation = String.class)),
        @Parameter(
            name = "allowNonRestored",
            description = "ignore savepoint if cannot be restored",
            in = ParameterIn.QUERY,
            required = false,
            schema = @Schema(implementation = boolean.class, defaultValue = "false"))
    })
    @ApiAccess
    @PermissionAction(id = "#app.id", type = PermissionType.APP)
    @PostMapping(value = "start")
    @RequiresPermissions("app:start")
    public RestResponse start(@Parameter(hidden = true) Application app) {
        /**
         * {
         * id: 100004
         * savePointed: false
         * savePoint:
         * allowNonRestored: false
         * teamId: 100000
         * }
         */
        try {
            applicationService.checkEnv(app);
            // 启动 application
            applicationService.start(app, false);
            return RestResponse.success(true);
        } catch (Exception e) {
            return RestResponse.success(false).message(e.getMessage());
        }
    }

    @Operation(
        summary = "Cancel application",
        tags = {ApiDocConstant.FLINK_APP_OP_TAG})
    @ApiAccess
    @Parameters({
        @Parameter(
            name = "id",
            description = "cancel app id",
            in = ParameterIn.QUERY,
            required = true,
            example = "100000",
            schema = @Schema(implementation = Long.class)),
        @Parameter(
            name = "savePointed",
            description = "trigger savepoint before taking stopping",
            in = ParameterIn.QUERY,
            required = true,
            schema = @Schema(implementation = boolean.class, defaultValue = "false")),
        @Parameter(
            name = "savePoint",
            description = "savepoint path",
            in = ParameterIn.QUERY,
            example = "hdfs:///savepoint/100000",
            schema = @Schema(implementation = String.class)),
        @Parameter(
            name = "drain",
            description = "send max watermark before canceling",
            in = ParameterIn.QUERY,
            required = true,
            example = "false",
            schema = @Schema(implementation = boolean.class, defaultValue = "false"))
    })
    @PermissionAction(id = "#app.id", type = PermissionType.APP)
    @PostMapping(value = "cancel")
    @RequiresPermissions("app:cancel")
    public RestResponse cancel(@Parameter(hidden = true) Application app) throws Exception {
        applicationService.cancel(app);
        return RestResponse.success();
    }

    @Operation(summary = "Clean application")
    @AppUpdated
    @ApiAccess
    @PermissionAction(id = "#app.id", type = PermissionType.APP)
    @PostMapping("clean")
    @RequiresPermissions("app:clean")
    public RestResponse clean(Application app) {
        applicationService.clean(app);
        return RestResponse.success(true);
    }

    /**
     * force stop(stop normal start or in progress)
     */
    @Operation(summary = "Force stop application")
    @PermissionAction(id = "#app.id", type = PermissionType.APP)
    @PostMapping("forcedStop")
    @RequiresPermissions("app:cancel")
    public RestResponse forcedStop(Application app) {
        applicationService.forcedStop(app);
        return RestResponse.success();
    }

    @Operation(summary = "Get application on yarn proxy address")
    @PostMapping("yarn")
    public RestResponse yarn() {
        return RestResponse.success(YarnUtils.getRMWebAppProxyURL());
    }

    @Operation(summary = "Get application on yarn name")
    @PostMapping("name")
    public RestResponse yarnName(Application app) {
        String yarnName = applicationService.getYarnName(app);
        return RestResponse.success(yarnName);
    }

    @Operation(summary = "Check the application exist status")
    @PostMapping("checkName")
    public RestResponse checkName(Application app) {
        AppExistsState exists = applicationService.checkExists(app);
        return RestResponse.success(exists.get());
    }

    @Operation(summary = "Get application conf")
    @PostMapping("readConf")
    public RestResponse readConf(Application app) throws IOException {
        String config = applicationService.readConf(app);
        return RestResponse.success(config);
    }

    @Operation(summary = "Get application main-class")
    @PostMapping("main")
    public RestResponse getMain(Application application) {
        String mainClass = applicationService.getMain(application);
        return RestResponse.success(mainClass);
    }

    @Operation(summary = "List application backups")
    @PostMapping("backups")
    public RestResponse backups(ApplicationBackUp backUp, RestRequest request) {
        IPage<ApplicationBackUp> backups = backUpService.page(backUp, request);
        return RestResponse.success(backups);
    }

    @Operation(summary = "List application operation logs")
    @PostMapping("optionlog")
    public RestResponse optionlog(ApplicationLog applicationLog, RestRequest request) {
        IPage<ApplicationLog> applicationList = applicationLogService.page(applicationLog, request);
        return RestResponse.success(applicationList);
    }

    @Operation(summary = "Delete application operation log")
    @PermissionAction(id = "#applicationLog.appId", type = PermissionType.APP)
    @PostMapping("deleteOperationLog")
    @RequiresPermissions("app:delete")
    public RestResponse deleteOperationLog(ApplicationLog applicationLog) {
        Boolean deleted = applicationLogService.delete(applicationLog);
        return RestResponse.success(deleted);
    }

    @Operation(summary = "Delete application")
    @PermissionAction(id = "#app.id", type = PermissionType.APP)
    @PostMapping("delete")
    @RequiresPermissions("app:delete")
    public RestResponse delete(Application app) throws InternalException {
        Boolean deleted = applicationService.delete(app);
        return RestResponse.success(deleted);
    }

    @Operation(summary = "Backup application when deleted")
    @PermissionAction(id = "#backUp.appId", type = PermissionType.APP)
    @PostMapping("deletebak")
    public RestResponse deleteBak(ApplicationBackUp backUp) throws InternalException {
        Boolean deleted = backUpService.delete(backUp.getId());
        return RestResponse.success(deleted);
    }

    @Operation(summary = "Check the application jar")
    @PostMapping("checkjar")
    public RestResponse checkjar(String jar) {
        File file = new File(jar);
        try {
            Utils.checkJarFile(file.toURI().toURL());
            return RestResponse.success(true);
        } catch (IOException e) {
            return RestResponse.success(file).message(e.getLocalizedMessage());
        }
    }

    @Operation(summary = "Upload the application jar")
    @PostMapping("upload")
    @RequiresPermissions("app:create")
    public RestResponse upload(MultipartFile file) throws Exception {
        String uploadPath = applicationService.upload(file);
        return RestResponse.success(uploadPath);
    }

    @Hidden
    @PostMapping("verifySchema")
    public RestResponse verifySchema(String path) {
        final URI uri = URI.create(path);
        final String scheme = uri.getScheme();
        final String pathPart = uri.getPath();
        RestResponse restResponse = RestResponse.success(true);
        String error = null;
        if (scheme == null) {
            error =
                "The scheme (hdfs://, file://, etc) is null. Please specify the file system scheme explicitly in the URI.";
        } else if (pathPart == null) {
            error =
                "The path to store the checkpoint data in is null. Please specify a directory path for the checkpoint data.";
        } else if (pathPart.length() == 0 || "/".equals(pathPart)) {
            error = "Cannot use the root directory for checkpoints.";
        }
        if (error != null) {
            restResponse = RestResponse.success(false).message(error);
        }
        return restResponse;
    }

    @Operation(summary = "Check the application savepoint path")
    @PostMapping("checkSavepointPath")
    public RestResponse checkSavepointPath(Application app) throws Exception {
        String error = applicationService.checkSavepointPath(app);
        if (error == null) {
            return RestResponse.success(true);
        } else {
            return RestResponse.success(false).message(error);
        }
    }

    @Operation(summary = "Get application on k8s deploy logs")
    @Parameters({
        @Parameter(
            name = "id",
            description = "app id",
            required = true,
            example = "100000",
            schema = @Schema(implementation = Long.class)),
        @Parameter(
            name = "offset",
            description = "number of log lines offset",
            required = true,
            example = "0",
            schema = @Schema(implementation = int.class)),
        @Parameter(
            name = "limit",
            description = "number of log lines loaded at once",
            required = true,
            example = "100",
            schema = @Schema(implementation = int.class)),
    })
    @PostMapping(value = "k8sStartLog")
    public RestResponse k8sStartLog(Long id, Integer offset, Integer limit) throws Exception {
        String resp = applicationService.k8sStartLog(id, offset, limit);
        return RestResponse.success(resp);
    }
}
