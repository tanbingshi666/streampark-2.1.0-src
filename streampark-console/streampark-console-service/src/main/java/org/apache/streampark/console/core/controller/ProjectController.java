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

import org.apache.streampark.console.base.domain.RestRequest;
import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.annotation.AppUpdated;
import org.apache.streampark.console.core.entity.Project;
import org.apache.streampark.console.core.enums.GitAuthorizedError;
import org.apache.streampark.console.core.service.ProjectService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

import com.baomidou.mybatisplus.core.metadata.IPage;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Tag(name = "PROJECT_TAG")
@Slf4j
@Validated
@RestController
@RequestMapping("flink/project")
public class ProjectController {

    @Autowired
    private ProjectService projectService;

    @Operation(summary = "Create project")
    @PostMapping("create")
    @RequiresPermissions("project:create")
    public RestResponse create(Project project) {
        /**
         * 一般请求参数如下
         * {
         *  name: streampark-example
         *  gitCredential: 1
         *  url: https://gitee.com/tanbingshi666/flink-1.16.0-code.git
         *  repository: 1
         *  type: 1
         *  branches: master
         *  userName: tanbingshi666@163.com
         *  password: 128505tan
         *  prvkeyPath:
         *  pom: flink-minio/pom.xml
         *  description: 测试
         *  teamId: 100000
         * }
         */
        ApiAlertException.throwIfNull(
            project.getTeamId(), "The teamId can't be null. Create team failed.");
        return projectService.create(project);
    }

    @Operation(summary = "Update project")
    @AppUpdated
    @PostMapping("update")
    @RequiresPermissions("project:update")
    public RestResponse update(Project project) {
        boolean update = projectService.update(project);
        return RestResponse.success().data(update);
    }

    @Operation(summary = "Get project")
    @PostMapping("get")
    public RestResponse get(Long id) {
        return RestResponse.success().data(projectService.getById(id));
    }

    @Operation(summary = "Build project")
    @PostMapping("build")
    @RequiresPermissions("project:build")
    public RestResponse build(Long id) throws Exception {
        /**
         * 一般请求参数
         * {
         * id: 100002
         * socketId: 5061933a65924fc0b12d84f17d3252c5
         * teamId: 100000
         * }
         */
        projectService.build(id);
        return RestResponse.success();
    }

    @Operation(summary = "Get project build logs")
    @PostMapping("buildlog")
    @RequiresPermissions("project:build")
    public RestResponse buildLog(
        Long id, @RequestParam(value = "startOffset", required = false) Long startOffset) {
        return projectService.getBuildLog(id, startOffset);
    }

    @Operation(summary = "List projects")
    @PostMapping("list")
    @RequiresPermissions("project:view")
    public RestResponse list(Project project, RestRequest restRequest) {
        /**
         * 一般请求参数如下：
         * {
         *  pageNum: 1,
         *  pageSize: 10,
         *  teamId: 100000
         * }
         */
        if (project.getTeamId() == null) {
            return RestResponse.success(Collections.emptyList());
        }
        /**
         * 分页查询 t_flink_project 表
         */
        /**
         * mysql> select * from t_flink_project \G;
         * *************************** 1. row ***************************
         *             id: 100001
         *        team_id: 100000
         *           name: flink-on-k8s-datastream
         * git_credential: 1
         *            url: https://gitee.com/tanbingshi666/flink-1.16.0-code.git
         *       branches: master
         *      user_name: tanbingshi666@163.com
         *       password: 128505tan
         *    prvkey_path:
         *            pom: flink-minio/pom.xml
         *     build_args: NULL
         *           type: 1
         *     repository: 1
         *     last_build: 2023-08-22 11:30:36
         *    description: NULL
         *    build_state: 1
         *    create_time: 2023-08-22 11:02:40
         *    modify_time: 2023-08-22 11:30:36
         */
        IPage<Project> page = projectService.page(project, restRequest);
        return RestResponse.success().data(page);
    }

    @Operation(summary = "List git project branches")
    @PostMapping("branches")
    public RestResponse branches(Project project) {
        /**
         * 一般请求参数如下
         * {
         * gitCredential: 1,
         * url: https://gitee.com/tanbingshi666/flink-1.16.0-code.git,
         * userName: tanbingshi666@163.com,
         * password: 128505tan,
         * prvkeyPath: ,
         * teamId: 100000
         * }
         */
        List<String> branches = project.getAllBranches();
        return RestResponse.success().data(branches);
    }

    @Operation(summary = "Delete project")
    @PostMapping("delete")
    @RequiresPermissions("project:delete")
    public RestResponse delete(Long id) {
        Boolean deleted = projectService.delete(id);
        return RestResponse.success().data(deleted);
    }

    @Operation(summary = "Authenticate git project")
    @PostMapping("gitcheck")
    public RestResponse gitCheck(Project project) {
        /**
         * 一般请求参数如下
         * {
         *  url: https://gitee.com/tanbingshi666/flink-1.16.0-code.git
         *  branches: master
         *  gitCredential: 1
         *  userName: tanbingshi666@163.com
         *  password: 128505tan
         *  prvkeyPath:
         *  teamId: 100000
         * }
         */
        GitAuthorizedError error = project.gitCheck();
        return RestResponse.success().data(error.getType());
    }

    @Operation(summary = "Check the project")
    @PostMapping("exists")
    public RestResponse exists(Project project) {
        boolean exists = projectService.checkExists(project);
        return RestResponse.success().data(exists);
    }

    @Operation(summary = "List project modules")
    @PostMapping("modules")
    public RestResponse modules(Long id) {
        List<String> result = projectService.modules(id);
        return RestResponse.success().data(result);
    }

    @Operation(summary = "List project jars")
    @PostMapping("jars")
    public RestResponse jars(Project project) {
        List<String> result = projectService.jars(project);
        return RestResponse.success().data(result);
    }

    @Operation(summary = "List project configurations")
    @PostMapping("listconf")
    public RestResponse listConf(Project project) {
        List<Map<String, Object>> list = projectService.listConf(project);
        return RestResponse.success().data(list);
    }

    @Operation(summary = "List the team projects")
    @PostMapping("select")
    public RestResponse select(@RequestParam Long teamId) {
        List<Project> list = projectService.findByTeamId(teamId);
        return RestResponse.success().data(list);
    }
}
