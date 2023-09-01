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

package org.apache.streampark.flink.packer.pipeline.impl

import org.apache.streampark.common.conf.Workspace
import org.apache.streampark.common.enums.DevelopmentMode
import org.apache.streampark.common.fs.{FsOperator, HdfsOperator, LfsOperator}
import org.apache.streampark.common.util.Utils
import org.apache.streampark.flink.packer.maven.MavenTool
import org.apache.streampark.flink.packer.pipeline._

import org.apache.commons.codec.digest.DigestUtils

import java.io.{File, FileInputStream, IOException}

/** Building pipeline for flink yarn application mode */
class FlinkYarnApplicationBuildPipeline(request: FlinkYarnApplicationBuildRequest)
  extends BuildPipeline {

  /** the type of pipeline */
  override def pipeType: PipelineType = PipelineType.FLINK_YARN_APPLICATION

  override def offerBuildParam: FlinkYarnApplicationBuildRequest = request

  /**
   * the actual build process. the effective steps progress should be implemented in multiple
   * BuildPipeline.execStep() functions.
   */
  @throws[Throwable]
  override protected def buildProcess(): SimpleBuildResponse = {
    execStep(1) {
      request.developmentMode match {
        // 如果是 flink-sql 模式下
        // 但是如果是 custom-code 模式 在 onStart() 已经上传了用户代码 jar
        case DevelopmentMode.FLINK_SQL =>
          // localWorkspace = ${workspace}/workspace/${application_id}
          LfsOperator.mkCleanDirs(request.localWorkspace)
          // yarnProvidedPath = ${workspace}/workspace/${application_id}
          HdfsOperator.mkCleanDirs(request.yarnProvidedPath)
        case _ =>
      }
      logInfo(s"recreate building workspace: ${request.yarnProvidedPath}")
    }.getOrElse(throw getError.exception)

    // 如果是 flink-sql 则检查需要哪些 maven 依赖
    val mavenJars =
      execStep(2) {
        request.developmentMode match {
          case DevelopmentMode.FLINK_SQL =>
            val mavenArts = MavenTool.resolveArtifacts(request.dependencyInfo.mavenArts)
            mavenArts.map(_.getAbsolutePath) ++ request.dependencyInfo.extJarLibs
          case _ => Set[String]()
        }
      }.getOrElse(throw getError.exception)

    execStep(3) {
      mavenJars.foreach(
        jar => {
          // 如果是 flink-sql 模式 则将依赖的jar 上传到 ${workspace}/workspace/${application_id} 目录下
          uploadToHdfs(FsOperator.lfs, jar, request.localWorkspace)
          uploadToHdfs(FsOperator.hdfs, jar, request.yarnProvidedPath)
        })
    }.getOrElse(throw getError.exception)

    SimpleBuildResponse()
  }

  @throws[IOException]
  private[this] def uploadToHdfs(fsOperator: FsOperator, origin: String, target: String): Unit = {
    val originFile = new File(origin)
    if (!fsOperator.exists(target)) {
      fsOperator.mkdirs(target)
    }
    if (originFile.isFile) {
      // check file in upload dir
      fsOperator match {
        case FsOperator.lfs =>
          fsOperator.copy(originFile.getAbsolutePath, target)
        case FsOperator.hdfs =>
          val uploadFile = s"${Workspace.remote.APP_UPLOADS}/${originFile.getName}"
          if (fsOperator.exists(uploadFile)) {
            Utils.using(new FileInputStream(originFile))(
              inputStream => {
                if (DigestUtils.md5Hex(inputStream) != fsOperator.fileMd5(uploadFile)) {
                  fsOperator.upload(originFile.getAbsolutePath, uploadFile)
                }
              })
          } else {
            fsOperator.upload(originFile.getAbsolutePath, uploadFile)
          }
          // copy jar from upload dir to target dir
          fsOperator.copy(uploadFile, target)
      }
    } else {
      fsOperator match {
        case FsOperator.hdfs => fsOperator.upload(originFile.getAbsolutePath, target)
        case _ =>
      }
    }
  }

}

object FlinkYarnApplicationBuildPipeline {
  def of(request: FlinkYarnApplicationBuildRequest): FlinkYarnApplicationBuildPipeline =
    new FlinkYarnApplicationBuildPipeline(request)
}
