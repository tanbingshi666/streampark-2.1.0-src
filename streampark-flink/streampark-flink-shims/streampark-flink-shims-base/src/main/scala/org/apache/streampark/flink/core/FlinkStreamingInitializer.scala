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
package org.apache.streampark.flink.core

import org.apache.streampark.common.conf.ConfigConst._
import org.apache.streampark.common.enums.ApiType
import org.apache.streampark.common.enums.ApiType.ApiType
import org.apache.streampark.common.util._
import org.apache.streampark.flink.core.conf.FlinkConfiguration

import collection.{mutable, Map}
import collection.JavaConversions._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaStreamEnv}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableConfig

import java.io.File

private[flink] object FlinkStreamingInitializer {

  private[this] var flinkInitializer: FlinkStreamingInitializer = _

  def initialize(args: Array[String], config: (StreamExecutionEnvironment, ParameterTool) => Unit)
  : (ParameterTool, StreamExecutionEnvironment) = {
    if (flinkInitializer == null) {
      this.synchronized {
        if (flinkInitializer == null) {
          flinkInitializer = new FlinkStreamingInitializer(args, ApiType.scala)
          flinkInitializer.streamEnvConfFunc = config
          flinkInitializer.initEnvironment()
        }
      }
    }
    (flinkInitializer.configuration.parameter, flinkInitializer.streamEnvironment)
  }

  /**
   * 初始化 Flink Streaming Job 配置
   *
   * @param args
   * @return
   */
  def initialize(args: StreamEnvConfig): (ParameterTool, StreamExecutionEnvironment) = {
    // 典型的 double check lock 机制
    if (flinkInitializer == null) {
      this.synchronized {
        if (flinkInitializer == null) {
          // 创建 FlinkStreamingInitializer
          flinkInitializer = new FlinkStreamingInitializer(args.args, ApiType.java)
          // 赋值函数式接口
          flinkInitializer.javaStreamEnvConfFunc = args.conf
          // 初始化环境变量
          flinkInitializer.initEnvironment()
        }
      }
    }
    // 返回全局配置参数 + StreamExecutionEnvironment
    (flinkInitializer.configuration.parameter, flinkInitializer.streamEnvironment)
  }
}

private[flink] class FlinkStreamingInitializer(args: Array[String], apiType: ApiType)
  extends Logger {

  var streamEnvConfFunc: (StreamExecutionEnvironment, ParameterTool) => Unit = _

  var tableConfFunc: (TableConfig, ParameterTool) => Unit = _

  // Flink Java DataStream 自定义环境变量函数式接口
  var javaStreamEnvConfFunc: StreamEnvConfigFunction = _

  // Flink Java Table 自定义环境变量函数式接口
  var javaTableEnvConfFunc: TableEnvConfigFunction = _

  // Flink DataStream Job 的标配执行环境
  private[this] var localStreamEnv: StreamExecutionEnvironment = _

  // scala 赖加载机制 也即使用到才执行
  // 初始化参数
  lazy val configuration: FlinkConfiguration = initParameter()

  def initParameter(): FlinkConfiguration = {
    // 一般在程序入口 main() 的入参配置 --conf ${path}
    val argsMap = ParameterTool.fromArgs(args)
    val config = argsMap.get(KEY_APP_CONF(), null) match {
      case null | "" =>
        throw new ExceptionInInitializerError(
          "[StreamPark] Usage:can't fond config,please set \"--conf $path \" in main arguments")
      case file => file
    }
    // 加载解析 application.yaml
    val configMap = parseConfig(config)
    // 解析 application.yaml 文件内容前缀为 flink.property. 的内容 (注意会截取前缀)
    val properConf = extractConfigByPrefix(configMap, KEY_FLINK_PROPERTY_PREFIX)
    // 解析 application.yaml 文件内容前缀为 app. 的内容 (注意会截取前缀)
    val appConf = extractConfigByPrefix(configMap, KEY_APP_PREFIX)

    // config priority: explicitly specified priority > project profiles > system profiles
    // 配置优先级：显示声明参数(程序入口参数) > application.yaml 定义参数 > 系统环境变量参数
    val parameter = ParameterTool
      // 加载系统环境变量参数
      .fromSystemProperties()
      // 合并 application.yaml 参数 (存在即覆盖)
      .mergeWith(ParameterTool.fromMap(properConf))
      // 合并 APP 参数 (比如 Kafka、MySQL 定义参数)
      .mergeWith(ParameterTool.fromMap(appConf))
      // 合并程序入口参数(args) (存在即覆盖)
      .mergeWith(argsMap)

    val envConfig = Configuration.fromMap(properConf)
    // 封装 Flink Job 配置参数 FlinkConfiguration (也即相关参数列表)
    FlinkConfiguration(parameter, envConfig, null)
  }

  def parseConfig(config: String): Map[String, String] = {

    lazy val content = DeflaterUtils.unzipString(config.drop(7))

    def readConfig(text: String): Map[String, String] = {
      // 解析配置文件 (application.yaml) 的文件后缀名
      val format = config.split("\\.").last.toLowerCase
      format match {
        // 一般情况下都是 yml 或者 yaml
        case "yml" | "yaml" => PropertiesUtils.fromYamlText(text)
        case "conf" => PropertiesUtils.fromHoconText(text)
        case "properties" => PropertiesUtils.fromPropertiesText(text)
        case _ =>
          throw new IllegalArgumentException(
            "[StreamPark] Usage: application config file error,must be [yaml|conf|properties]")
      }
    }

    // 判断配置文件 application.yaml 存储在哪里
    val map = config match {
      case x if x.startsWith("yaml://") => PropertiesUtils.fromYamlText(content)
      case x if x.startsWith("conf://") => PropertiesUtils.fromHoconText(content)
      case x if x.startsWith("prop://") => PropertiesUtils.fromPropertiesText(content)
      case x if x.startsWith("hdfs://") =>
        // If the configuration file with the hdfs, user will need to copy the hdfs-related configuration files under the resources dir
        // 如果配置文件 application.yaml 存储在 HDFS 上
        val text = HdfsUtils.read(x)
        readConfig(text)
      case _ =>
        // 配置文件 application.yaml 存储在本地文件
        val configFile = new File(config)
        // 判断配置文件是否存在
        require(
          configFile.exists(),
          s"[StreamPark] Usage: application config file: $configFile is not found!!!")
        // 读取配置文件内容
        val text = FileUtils.readString(configFile)
        // 解析配置文件内容
        readConfig(text)
    }
    // 过滤
    map.filter(_._2.nonEmpty)
  }

  def extractConfigByPrefix(configMap: Map[String, String], prefix: String): Map[String, String] = {
    val map = mutable.Map[String, String]()
    configMap.foreach(
      x =>
        if (x._1.startsWith(prefix)) {
          map += x._1.drop(prefix.length) -> x._2
        })
    map
  }

  def streamEnvironment: StreamExecutionEnvironment = {
    if (localStreamEnv == null) {
      this.synchronized {
        if (localStreamEnv == null) {
          initEnvironment()
        }
      }
    }
    localStreamEnv
  }

  def initEnvironment(): Unit = {
    // Flink DataStream 标配执行环境获取
    localStreamEnv = new StreamExecutionEnvironment(
      JavaStreamEnv.getExecutionEnvironment(configuration.envConfig))

    // 判断用户是否配置了自定义函数式接口 如果定义了 则执行
    apiType match {
      case ApiType.java if javaStreamEnvConfFunc != null =>
        javaStreamEnvConfFunc.configuration(localStreamEnv.getJavaEnv, configuration.parameter)
      case ApiType.scala if streamEnvConfFunc != null =>
        streamEnvConfFunc(localStreamEnv, configuration.parameter)
      case _ =>
    }
    // 设置 Flink Job 的全局配置参数
    localStreamEnv.getConfig.setGlobalJobParameters(configuration.parameter)
  }

}
