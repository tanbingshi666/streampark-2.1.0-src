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
package org.apache.streampark.flink.core.conf

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration

case class FlinkConfiguration(
                               // 全局配置参数 (包括系统配置参数、application.yaml 参数、程序入口参数)
                               parameter: ParameterTool,
                               // Flink Job 参数 也即 application.yaml 前缀为 flink.property. 对应的所有参数 (不包含值为空的参数)
                               envConfig: Configuration,
                               // 针对  FLink DataStream Job 而言为 null
                               tableConfig: Configuration
                             )
