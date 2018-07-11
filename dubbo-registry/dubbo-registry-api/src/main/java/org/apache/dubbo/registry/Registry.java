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
package org.apache.dubbo.registry;

import org.apache.dubbo.common.Node;
import org.apache.dubbo.common.URL;

/**
 * Registry. (SPI, Prototype, ThreadSafe)
 * 注册中心接口
 * Registry 继承了RegistryService 接口，拥有拥有注册、订阅、查询三种操作方法
 * Registry 同时继承了Node接口，拥有节点相关的方法
 * @see org.apache.dubbo.registry.RegistryFactory#getRegistry(URL)
 * @see org.apache.dubbo.registry.support.AbstractRegistry
 */
public interface Registry extends Node, RegistryService {
}