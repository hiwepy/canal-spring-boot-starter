/*
 * Copyright (c) 2018, hiwepy (https://github.com/hiwepy).
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.alibaba.otter.canal.spring.boot.message;

import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.spring.boot.Action;

/**
 * 消息监听器，Consumer注册消息监听器来订阅消息.
 *
 * <p>
 *     <strong>线程安全性要求: </strong> 该接口会被多个线程并发调用, 用户需要保证并发安全性.
 * </p>
 */
public interface FlatMessageListener {
	
    /**
     * 消费消息接口，由应用来实现<br>
     * 网络抖动等不稳定的情形可能会带来消息重复，对重复消息敏感的业务可对消息做幂等处理
     *
     * @param message 消息
     * @return 消费结果，如果应用抛出异常或者返回Null等价于返回Action.ReconsumeLater
     */
    Action consume(final FlatMessage message);
}
