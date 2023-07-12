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
package com.alibaba.otter.canal.spring.boot.hooks;

import com.alibaba.otter.canal.spring.boot.consumer.ConsumeMessageService;

import java.util.Objects;

public class CanalConsumerHook extends Thread{

	private ConsumeMessageService consumeMessageService;
	private long awaitTerminateMillis;
	public CanalConsumerHook(ConsumeMessageService consumeMessageService, long awaitTerminateMillis) {
		this.setName("canal-ConsumeMessageService-shutdown-hook");
		this.consumeMessageService = consumeMessageService;
		this.awaitTerminateMillis = awaitTerminateMillis;
	}
	
	@Override
	public void run() {
		if(Objects.nonNull(consumeMessageService)){
			consumeMessageService.shutdown(awaitTerminateMillis);
		}
	}
	
}