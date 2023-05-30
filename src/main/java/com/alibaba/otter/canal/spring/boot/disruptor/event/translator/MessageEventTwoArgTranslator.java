/*
 * Copyright (c) 2017, hiwepy (https://github.com/hiwepy).
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
package com.alibaba.otter.canal.spring.boot.disruptor.event.translator;

import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spring.boot.disruptor.event.MessageEvent;
import com.lmax.disruptor.EventTranslatorTwoArg;

import java.util.Arrays;

public class MessageEventTwoArgTranslator implements EventTranslatorTwoArg<MessageEvent, Boolean, Message> {

	@Override
	public void translateTo(MessageEvent event, long sequence, Boolean withoutAck, Message message) {
		event.setMessages(Arrays.asList(message));
	}

}
