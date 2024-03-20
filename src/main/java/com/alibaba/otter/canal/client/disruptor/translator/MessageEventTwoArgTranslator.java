package com.alibaba.otter.canal.client.disruptor.translator;

import com.alibaba.otter.canal.client.disruptor.MessageEvent;
import com.alibaba.otter.canal.protocol.Message;
import com.lmax.disruptor.EventTranslatorTwoArg;

import java.util.Arrays;

public class MessageEventTwoArgTranslator implements EventTranslatorTwoArg<MessageEvent, Boolean, Message> {

    @Override
    public void translateTo(MessageEvent event, long sequence, Boolean withoutAck, Message message) {
        event.setMessages(Arrays.asList(message));
    }

}
