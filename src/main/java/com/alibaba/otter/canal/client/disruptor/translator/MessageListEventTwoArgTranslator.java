package com.alibaba.otter.canal.client.disruptor.translator;


import com.alibaba.otter.canal.client.disruptor.MessageEvent;
import com.alibaba.otter.canal.protocol.Message;
import com.lmax.disruptor.EventTranslatorTwoArg;

import java.util.List;

public class MessageListEventTwoArgTranslator implements EventTranslatorTwoArg<MessageEvent, Boolean, List<Message>> {

    @Override
    public void translateTo(MessageEvent event, long sequence, Boolean withoutAck, List<Message> message) {
        event.setMessages(message);
    }

}
