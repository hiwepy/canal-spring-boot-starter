package com.alibaba.otter.canal.client.disruptor;

import com.alibaba.otter.canal.protocol.Message;
import lombok.Data;

import java.util.List;

@Data
public class MessageEvent {

    String uuid;
    Message message;
    List<Message> messages;

}
