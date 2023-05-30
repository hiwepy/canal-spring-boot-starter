package com.alibaba.otter.canal.spring.boot.disruptor.event;

import com.alibaba.otter.canal.protocol.Message;
import lombok.Data;

import java.util.List;

@Data
public class MessageEvent {

    String uuid;
    List<Message> messages;

}
