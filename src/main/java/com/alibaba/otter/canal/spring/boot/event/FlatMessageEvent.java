package com.alibaba.otter.canal.spring.boot.event;

import com.alibaba.otter.canal.protocol.FlatMessage;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
public class FlatMessageEvent extends MessageEvent<FlatMessage> {

}
