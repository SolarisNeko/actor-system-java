package com.neko233.actor.samples.data;

import com.neko233.actor.Actor;
import com.neko233.skilltree.commons.core.base.StringUtils233;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DemoActorUser extends Actor {

    @Override
    public void onReceiveMessageWithoutAnyMatch(String sender,
                                                Object message) {
        String content = StringUtils233.format("from = {}, Received message= {}", sender, message);
        log.info(content);
    }
}
