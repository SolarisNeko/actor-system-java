package com.neko233.actor.message.handler;

import com.neko233.actor.Actor;
import com.neko233.skilltree.annotation.NotNull;
import com.neko233.skilltree.annotation.Nullable;

public interface ActorMessageHandler<T> {

    /**
     * @param sender   发送者
     * @param receiver 接收人 (自己)
     * @param message  消息
     */
    void handle(@Nullable Actor sender,
                @NotNull Actor receiver,
                T message);

}
