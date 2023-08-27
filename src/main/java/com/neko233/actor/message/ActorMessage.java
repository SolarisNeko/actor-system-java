package com.neko233.actor.message;

import lombok.Getter;

// Actor message
@Getter
public class ActorMessage {

    private final String sender;
    private final Object message;

    public ActorMessage(String sender,
                        Object message) {
        this.sender = sender;
        this.message = message;
    }

}