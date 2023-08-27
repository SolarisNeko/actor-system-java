package com.neko233.actor.message;

import lombok.Getter;

import java.util.concurrent.CountDownLatch;

@Getter
public class ActorSyncMessage {

    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final String sender;
    private final Object message;

    public ActorSyncMessage(String sender,
                            Object message) {
        this.sender = sender;
        this.message = message;
    }

    public void countDown() {
        this.countDownLatch.countDown();
    }

    public void waitFinish() throws Exception {
        this.countDownLatch.await();
    }

}