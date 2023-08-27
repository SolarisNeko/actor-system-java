package com.neko233.actor.samples;

import com.neko233.actor.Actor;
import com.neko233.actor.ActorSystem;
import com.neko233.actor.samples.data.DemoActorUser;
import com.neko233.actor.samples.data.DemoSyncCallbackMessage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ActorSample {
    public static void main(String[] args) {
        ActorSystem actorSystem = new ActorSystem();

        // build actor
        final Actor actor1 = new DemoActorUser()
                .registerHandler(String.class, (sender, receiver, msg) -> {
                    log.info("收到 string msg = {}", msg);
                })
                .registerHandler(DemoSyncCallbackMessage.class, (sender, receiver, msg) -> {
                    String request = msg.getRequest();
                    log.info("[sync] 收到 msg = {}", request);

                    msg.setResponse("已收到 sync msg. receiver = " + receiver.getActorId());
                });
        final Actor actor2 = new DemoActorUser();

        actorSystem.addActor("actor1", actor1);
        actorSystem.addActor("actor2", actor2);

        // 1、async invoke
        actorSystem.sendMessageAsync("actor1", "actor2", "Hello from actor1!");
        actorSystem.sendMessageAsync("actor2", "actor1", "Hello from actor2!");

        // actor send
        actor2.send("actor1", "halo actor1");

        DemoSyncCallbackMessage build = DemoSyncCallbackMessage.builder()
                .request("[sync] demo callback msg")
                .build();
        // 2、sync invoke
        actor2.talkOrderly("actor1", build);
        log.info("[sync-callback] message = {}", build.getResponse());
    }
}
