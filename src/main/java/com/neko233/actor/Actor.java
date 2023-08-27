package com.neko233.actor;

import com.neko233.actor.message.ActorMessage;
import com.neko233.actor.message.ActorSyncMessage;
import com.neko233.actor.message.handler.ActorMessageHandler;
import com.neko233.actor.message.handler.ActorMessageTypeHandler;
import com.neko233.skilltree.annotation.Nullable;
import com.neko233.skilltree.commons.json.JsonUtils233;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public abstract class Actor {

    // register to actor-system-java get
    @Getter
    private String actorId;
    @Getter
    private ActorSystem actorSystem;
    // 邮箱
    private final BlockingQueue<ActorMessage> mailbox = new LinkedBlockingQueue<>();
    // 消息类型处理器
    private final Map<Class<?>, ActorMessageHandler<?>> messageTypeHandlerMap = new HashMap<>();
    // state
    private final AtomicBoolean isExecute = new AtomicBoolean(false);

    // actor system invoke
    protected void registerActorName(String name) {
        this.actorId = name;
    }

    // actor system invoke
    protected void registerActorSystem(ActorSystem actorSystem) {
        this.actorSystem = actorSystem;
    }


    public boolean receiveMessage(String sender,
                                  Object message) {
        ActorMessage msg = new ActorMessage(sender, message);
        return mailbox.offer(msg);
    }

    public <T> Actor registerHandler(ActorMessageTypeHandler<T> actorMessageTypeHandler) {
        return registerHandler(actorMessageTypeHandler.getType(), actorMessageTypeHandler);
    }

    public <T> Actor registerHandler(Class<T> type,
                                     ActorMessageHandler<T> actorMessageTypeHandler) {
        if (type == null) {
            return this;
        }
        if (actorMessageTypeHandler == null) {
            return this;
        }
        messageTypeHandlerMap.merge(type, actorMessageTypeHandler, (v1, v2) -> {
            log.info("typeHandler merge to newHandler. type = {}", type.getName());
            return v2;
        });
        return this;
    }


    public void handleMessageByPipeline(String senderActorName,
                                        Object message) {

        Actor sender = getActorByName(senderActorName);

        if (!(message instanceof ActorSyncMessage)) {
            // async message
            handleMessageAuto(senderActorName, message, sender);
            return;
        }

        final ActorSyncMessage syncMessage = (ActorSyncMessage) message;
        Object data = syncMessage.getMessage();
        handleMessageAuto(senderActorName, data, sender);

        syncMessage.countDown();
    }

    private void handleMessageAuto(String senderActorName,
                                   Object message,
                                   Actor sender) {
        Class<?> type = message.getClass();
        ActorMessageHandler actorMessageTypeHandler = messageTypeHandlerMap.get(type);
        if (actorMessageTypeHandler != null) {
            actorMessageTypeHandler.handle(sender, this, message);
            return;
        }

        onReceiveMessageWithoutAnyMatch(senderActorName, message);
    }

    @Nullable
    private Actor getActorByName(String actorId) {
        if (this.actorSystem == null) {
            return null;
        }

        return this.actorSystem.getActorByName(actorId);
    }

    /**
     * 当收到消息
     *
     * @param sender  发送人
     * @param message 消息
     */
    public abstract void onReceiveMessageWithoutAnyMatch(String sender,
                                                         Object message);


    public void takeAll() {
        while (true) {
            ActorMessage message = null;
            try {
                message = mailbox.poll(100, TimeUnit.MILLISECONDS);
                if (message == null) {
                    break;
                }

                this.handleMessageByPipeline(
                        message.getSender(),
                        message.getMessage()
                );
            } catch (Throwable e) {
                log.error("handle message error. message = {}", JsonUtils233.toJsonString(message), e);
            }
        }
    }

    public void consumeMessage() {
        ActorMessage message = null;
        try {
            message = mailbox.poll(100, TimeUnit.MILLISECONDS);
            if (message == null) {
                log.warn("空的调度. actorId = {}", actorId);
                return;
            }
            this.handleMessageByPipeline(
                    message.getSender(),
                    message.getMessage()
            );
        } catch (InterruptedException e) {
            log.error("handle message error. message = {}", JsonUtils233.toJsonString(message), e);
        }

        this.trySetIdleState();
    }

    /**
     * @param receiverActorName 接收人 actorId
     * @param message           消息
     * @return is success ?
     */
    public boolean send(String receiverActorName,
                        Object message) {
        return actorSystem.sendMessageAsync(this.actorId, receiverActorName, message);
    }

    /**
     * 同步调用. 跨线程, 霸占式
     *
     * @param receiverActorName 接收人的 actorId
     * @param message           消息
     * @return is success ?
     */
    public boolean talkPredatory(String receiverActorName,
                                 Object message) {
        return actorSystem.sendMessageSyncPredatory(receiverActorName, this.actorId, message);
    }

    /**
     * 同步调用. 跨线程, 顺序式
     *
     * @param receiverActorName 接收人的 actorId
     * @param message           消息
     * @return is success ?
     */
    public boolean talkOrderly(String receiverActorName,
                               Object message) {
        return actorSystem.sendMessageSyncOrderly(receiverActorName, this.actorId, message);
    }

    /**
     * @return 是否在执行中?
     */
    public boolean isInExecuteState() {
        return isExecute.get();
    }

    /**
     * @return 尝试设置 execute 状态
     */
    public boolean trySetExecuteState() {
        return this.isExecute.compareAndSet(false, true);
    }

    /**
     * @return 尝试设置 idle 状态
     */
    public boolean trySetIdleState() {
        return this.isExecute.compareAndSet(true, false);
    }

}
