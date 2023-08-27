package com.neko233.actor;

import com.neko233.actor.message.ActorSyncMessage;
import com.neko233.skilltree.commons.core.threadPool.ThreadPoolBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ActorSystem {

    public static final int MAX_NO_HANDLE_COUNT = 10;
    private final AtomicInteger systemFailExecuteCount = new AtomicInteger(0);
    private final Map<String, Actor> actorNameToActorMap = new HashMap<>();
    private final Map<String, AtomicInteger> actorNameToMessageCountMap = new ConcurrentHashMap<>();

    // async
    private final ThreadPoolExecutor asyncInvokeTp = ThreadPoolBuilder.builder()
            .corePoolSize(Runtime.getRuntime().availableProcessors())
            .maximumPoolSize(Runtime.getRuntime().availableProcessors())
            .keepAliveTime(30)
            .keepAliveTimeUnit(TimeUnit.SECONDS)
            .threadFactory(new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(1);

                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName("actor-system-java-async-" + counter.getAndIncrement());
                    return thread;
                }
            })
            .rejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy())
            .taskQueue(new LinkedBlockingQueue<>())
            .build();


    // 分发器
    private final Thread dispatcherThread = new Thread(() -> {
        while (true) {
            try {
                // 没有需要执行的
                if (actorNameToMessageCountMap.isEmpty()) {
                    this.systemFailExecuteCount.getAndIncrement();
                }

                for (Map.Entry<String, AtomicInteger> entry : actorNameToMessageCountMap.entrySet()) {
                    String actorId = entry.getKey();
                    AtomicInteger counter = entry.getValue();
                    int messageCount = counter.get();
                    if (messageCount <= 0) {
                        actorNameToMessageCountMap.remove(actorId);
                        continue;
                    }

                    Actor actor = actorNameToActorMap.get(actorId);
                    if (actor == null) {
                        continue;
                    }

                    // 正在执行中
                    boolean isExecute = actor.trySetExecuteState();
                    if (!isExecute) {
                        continue;
                    }

                    boolean isConsumeMessageSuccess = counter.compareAndSet(messageCount, messageCount - 1);
                    if (!isConsumeMessageSuccess) {
                        continue;
                    }

                    // 成功处理
                    this.systemFailExecuteCount.set(0);

                    // async invoke
                    asyncInvokeTp.submit(actor::consumeMessage);
                }

                if (systemFailExecuteCount.get() > MAX_NO_HANDLE_COUNT) {
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            } catch (Throwable t) {
                log.error("dispatcher-thread happen unknown exception", t);
            }
        }
    });

    public ActorSystem() {
        dispatcherThread.start();
    }

    public void addActor(String actorId,
                         Actor actor) {
        actorNameToActorMap.put(actorId, actor);

        actor.registerActorName(actorId);
        actor.registerActorSystem(this);
    }

    /**
     * 异步消息
     *
     * @param senderActorName   发送者的 actorId
     * @param receiverActorName 接收人的 actorId
     * @param message           消息
     * @return
     */
    public boolean sendMessageAsync(String senderActorName,
                                    String receiverActorName,
                                    Object message) {
        Actor actor = actorNameToActorMap.get(receiverActorName);
        if (actor == null) {
            log.error("not found actorId = {}", receiverActorName);
            return false;
        }

        boolean isSuccess = actor.receiveMessage(senderActorName, message);
        if (!isSuccess) {
            return false;
        }

        this.addMessageCount(receiverActorName);
        return true;
    }

    private void addMessageCount(String actorId) {
        AtomicInteger atomicInteger = actorNameToMessageCountMap.computeIfAbsent(actorId,
                key -> new AtomicInteger(0));
        atomicInteger.getAndIncrement();
    }

    /**
     * 同步霸占式调用
     *
     * @param receiverActorName 接收人 actorId
     * @param senderActorName   发送者 actorId
     * @param message           消息
     * @return is success ?
     */
    public boolean sendMessageSyncPredatory(String receiverActorName,
                                            String senderActorName,
                                            Object message) {
        Actor actor = actorNameToActorMap.get(receiverActorName);
        if (actor == null) {
            log.error("not found actorId = {}", receiverActorName);
            return false;
        }

        // 同步调用, 自旋同步尝试
        while (true) {
            try {
                boolean isSetStateSuccess = actor.trySetExecuteState();
                if (!isSetStateSuccess) {
                    TimeUnit.MILLISECONDS.sleep(50);
                    continue;
                }

                actor.handleMessageByPipeline(senderActorName, message);
                break;

            } catch (Exception e) {
                log.error("同步执行报错. senderActorName={}, receiverActorName={}",
                        senderActorName,
                        receiverActorName,
                        e);
                return false;
            } finally {
                actor.trySetIdleState();
            }
        }

        return true;

    }

    /**
     * 同步有序式调用
     *
     * @param receiverActorName 接收人 actorId
     * @param senderActorName   发送者 actorId
     * @param message           消息
     * @return is success ?
     */
    public boolean sendMessageSyncOrderly(String receiverActorName,
                                          String senderActorName,
                                          Object message) {
        Actor actor = actorNameToActorMap.get(receiverActorName);
        if (actor == null) {
            log.error("not found actorId = {}", receiverActorName);
            return false;
        }

        if (message instanceof ActorSyncMessage) {
            log.error("不允许发送 type = ActorSyncMessage 作为数据的同步消息!");
            return false;
        }
        final ActorSyncMessage syncMessage = new ActorSyncMessage(
                senderActorName,
                message
        );

        sendMessageAsync(senderActorName, receiverActorName, syncMessage);

        try {
            syncMessage.waitFinish();
        } catch (Exception e) {
            log.error("同步等待时报错. message = {}", message, e);
            return false;
        }

        return true;

    }

    public Actor getActorByName(String actorId) {
        return actorNameToActorMap.get(actorId);
    }
}
