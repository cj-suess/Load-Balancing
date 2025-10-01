package csx55.util;

import java.util.logging.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import csx55.hashing.Miner;
import csx55.hashing.Task;
import csx55.threads.ComputeNode;
import csx55.wireformats.NodeID;
import csx55.wireformats.Protocol;
import csx55.wireformats.TaskComplete;

public class TaskProcessor {

    private Logger log = Logger.getLogger(this.getClass().getName());
    private NodeID id;
    private ComputeNode node;
    public int numRounds;

    private List<Thread> threadPool = Collections.synchronizedList(new ArrayList<>());
    public ArrayBlockingQueue<Task> taskQueue = new ArrayBlockingQueue<>(50000);
    public Set<NodeID> completedTaskSumNodes = ConcurrentHashMap.newKeySet();
    public ArrayBlockingQueue<Task> excessQueue = new ArrayBlockingQueue<>(50000);

    public enum Phase {PROCESSING, LOAD_BALANCE, DONE};
    public AtomicReference<Phase> phase = new AtomicReference<>(Phase.PROCESSING);
    public AtomicInteger tasksBeingMined = new AtomicInteger(0);
    public AtomicInteger pendingRequests = new AtomicInteger(0);
    private int totalTasks = 0;
    private int numThreads;
    public AtomicInteger numTasksToComplete = new AtomicInteger(0);
    public AtomicInteger tasksCompleted = new AtomicInteger(0);
    public AtomicInteger networkTaskSum = new AtomicInteger(0);
    public AtomicInteger totalNumRegisteredNodes = new AtomicInteger(0);
    public AtomicInteger excessTasks = new AtomicInteger(0);
    public AtomicInteger startingTaskQueueBeforeMining = new AtomicInteger(0);
    public float percentOfWorkCompleted = 0;

    public TaskProcessor(int numThreads, NodeID id, ComputeNode node) {
        this.numThreads = numThreads;
        this.id = id;
        this.node = node;
    }

    public void computeLoadBalancing() {
        computeFairShare();
        computeExcess();
        if(excessTasks.get() > 0) {
            for(int i = 0; i < excessTasks.get(); i++) {
                Task t = taskQueue.poll();
                if(t != null) {
                    excessQueue.add(t);
                }
            }
        }
        excessTasks.set(excessQueue.size());
        startingTaskQueueBeforeMining.set(taskQueue.size());
        log.info("Number of tasks to complete: " + numTasksToComplete.get() + "\n\tExcess tasks to disperse: " + excessTasks.get());
    }

    public void setNumRounds(int numRounds){
        this.numRounds = numRounds;
    }

    private void computeExcess() {
        excessTasks.set(totalTasks - numTasksToComplete.get());
    }

    private void computeFairShare(){
        if(totalNumRegisteredNodes.get() != 0){
            numTasksToComplete.getAndSet(networkTaskSum.get()/totalNumRegisteredNodes.get());
        }
    }

    public int remainingTasksNeeded() {
        return Math.max(0, numTasksToComplete.get() - (tasksCompleted.get() + tasksBeingMined.get()));
    }

    public float calculatePercentageOfWork() {
        return (float) tasksCompleted.get() / (float) networkTaskSum.get();
    }

    private boolean stop(){
        return phase.get() == Phase.DONE;
    }
    
    public void createTasks(int totalNumRounds){
        Random rand = new Random();
        for(int i = 0; i < totalNumRounds; i++) {
            int tasks = rand.nextInt(999) + 1;
            for(int j = 0; j < tasks; j++){
                Task task = new Task(id.getIP(), id.getPort(), i, j);
                taskQueue.add(task);
            }
        }
        totalTasks = taskQueue.size();
    }

    private AtomicLong startTime = new AtomicLong(0);
    public void createThreadPool(int numThreads) {
        for(int i = 0; i < numThreads; i++){
            Thread thread = new Thread(() -> {
                Miner miner = new Miner();
                try {
                    while(!stop()) {
                        Task t = taskQueue.poll(100, TimeUnit.MILLISECONDS);
                        if(t==null) {
                            node.checkForLoadBalancing();
                            continue;
                        }
                        startTime.compareAndSet(0, System.nanoTime());
                        try {
                            tasksBeingMined.incrementAndGet();
                            miner.mine(t);
                            System.out.println(t.toString());
                            Thread.yield();
                            tasksCompleted.incrementAndGet();
                            // log.info("Tasks mined: " + tasksCompleted.get() + "/" + numTasksToComplete.get());
                            if (tasksCompleted.get() % 10 == 0) {
                                // log.info("Tasks mined: " + tasksCompleted.get() + "/" + numTasksToComplete.get());
                            }
                        } finally {
                            tasksBeingMined.decrementAndGet();
                        }
                        if(tasksCompleted.get() == numTasksToComplete.get()) {
                            long endTime = System.nanoTime();
                            long durationNanos = endTime - startTime.get();
                            long durationSeconds = TimeUnit.NANOSECONDS.toSeconds(durationNanos);
                            log.info("DONE! Total mining time: " + durationSeconds + " s");
                            // phase.compareAndSet(Phase.PROCESSING, Phase.DONE);
                            try {
                                TaskComplete tc = new TaskComplete(Protocol.TASK_COMPLETE, node.myNode.getIP(), node.myNode.getPort());
                                node.registryConn.sender.sendData(tc.getBytes());
                            } catch(IOException e) {
                                log.warning("IOException while sending task complete message to registry..." + e.getStackTrace());
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    log.warning("Exception while mining" + e.getStackTrace());
                }
            });
            thread.start();
            threadPool.add(thread);
        }
    }

    public void drainQueues(){
        taskQueue.addAll(excessQueue);
        if(phase.compareAndSet(Phase.DONE, Phase.PROCESSING)){
            log.info("State is DONE. Draining task queue and returning to PROCESSING.");
        }
    }

    public int getTotalTasks() {
        return totalTasks;
    }

    public void printPhase() {
        log.info(phase.toString());
    }

    public int getPendingRequests() {
        return pendingRequests.get();
    }

    public void printPendingRequests() {
        log.info("Current pending requests: " + getPendingRequests());
    }

    public void printTasksInQueue() {
        log.info("Total number of tasks in task queue: " + String.valueOf(taskQueue.size()));
    }

    public void printTaskInfo(){
        log.info("Total number of tasks created: " + String.valueOf(getTotalTasks()));
        log.info("Starting tasks in queue before mining: " + String.valueOf(startingTaskQueueBeforeMining.get()));
        log.info("Total number of tasks in task queue: " + String.valueOf(taskQueue.size()));
        log.info("Total number of tasks in excess queue: " + String.valueOf(excessQueue.size()));
        log.info("Network task sum: " + String.valueOf(networkTaskSum.get()));
    }
}
