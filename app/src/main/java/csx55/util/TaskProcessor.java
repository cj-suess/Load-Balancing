package csx55.util;

import java.util.logging.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import csx55.hashing.Miner;
import csx55.hashing.Task;
import csx55.threads.ComputeNode;
import csx55.wireformats.NodeID;

public class TaskProcessor {

    private Logger log = Logger.getLogger(this.getClass().getName());
    private NodeID id;
    private ComputeNode node;

    private List<Thread> threadPool = Collections.synchronizedList(new ArrayList<>());
    public BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    public Set<NodeID> completedTaskSumNodes = ConcurrentHashMap.newKeySet();

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

    public TaskProcessor(int numThreads, NodeID id, ComputeNode node) {
        this.numThreads = numThreads;
        this.id = id;
        this.node = node;
    }

    public void computeLoadBalancing() {
        computeFairShare();
        computeExcess();
        log.info("Number of tasks to complete: " + numTasksToComplete.get() + "\n\tExcess tasks to disperse: " + excessTasks.get());
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

    private boolean stop(){
        return phase.get() == Phase.DONE;
    }
    
    public void createTasks(int totalNumRounds){
        Random rand = new Random();
        for(int i = 0; i < totalNumRounds; i++) {
            int tasks = rand.nextInt(100) + 1;
            for(int j = 0; j < tasks; j++){
                Task task = new Task(id.getIP(), id.getPort(), i, j);
                taskQueue.add(task);
            }
        }
        totalTasks = taskQueue.size();
    }

    public void createThreadPool(int numThreads) {
        for(int i = 0; i < numThreads; i++){
            Thread thread = new Thread(() -> {
                try{
                    Miner miner = new Miner();
                    while(!stop()) {
                        Task t = taskQueue.poll(100, TimeUnit.MILLISECONDS);
                        if(t==null) {
                            node.checkForLoadBalancing();
                            continue;
                        }
                        try {
                            tasksBeingMined.incrementAndGet();
                            miner.mine(t);
                            tasksCompleted.incrementAndGet();
                        } finally {
                            tasksBeingMined.decrementAndGet();
                        }
                    }
                    if(tasksCompleted.get() == numTasksToComplete.get()) {
                        log.info("DONE");
                    }
                } catch(InterruptedException e) {
                    log.warning("Exception while processing tasks" + e.getStackTrace());
                }
            });
            thread.start();
            threadPool.add(thread);
        }
    }

    public int getTotalTasks() {
        return totalTasks;
    }

    public void printTasksInQueue(){
        log.info("Total number of tasks in queue: " + String.valueOf(taskQueue.size()));
    }
}
