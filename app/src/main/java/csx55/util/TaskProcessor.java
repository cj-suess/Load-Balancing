package csx55.util;

import java.util.logging.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import csx55.hashing.Miner;
import csx55.hashing.Task;
import csx55.threads.ComputeNode;
import csx55.wireformats.NodeID;

public class TaskProcessor {

    private Logger log = Logger.getLogger(this.getClass().getName());

    private ComputeNode node;
    private NodeID id;

    private List<Thread> threadPool = new ArrayList<>();
    public BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    public Set<NodeID> completedTaskSumNodes = ConcurrentHashMap.newKeySet();

    private int totalTasks = 0;
    private int numThreads;
    public AtomicInteger numTasksToComplete = new AtomicInteger(0);
    public AtomicInteger tasksCompleted = new AtomicInteger(0);
    public AtomicInteger networkTaskSum = new AtomicInteger(0);
    public AtomicInteger totalNumRegisteredNodes = new AtomicInteger(0);
    public AtomicInteger excessTasks = new AtomicInteger(0);

    public TaskProcessor(ComputeNode node, int numThreads, NodeID id) {
        this.node = node;
        this.numThreads = numThreads;
        this.id = id;
    }

    public void computeLoadBalancing() {
        computeFairShare();
        computeExcess();
        log.info("Number of tasks to complete: " + numTasksToComplete.get() + "\n\tExcess tasks to disperse: " + excessTasks.get());
    }

    private void computeExcess() {
        excessTasks.getAndSet(totalTasks - numTasksToComplete.get());
    }

    private void computeFairShare(){
        numTasksToComplete.getAndSet(networkTaskSum.get()/totalNumRegisteredNodes.get());
    }
    
    public void processTasks(){
        createThreadPool(numThreads);
        for(Thread t : threadPool) {
            t.start();
        }
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

    private void createThreadPool(int numThreads) {
        for(int i = 0; i < numThreads; i++){
            Thread thread = new Thread(() -> {
                try{
                    while(true){
                        int completedTasks = tasksCompleted.get();
                        if(completedTasks == numTasksToComplete.get()){
                            break;
                        }
                        if(tasksCompleted.compareAndSet(completedTasks, completedTasks + 1)){
                            Task task = taskQueue.take();
                            Miner miner = new Miner();
                            miner.mine(task);

                            long newCount = completedTasks + 1;
                            log.info("Tasks completed: " + newCount + "/" + numTasksToComplete.get());

                            if(newCount == numTasksToComplete.get()){
                                log.info("All required tasks processed...");

                            }
                        }
                    }
                }catch(InterruptedException e) {
                    log.warning("Exception while processing tasks" + e.getStackTrace());
                }
            });
            threadPool.add(thread);
        }
    }

    public int getTotalTasks() {
        return totalTasks;
    }

    public void printTasks(){
        log.info("Total number of tasks created: " + String.valueOf(taskQueue.size()));
    }
}
