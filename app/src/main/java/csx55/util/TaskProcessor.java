package csx55.util;

import java.util.logging.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import csx55.hashing.Task;
import csx55.wireformats.NodeID;

public class TaskProcessor {

    private Logger log = Logger.getLogger(this.getClass().getName());

    private NodeID node;
    private List<Thread> threadPool = new ArrayList<>();
    private BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private int taskSum = 0;
    private int totalNumRegisteredNodes;
    private int numThreads;
    private AtomicInteger numTasksToComplete = new AtomicInteger(0);
    private AtomicInteger tasksCompleted = new AtomicInteger(0);
    private Set<NodeID> completedNodes = ConcurrentHashMap.newKeySet();

    public TaskProcessor(NodeID node, int totalNumRegisteredNodes, int numThreads) {
        this.node = node;
        this.totalNumRegisteredNodes = totalNumRegisteredNodes;
        this.numThreads = numThreads;
        createThreadPool(numThreads);
    }

    // wait until completedNodes.size() == totalNumRegisteredNodes
        // clear set
    // compute numTasksToComplete -> totalNumTasks / totalNumRegisteredNodes
    // compute taskExcess -> taskQueue.size() - numTasksToComplete
    // wait for completedNodes.size() == totalNumRegisteredNodes
            // if excess is positive
                // send excess to neighbors
                // set numTasksToComplete to taskQueue.size()
                // send READY message
            // if excess is negative
                // wait for more tasks until taskQueue.size() == numTasksToComplete
                // send READY message
        // when all nodes are ready
            // process tasks 
            // send TASK_COMPLETE message
    
    
    public void createTasks(int totalNumRounds){
        Random rand = new Random();
        for(int i = 0; i < totalNumRounds; i++) {
            int tasks = rand.nextInt(999) + 1;
            for(int j = 0; j < tasks; j++){
                Task task = new Task(node.getIP(), node.getPort(), i, j);
                taskQueue.add(task);
            }
            taskSum += tasks;
        }
    }

    private void createThreadPool(int numThreads) {
        for(int i = 0; i < numThreads; i++){
            Thread t = new Thread(() -> {
                // while tasksCompleted != numTasksToComplete
                    // grab task from queue
                    // process task
                    // update tasksCompleted
            });
            t.start();
            threadPool.add(t);
        }
    }

    public int getTaskSum() {
        return taskSum;
    }

    public void printTasks(){
        log.info("Total number of tasks created: " + String.valueOf(taskQueue.size()));
    }
}
