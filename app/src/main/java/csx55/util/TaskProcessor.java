package csx55.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import csx55.hashing.Task;
import csx55.wireformats.NodeID;

public class TaskProcessor {

    private List<Thread> threadPool = new ArrayList<>();
    private BlockingQueue<Task> taskQueue = new ArrayBlockingQueue<>(1000);
    private AtomicInteger totalNumTasks = new AtomicInteger();
    private AtomicInteger numTasksToComplete = new AtomicInteger(0);
    private AtomicInteger tasksCompleted = new AtomicInteger(0);
    private AtomicInteger currRoundNum = new AtomicInteger();
    private AtomicInteger totalNumRounds = new AtomicInteger();
    private int totalNumRegisteredNodes;
    private Set<NodeID> completedNodes = ConcurrentHashMap.newKeySet();

    public TaskProcessor() {

    }
    
    // while(currRoundNum != totalNumRounds)
        // generate tasks
        // wait for completedNodes.size() == totalNumRegisteredNodes
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
            // clear set
            // create CountDownLatch set to numTasksToComplete
                // process tasks 
                // send TASK_COMPLETE message

    
}   // private void createTasks(){
    //     Random rand = new Random();
    //     for(int i = 0; i < rand.nextInt(999) + 1; i++){
    //         Task task = new Task(node.getIP(), node.getPort(), currRoundNum.get(), i);
    //         taskQueue.add(task);
    //     }
    // }

    // private void createThreadPool(int numThreads) {
    //     for(int i = 0; i < numThreads; i++){
    //         Thread t = new Thread(() -> {
    //             // while tasksCompleted != numTasksToComplete
    //                 // grab task from queue
    //                 // process task
    //                 // update tasksCompleted
    //         });
    //         t.start();
    //         threadPool.add(t);
    //     }
    // }
