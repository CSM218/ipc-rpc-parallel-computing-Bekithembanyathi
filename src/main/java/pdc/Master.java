package pdc;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;


public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ConcurrentHashMap<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> results = new ConcurrentHashMap<>();
    private final BlockingQueue<RpcTask> taskQueue = new LinkedBlockingQueue<>();
    private final ConcurrentHashMap<String, RpcTask> pendingTasks = new ConcurrentHashMap<>();
    private final AtomicInteger taskIdCounter = new AtomicInteger(0);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String studentId;
    
  
    static class WorkerInfo {
        Socket socket;
        long lastSeen;
        volatile boolean active = true;
        String workerId;
        
        WorkerInfo(Socket socket, String workerId) {
            this.socket = socket;
            this.workerId = workerId;
            this.lastSeen = System.currentTimeMillis();
        }
    }
    
  
    static class RpcTask {
        String taskId;
        String taskData;
        int attemptCount = 0;
        long submittedTime;
        
        RpcTask(String taskId, String taskData) {
            this.taskId = taskId;
            this.taskData = taskData;
            this.submittedTime = System.currentTimeMillis();
        }
    }
    
    public Master() {
     
        this.studentId = System.getenv("STUDENT_ID") != null ? 
            System.getenv("STUDENT_ID") : "student01";
    }
  
    void handleWorker(Socket workerSocket) {
        String workerId = null;
        try {
         
            Message.readFrom(workerSocket.getInputStream());        
            Message registrationMessage = Message.readFrom(workerSocket.getInputStream());  
            workerId = registrationMessage.payload;
            
          
            new Message("WORKER_ACK", studentId, "ok").writeTo(workerSocket.getOutputStream());
            Message.readFrom(workerSocket.getInputStream());     
            workers.put(workerId, new WorkerInfo(workerSocket, workerId));
            
         
            while (running.get()) {
                Message message = Message.readFrom(workerSocket.getInputStream());
                              
                WorkerInfo workerInfo = workers.get(workerId);
                if (workerInfo != null) {
                    workerInfo.lastSeen = System.currentTimeMillis();
                }
            
                if (message.messageType.equals("TASK_COMPLETE")) {
                    String[] payloadParts = message.payload.split("\\|");
                    results.put(payloadParts[0], payloadParts[1]);
                    pendingTasks.remove(payloadParts[0]);
                }
            }
        } catch (Exception e) {
       
            if (workerId != null) {
                WorkerInfo info = workers.get(workerId);
                if (info != null) {
                    info.active = false;
                }
            }
        }
    }
    
  
    void dispatchRpcRequests() {
        systemThreads.submit(() -> {
            while (running.get()) {
                try {
                    RpcTask task = taskQueue.poll(1, TimeUnit.SECONDS);
                    if (task != null) {
                   
                        List<WorkerInfo> workerList = new ArrayList<>(workers.values());
                        workerList.removeIf(w -> !w.active);
                        
                        if (!workerList.isEmpty()) {
                            int workerIndex = taskIdCounter.getAndIncrement() % workerList.size();
                            pendingTasks.put(task.taskId, task);
                            new Message("RPC_REQUEST", studentId, task.taskId + "|" + task.taskData).writeTo(workerList.get(workerIndex).socket.getOutputStream());
                        } else {
                            
                            taskQueue.offer(task);
                        }
                    }
                } catch (Exception e) {
                  
                }
            }
        });
    }

  
    public Object coordinate(String operation, int[][] data, int workerCount) {
        
        List<Callable<int[][]>> tasks = new ArrayList<>();
        
        for (int i = 0; i < workerCount; i++) {
            final int workerId = i;
            tasks.add(() -> {
                
                return new int[data.length][data[0].length];
            });
        }
        
        try {
            systemThreads.invokeAll(tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        return data;
    }


    public void listen(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        
      
        dispatchRpcRequests();
        
      
        while (running.get()) {
            Socket workerSocket = serverSocket.accept();
            systemThreads.submit(() -> handleWorker(workerSocket));
        }
    }

   
    public void reconcileState() {
      
        workers.entrySet().removeIf(entry -> !entry.getValue().active);
    }
    
    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 1111;
        Master master = new Master();
        master.listen(port);
    }
}
