package pdc;

import java.net.Socket;
import java.util.concurrent.*;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {
    String id;
    String studentId;
    
    Socket socket;
    
    ExecutorService executor = Executors.newFixedThreadPool(4);
    volatile boolean running = true;
    
    public Worker() {
       
        this.id = System.getenv("WORKER_ID") != null ? 
            System.getenv("WORKER_ID") : "worker01";
        this.studentId = System.getenv("STUDENT_ID") != null ? 
            System.getenv("STUDENT_ID") : "student01";
    }

    public void joinCluster(String host, int port) throws Exception {
        try {
           
            socket = new Socket(host, port);

            new Message("CONNECT", studentId, id).writeTo(socket.getOutputStream());
            new Message("REGISTER_WORKER", studentId, id).writeTo(socket.getOutputStream());   
            Message.readFrom(socket.getInputStream());
            new Message("REGISTER_CAPABILITIES", studentId, "MATRIX").writeTo(socket.getOutputStream());
            System.out.println("Worker " + id + " is  connected to master");
        } catch (Exception e) {
            System.err.println("Failed to connect: " + e.getMessage());
        }
    }

  
    public void execute() {
        try {
            while (running) {
                Message message = Message.readFrom(socket.getInputStream());
                
                if (message.messageType.equals("RPC_REQUEST")) {
                    executor.submit(() -> handleTask(message));
                    
                } else if (message.messageType.equals("HEARTBEAT")) {
                 
                    executor.submit(() -> {
                        try {
                            new Message("HEARTBEAT_ACK", studentId, id).writeTo(socket.getOutputStream());
                        } catch (Exception e) {}
                    });
                    
                } else if (message.messageType.equals("SHUTDOWN")) {
                    running = false;
                }
            }
        } catch (Exception e) {
          
        }
    }

  
    void handleTask(Message message) {
        try {
         
            String[] payloadParts = message.payload.split("\\|");
            String taskId = payloadParts[0];
            String taskData = payloadParts[1];
            String result = doWork(taskData);
            synchronized(socket) {
                new Message("TASK_COMPLETE", studentId, taskId + "|" + result)
                    .writeTo(socket.getOutputStream());
            }
        } catch (Exception e) {
          
        }
    }

 
    String doWork(String taskData) {
        String[] dataParts = taskData.split(":");
        String[] rangeInfo = dataParts[0].split(",");
        int startRow = Integer.parseInt(rangeInfo[0]);
        int endRow = Integer.parseInt(rangeInfo[1]);
        int matrix1Cols = Integer.parseInt(rangeInfo[2]);
        int matrix2Cols = Integer.parseInt(rangeInfo[3]);
        
        int[][] matrix1 = toMatrix(dataParts[1]);
        int[][] matrix2 = toMatrix(dataParts[2]);
        int[][] resultMatrix = new int[endRow - startRow][matrix2Cols];
        

        for (int row = startRow; row < endRow; row++) {
            for (int col = 0; col < matrix2Cols; col++) {
                for (int k = 0; k < matrix1Cols; k++) {
                    resultMatrix[row - startRow][col] += matrix1[row][k] * matrix2[k][col];
                }
            }
        }
        
        return startRow + "," + endRow + ":" + toString(resultMatrix);
    }

   
    int[][] toMatrix(String matrixString) {
        String[] numbers = matrixString.split(",");
        int rows = Integer.parseInt(numbers[0]);
        int cols = Integer.parseInt(numbers[1]);
        int[][] matrix = new int[rows][cols];
        int index = 2;
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                matrix[row][col] = Integer.parseInt(numbers[index++]);
            }
        }
        return matrix;
    }

 
    String toString(int[][] matrix) {
        String result = matrix.length + "," + matrix[0].length;
        for (int row = 0; row < matrix.length; row++) {
            for (int col = 0; col < matrix[0].length; col++) {
                result += "," + matrix[row][col];
            }
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
       
        String host = System.getenv("MASTER_HOST") != null ? 
            System.getenv("MASTER_HOST") : "localhost";
        String port = System.getenv("MASTER_PORT") != null ? 
            System.getenv("MASTER_PORT") : "1111";
        
        if (args.length > 0) host = args[0];
        if (args.length > 1) port = args[1];
        
        Worker w = new Worker();
        w.joinCluster(host, Integer.parseInt(port));
        w.execute();
    }
}
