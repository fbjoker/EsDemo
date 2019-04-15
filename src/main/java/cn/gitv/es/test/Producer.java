package cn.gitv.es.test;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Producer implements Runnable {
    private final Queue queue;
    private static ExecutorService queueService = Executors.newCachedThreadPool();
    private static AtomicInteger syncount=new AtomicInteger(0);
    private String index;
    private int rows;
    private int num;
    private int thread;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public int getThread() {
        return thread;
    }

    public void setThread(int thread) {
        this.thread = thread;
    }

    Producer(Queue q) {
        queue = q;
    }
    public void run() {
            while(true) {
                for(int n=0;n<thread;n++){
                    queueService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                List<Map<String,Object>> allData= Collections.synchronizedList(new ArrayList<Map<String,Object>>());
                                for(int n=0;n<rows;n++){
                                    if("bulktest4".equalsIgnoreCase(index)){
                                        allData.add(EsTest.createDataNew());
                                    }else{
                                        allData.add(EsTest.createData(num));
                                    }
                                }
                                //queue.put(allData);
                                queue.add(allData);
                                syncount.getAndIncrement();
                            }catch (Exception e){
                                e.printStackTrace();
                            }

                        }
                    });

                }
                while (true){
                    if(syncount.get()<thread){
                        try {
                            Thread.sleep(10);
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }else{
                        if(queue.size()>1000){
                            try {
                                Thread.sleep(1000);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }else{
                            System.out.println("createData--------------------------"+queue.size());
                            syncount.set(0);
                            break;
                        }
                    }
                }
            }
    }


}
