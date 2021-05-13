package gca.main;

import file.text.TextWriter;
import guarana.framework.message.Message;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;

import org.w3c.dom.Document;

import tdg.concurrency.activity.ActiveObject;
import gca.cafe.solution.IntProcess;

public class CafeWorker extends ActiveObject {

    private int rate;
    private IntProcess process;
    private String summaryFile;
    private boolean stop;
    private ArrayList<Message<Document>> messages;

    public CafeWorker(int rate, IntProcess prc, ArrayList<Message<Document>> messages, String summaryFile) {
        super("Worker");
        this.rate = rate;
        this.process = prc;
        this.messages = messages;
        this.summaryFile = summaryFile;
    }

    public void stop() {
        super.stop();
        stop = true;
    }

    @Override
    protected void doWork() {

        long start = 0, end = 0, beginInj = 0, endInj = 0;
        DecimalFormat df = new DecimalFormat("####.######");

        String msg = "Message injection started at --> " + new Date();
        System.out.println(msg);
        TextWriter.writeString2File(msg, summaryFile, true);

        // novo 
        int index = 0;
        try {
            start = System.currentTimeMillis();
            while (!stop) {
                beginInj = System.currentTimeMillis();
                for (int k = 0; (k < rate & !stop & index < messages.size()); k++) {

                    this.messages.get(index).getHeader().addDynamicAttribute("enter", System.nanoTime());

                    process.communicatorEntry.pushRead(this.messages.get(index));
                    index++;
                }
                endInj = System.currentTimeMillis();

               
                long time = 1000 - (endInj - beginInj);
                if (time > 0) {
                    Thread.sleep(time);
                }
            }
            end = System.currentTimeMillis();
        } catch (Exception ex) {
            ex.printStackTrace();
        }


        msg = "Number of messages injected ----> " + index;
        System.out.println(msg);
        TextWriter.writeString2File(msg, summaryFile, true);

        msg = "Actual injection time ---------> " + df.format(((end - start) / 1000f / 60f)) + " min";
        System.out.println(msg);
        TextWriter.writeString2File(msg, summaryFile, true);

        msg = "Message injection finished!";
        System.out.println(msg);
        TextWriter.writeString2File(msg, summaryFile, true);

    }

}
