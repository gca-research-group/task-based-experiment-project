package gca.main;

import file.text.TextWriter;
import file.xml.XMLParams;
import guarana.framework.message.Message;
import guarana.toolkit.engine.Scheduler;
import guarana.util.deepcopy.DeepCopy;
import guarana.util.xml.XMLHandler;

import java.math.BigInteger;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import gca.cafe.solution.IntProcess;

public class Start {

    public static int REPETITION_TIMES;
    public static int TIME_LIMIT;

    public static String EXPERIMENTS_CONF_FILE = "./experiments-conf.xml";
    public static String GUARANA_CONF_TEMPLATE_FILE = "./guarana-conf-template.xml";
    public static String GUARANA_CONF_FILE = "./guarana-conf.xml";

    public static ArrayList<Integer> ARRIVAL_RATES;
    public static ArrayList<Integer> NUMBER_OF_WORKERS;
    public static String STATS_FOLDER;

    static {
        String[] params = {"time-limit", "repeat", "arrival-rates", "threads", "stats-folder"};
        Map<String, String> values = XMLParams.load(params, EXPERIMENTS_CONF_FILE);

        ARRIVAL_RATES = new ArrayList<Integer>();
        for (String e : values.get("arrival-rates").split(",")) {
            ARRIVAL_RATES.add(new Integer(e.trim()));
        }

        NUMBER_OF_WORKERS = new ArrayList<Integer>();
        for (String e : values.get("threads").split(",")) {
            NUMBER_OF_WORKERS.add(new Integer(e.trim()));
        }

        REPETITION_TIMES = Integer.parseInt(values.get("repeat").trim());
        TIME_LIMIT = Integer.parseInt(values.get("time-limit").trim());
        STATS_FOLDER = values.get("stats-folder").trim();
    }

    public static void main(String[] args) {
        System.out.println("<Experimentation started!>");
        System.out.println("---- Cafe ----");
        new Start().start();
        System.out.println("<Experimentation finished!>");
    }

    public void start() {
        String msg = null;
        ArrayList<Message<Document>> generatedMessages = null;

        try {

            for (int r : ARRIVAL_RATES) {

                HashMap<Integer, DataSet> data = new HashMap<Integer, DataSet>();

                msg = ">>> Building messages... ";
                System.out.println(msg);

                generatedMessages = buildMessages(r * TIME_LIMIT);
                int m = generatedMessages.size();

                msg = ">>> Built: " + generatedMessages.size();
                System.out.println(msg);

                for (int w : NUMBER_OF_WORKERS) {
                    String summaryFile = STATS_FOLDER + "execution-summary - [" + w + "] [" + r + "] [" + m + "].txt";

                    data.put(w, new DataSet());

                    for (int t = 0; t < REPETITION_TIMES; t++) {

                        long a = System.currentTimeMillis();
                        ArrayList<Message<Document>> messages = (ArrayList<Message<Document>>) DeepCopy.copy(generatedMessages);
                        long b = System.currentTimeMillis();

                        DecimalFormat df = new DecimalFormat("####.######");
                        System.out.println(">>> Time for copying messages: " + df.format(((b - a) / 1000f / 60f)) + " min");

                        msg = "Starting system at -------------> " + new Date() + " [" + (t + 1) + "/" + REPETITION_TIMES + "]";
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        msg = "Number of workers --------------> " + w;
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        msg = "Arrival rate -------------------> " + r + " msg/seg";
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        Document doc = XMLHandler.readXmlFile(GUARANA_CONF_TEMPLATE_FILE);

                        XPathExpression e1 = XMLHandler.getXPathExpression("//threads");
                        ((NodeList) e1.evaluate(doc, XPathConstants.NODESET)).item(0).setTextContent(Integer.toString(w));

                        XPathExpression e2 = XMLHandler.getXPathExpression("//monitoring/work-queue/file");
                        ((NodeList) e2.evaluate(doc, XPathConstants.NODESET)).item(0).setTextContent(STATS_FOLDER + "work-queue-stats - [" + w + "] [" + r + "] [" + m + "].txt");

                        XPathExpression e3 = XMLHandler.getXPathExpression("//monitoring/workers/file");
                        ((NodeList) e3.evaluate(doc, XPathConstants.NODESET)).item(0).setTextContent(STATS_FOLDER + "workers-stats - [" + w + "] [" + r + "] [" + m + "].txt");

                        XPathExpression e4 = XMLHandler.getXPathExpression("//monitoring/memory/file");
                        ((NodeList) e4.evaluate(doc, XPathConstants.NODESET)).item(0).setTextContent(STATS_FOLDER + "memory-stats - [" + w + "] [" + r + "] [" + m + "].txt");

                        XMLHandler.writeXmlFile(doc, GUARANA_CONF_FILE);

                        Scheduler exec = new Scheduler("Scheduler");
                        IntProcess prc = new IntProcess();
                        exec.registerProcess(prc);

                        msg = "Starting workers... ";
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        long start = System.currentTimeMillis();
                        exec.start();

                        msg = "System running...";
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        CafeWorker pusher = new CafeWorker(r, prc, messages, summaryFile);
                        pusher.start();

                        // stop the system after a given time
                        Thread.sleep(TIME_LIMIT * 1000);

                        pusher.stop();

                        msg = "Stop message injection";
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        long orders = prc.ordersServed;
                        data.get(w).messages.add(orders);
                        msg = "Number of orders processed: " + orders;
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        long wu = exec.getNumberOfWorkUnitsProcessed();
                        data.get(w).workunits.add(wu);
                        msg = "Number of work units processed -> " + wu;
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        /* MAKESPAN */
                        BigInteger totalMakespan = new BigInteger("0");
                        long spanMax = 0;
                        long spanMin = Long.MAX_VALUE;

                        for (int k = 0; k < prc.processingTimes.size(); k++) {
                            Long s = prc.processingTimes.get(k);
                            long v = s.longValue();
                            totalMakespan = totalMakespan.add(new BigInteger(Long.toString(v)));
                            if (v > spanMax) {
                                spanMax = v;
                            }
                            if (v < spanMin) {
                                spanMin = v;
                            }
                        }

                        Makespan mk = new Makespan();
                        if (!prc.processingTimes.isEmpty()) {
                            mk.setAverage(Long.parseLong(totalMakespan.divide(new BigInteger(Long.toString(prc.processingTimes.size()))).toString()));
                            mk.setMin(spanMin);
                            mk.setMax(spanMax);
                        }

                        data.get(w).makespans.add(mk);

                        msg = "Shutting down the solution...";
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        msg = "Stopping workers...";
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        exec.stop();

                        msg = "System stopped!";
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        msg = "Cleaning up heap...";
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        doc = null;
                        e1 = null;
                        e2 = null;
                        e3 = null;
                        e4 = null;
                        exec = null;
                        prc = null;
                        pusher = null;
                        messages = null;

                        cleanHeap();
                        long end = System.currentTimeMillis();

                        msg = "System run for -----------------> " + df.format((end - start) / 1000f / 60f) + " min (Actual time)";
                        System.out.println(msg);
                        TextWriter.writeString2File(msg, summaryFile, true);

                        System.out.println("-----------------------------------------------------------------");
                    }

                }

                msg = "THREADS; TOTAL MESSAGES; AVERAGE MESSAGES; TOTAL WORK-UNITS; AVERAGE WORK-UNITS ";
                TextWriter.writeString2File(msg, STATS_FOLDER + "messages-workunits-" + r + ".txt", true);

                for (Integer worker : data.keySet()) {
                    msg = worker + ";" + data.get(worker).getTotalMessages() + ";" + data.get(worker).getAverageMessagesRepetitions() + ";" + data.get(worker).getTotalWorkUnits() + ";" + data.get(worker).getAverageWokUnitsRepetitions();
                    TextWriter.writeString2File(msg, STATS_FOLDER + "messages-workunits-" + r + ".txt", true);
                }

                for (Integer worker : data.keySet()) {
                    msg = "THREADS; MESSAGES; WORK-UNITS";
                    TextWriter.writeString2File(msg, STATS_FOLDER + "messages-workunits-" + r + ".txt", true);

                    for (int t = 0; t < data.get(worker).messages.size(); t++) {
                        msg = worker + ";" + data.get(worker).messages.get(t) + ";" + data.get(worker).workunits.get(t);
                        TextWriter.writeString2File(msg, STATS_FOLDER + "messages-workunits-" + r + ".txt", true);
                    }
                    TextWriter.writeString2File("\n", STATS_FOLDER + "messages-workunits-" + r + ".txt", true);
                }

                /* DUMP MAKESPAN */
                msg = "THREAD; AVERAGE-REPETITIONS; MIN-REPETITIONS; MAX-REPETITIONS (nanoseconds)";
                TextWriter.writeString2File(msg, STATS_FOLDER + "makespan-" + r + ".txt", true);

                for (Integer worker : data.keySet()) {
                    String avg = Long.toString(data.get(worker).getAverageMakespanRepetitions());
                    String min = Long.toString(data.get(worker).getAverageMakespanMinRepetitions());
                    String max = Long.toString(data.get(worker).getAverageMakespanMaxRepetitions());

                    msg = worker + ";" + avg + ";" + min + ";" + max;
                    TextWriter.writeString2File(msg, STATS_FOLDER + "makespan-" + r + ".txt", true);
                }

                for (Integer worker : data.keySet()) {
                    msg = "THREAD; REPETITION; AVERAGE; MIN; MAX (nanoseconds)";
                    TextWriter.writeString2File(msg, STATS_FOLDER + "makespan-" + r + ".txt", true);

                    for (int t = 0; t < data.get(worker).makespans.size(); t++) {
                        String avg = Long.toString(data.get(worker).makespans.get(t).getAverage());
                        String min = Long.toString(data.get(worker).makespans.get(t).getMin());
                        String max = Long.toString(data.get(worker).makespans.get(t).getMax());

                        msg = worker + ";" + (t + 1) + ";" + avg + ";" + min + ";" + max;
                        TextWriter.writeString2File(msg, STATS_FOLDER + "makespan-" + r + ".txt", true);

                    }
                    TextWriter.writeString2File("\n", STATS_FOLDER + "makespan-" + r + ".txt", true);
                }

            }
        } catch (Exception ex) {
            ex.printStackTrace();
            TextWriter.writeString2File(ex.toString(), STATS_FOLDER + "_ERROR.txt", true);
        }
    }

    private void cleanHeap() throws InterruptedException {
        System.runFinalization();
        System.gc();
        try {
            Thread.sleep(1 * 30 * 1000);
        } catch (InterruptedException ex) {
            throw ex;
        }
    }

    private ArrayList<Message<Document>> buildMessages(long messages) {
        ArrayList<Message<Document>> result = new ArrayList<Message<Document>>();

        long start = System.currentTimeMillis();
        for (int i = 0; i < messages; i++) {
            Document docX1 = XMLHandler.readXmlFile("./order-template.xml");
            docX1.getElementsByTagName("OrderId").item(0).setTextContent(UUID.randomUUID().toString());
            Message<Document> m = new Message<Document>();
            m.setBody(docX1);
            result.add(m);
        }
        long end = System.currentTimeMillis();

        DecimalFormat df = new DecimalFormat("####.######");
        System.out.println(">>> Time to build messages: " + df.format(((end - start) / 1000f / 60f)) + " min");

        return result;

    }

}
