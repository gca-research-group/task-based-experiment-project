package gca.cafe.solution;

import guarana.framework.message.Exchange;
import guarana.framework.message.Message;
import guarana.framework.port.EntryPort;
import guarana.framework.port.ExitPort;
import guarana.framework.port.OneWayPort;
import guarana.framework.port.SolicitorPort;
import guarana.framework.port.TwoWayPort;
import guarana.framework.process.Process;
import guarana.framework.task.Slot;
import guarana.framework.task.Task;
import guarana.framework.task.TaskExecutionException;
import guarana.toolkit.task.communicators.dummy.InDummyCommunicator;
import guarana.toolkit.task.communicators.dummy.OutDummyCommunicator;
import guarana.toolkit.task.communicators.dummy.OutInDummyCommunicator;
import guarana.toolkit.task.modifiers.ContextBasedContentEnricher;
import guarana.toolkit.task.routers.Correlator;
import guarana.toolkit.task.routers.Dispatcher;
import guarana.toolkit.task.routers.Merger;
import guarana.toolkit.task.routers.Replicator;
import guarana.toolkit.task.transformers.Aggregator;
import guarana.toolkit.task.transformers.Splitter;
import guarana.toolkit.task.transformers.Translator;
import guarana.util.xml.XMLHandler;

import java.util.List;
import java.util.Vector;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class IntProcess extends Process {

    private Slot[] slot;
    private Task[] task;
    private OneWayPort entryPortOrders, exitPortWaiter;
    private TwoWayPort solicitorPortCD, solicitorPortHD;

    public InDummyCommunicator communicatorEntry;
    public long ordersServed;
    public List<Long> processingTimes;

    public IntProcess() {
        super("Integration Process");
        processingTimes = new Vector<>();

        slot = new Slot[14];
        for (int i = 0; i < slot.length; i++) {
            slot[i] = new Slot("Slot " + i);
        }

        task = new Task[12];

        // **** Entry Port - Orders		
        entryPortOrders = new EntryPort("Entry Port Orders") {
            @Override
            public void initialise() {
                setInterSlot(new Slot("InterSlot"));

                // **** Communicator
                communicatorEntry = new InDummyCommunicator("Communicator@Prt-Orders@Cafe");
                communicatorEntry.output[0].bind(getInterSlot());
                setCommunicator(communicatorEntry);
            }
        };
        addPort(entryPortOrders);

        // **** Solicitor Port - Cold Drinks
        solicitorPortCD = new SolicitorPort("Solicitor Port CD") {
            @Override
            public void initialise() {
                setInterSlotIn(new Slot("InterSlot In"));
                setInterSlotOut(new Slot("InterSlot Out"));
		
                Task communicator = new OutInDummyCommunicator("Port CD") {
                    @SuppressWarnings("unchecked")
                    @Override
                    public void doWork(Exchange exchange) throws TaskExecutionException {
                        Message<Document> request = (Message<Document>) exchange.input[0].poll();

                        Document docX3 = (Document) request.getBody();
                        Document docX4 = XMLHandler.newDocument();

                        // <DrinkResponse>
                        Element root = docX4.createElement("DrinkResponse");
                        docX4.appendChild(root);

                        // <ReplyTo>
                        Element replyTo = docX4.createElement("ReplyTo");
                        replyTo.setTextContent(docX3.getElementsByTagName("RequestId").item(0).getTextContent());
                        root.appendChild(replyTo);

                        // <Status>
                        Element status = docX4.createElement("Status");
                        status.setTextContent("Ready");
                        root.appendChild(status);

                        Message<Document> outMsg = new Message<Document>(request);
                        outMsg.setBody(docX4);

                        exchange.output[0].add(outMsg);
                    }
                };
                communicator.input[0].bind(getInterSlotIn());
                communicator.output[0].bind(getInterSlotOut());
                setCommunicator(communicator);
            }
        };
        addPort(solicitorPortCD);

        // **** Solicitor Port - Hot Drinks
        solicitorPortHD = new SolicitorPort("Solicitor Port HD") {
            @Override
            public void initialise() {
                setInterSlotIn(new Slot("InterSlot In"));
                setInterSlotOut(new Slot("InterSlot Out"));

                Task communicator = new OutInDummyCommunicator("Port HD") {
                    @SuppressWarnings("unchecked")
                    @Override
                    public void doWork(Exchange exchange) throws TaskExecutionException {
                        Message<Document> request = (Message<Document>) exchange.input[0].poll();

                        Document docX3 = (Document) request.getBody();
                        Document docX4 = XMLHandler.newDocument();

                        // <DrinkResponse>
                        Element root = docX4.createElement("DrinkResponse");
                        docX4.appendChild(root);

                        // <ReplyTo>
                        Element replyTo = docX4.createElement("ReplyTo");
                        replyTo.setTextContent(docX3.getElementsByTagName("RequestId").item(0).getTextContent());
                        root.appendChild(replyTo);

                        // <Status>
                        Element status = docX4.createElement("Status");
                        status.setTextContent("Ready");
                        root.appendChild(status);

                        Message<Document> outMsg = new Message<Document>(request);
                        outMsg.setBody(docX4);

                        exchange.output[0].add(outMsg);
                    }
                };
                communicator.input[0].bind(getInterSlotIn());
                communicator.output[0].bind(getInterSlotOut());
                setCommunicator(communicator);
            }
        };
        addPort(solicitorPortHD);

        // **** Exit Port - Waiter		
        exitPortWaiter = new ExitPort("Exit Port Waiter") {
            @Override
            public void initialise() {
                setInterSlot(new Slot("InterSlot"));

                // **** Communicator - File Adapter
                Task communicator = new OutDummyCommunicator("Communicator@Prt-Waiter@Cafe") {
                    @Override
                    public void execute() throws TaskExecutionException {
                        Message<?> inputMsg = input[0].getMessage();
                        long exit = System.nanoTime();

                        long enter = (long) inputMsg.getHeader().getDynamicAttribute("enter");

                        processingTimes.add((exit - enter));
                        // END makespan

                        ordersServed++;
                    }
                };
                communicator.input[0].bind(getInterSlot());
                setCommunicator(communicator);
            }
        };
        addPort(exitPortWaiter);

        // ----------------- TASKS
        // **** Splitter
        task[0] = new Splitter("Splitter t0") {
            @SuppressWarnings("unchecked")
            @Override
            public void doWork(Exchange exchange) throws TaskExecutionException {
                Message<Document> inMsg = (Message<Document>) exchange.input[0].poll();

                Document docX1 = inMsg.getBody();

                String orderId = docX1.getElementsByTagName("OrderId").item(0).getTextContent();

                NodeList drinks = docX1.getElementsByTagName("Drink");

                for (int i = 0; i < drinks.getLength(); i++) {
                    Element d = (Element) drinks.item(i);

                    Document docX2 = XMLHandler.newDocument();

                    // <Drink>
                    Element rootX2 = docX2.createElement("Drink");
                    docX2.appendChild(rootX2);

                    // <OrderDrinkId>
                    Element id = docX2.createElement("OrderDrinkId");
                    id.setTextContent(orderId + "#" + i);
                    rootX2.appendChild(id);

                    // <Name>
                    Element name = docX2.createElement("Name");
                    name.setTextContent(d.getElementsByTagName("Name").item(0).getTextContent());
                    rootX2.appendChild(name);

                    // <Type>
                    Element type = docX2.createElement("Type");
                    type.setTextContent(d.getElementsByTagName("Type").item(0).getTextContent());
                    rootX2.appendChild(type);

                    // <Status>
                    Element status = docX2.createElement("Status");
                    rootX2.appendChild(status);

                    Message<Document> outMsg = new Message<Document>(inMsg);
                    outMsg.setBody(docX2);
                    exchange.output[0].add(outMsg);
                }

            }
        };
        task[0].input[0].bind(entryPortOrders.getInterSlot());
        task[0].output[0].bind(slot[0]);
        addTask(task[0]);

        // **** Dispatcher
        task[1] = new Dispatcher("Dispatcher t1", 2) {
            @SuppressWarnings("unchecked")
            @Override
            public void doWork(Exchange exchange) throws TaskExecutionException {
                Message<Document> inMsg = (Message<Document>) exchange.input[0].poll();

                Document docX2 = inMsg.getBody();
                String type = docX2.getElementsByTagName("Type").item(0).getTextContent();

                if (type.equalsIgnoreCase("Cold")) {
                    exchange.output[0].add(inMsg);
                } else {
                    exchange.output[1].add(inMsg);
                }

            }
        };
        task[1].input[0].bind(slot[0]);
        task[1].output[0].bind(slot[1]); // Cold Drinks
        task[1].output[1].bind(slot[2]); // Hot Drinks
        addTask(task[1]);

        // **** Replicator
        task[2] = new Replicator("Replicator t2", 2);
        task[2].input[0].bind(slot[1]);
        task[2].output[0].bind(slot[3]);
        task[2].output[1].bind(slot[4]);
        addTask(task[2]);

        // **** Translator
        task[3] = new Translator("Translator t3") {
            @SuppressWarnings("unchecked")
            @Override
            public void doWork(Exchange exchange) throws TaskExecutionException {
                Message<Document> inMsg = (Message<Document>) exchange.input[0].poll();

                Document docX2 = inMsg.getBody();
                Document docX3 = XMLHandler.newDocument();

                // <DrinkRequest>
                Element root = docX3.createElement("DrinkRequest");
                docX3.appendChild(root);

                // <RequestId>
                Element requestId = docX3.createElement("RequestId");
                requestId.setTextContent(docX2.getElementsByTagName("OrderDrinkId").item(0).getTextContent());
                root.appendChild(requestId);

                // <Name>
                Element name = docX3.createElement("Name");
                name.setTextContent(docX2.getElementsByTagName("Name").item(0).getTextContent());
                root.appendChild(name);

                Message<Document> outMsg = new Message<Document>(inMsg);
                outMsg.setBody(docX3);
                exchange.output[0].add(outMsg);
            }
        };
        task[3].input[0].bind(slot[3]);
        task[3].output[0].bind(solicitorPortCD.getInterSlotIn());
        addTask(task[3]);

        // **** Correlator
        task[4] = new Correlator("Correlator t4", 2, 2) {
            @SuppressWarnings("unchecked")
            @Override
            public void doWork(Exchange exchange) throws TaskExecutionException {
                Message<Document> inMsg0 = (Message<Document>) exchange.input[0].poll();
                Message<Document> inMsg1 = (Message<Document>) exchange.input[1].poll();

                Document docX4 = inMsg0.getBody();
                String replyTo = docX4.getElementsByTagName("ReplyTo").item(0).getTextContent();

                Document docX2 = inMsg1.getBody();
                String orderDrinkId = docX2.getElementsByTagName("OrderDrinkId").item(0).getTextContent();

                if (replyTo.equals(orderDrinkId)) {
                    exchange.output[0].add(inMsg0);
                    exchange.output[1].add(inMsg1);
                }
            }
        };
        task[4].input[0].bind(solicitorPortCD.getInterSlotOut());
        task[4].input[1].bind(slot[4]);
        task[4].output[0].bind(slot[5]);
        task[4].output[1].bind(slot[6]);
        addTask(task[4]);

        // **** Context Content Enricher
        task[5] = new ContextBasedContentEnricher("ContextContentEnricher t5") {
            @SuppressWarnings("unchecked")
            @Override
            public void doWork(Exchange exchange) throws TaskExecutionException {
                Message<Document> inMsg0 = (Message<Document>) exchange.input[0].poll(); // Context
                Message<Document> inMsg1 = (Message<Document>) exchange.input[1].poll();

                Document docX4 = inMsg0.getBody();
                Document docX2 = inMsg1.getBody();

                docX2.getElementsByTagName("Status").item(0).setTextContent(docX4.getElementsByTagName("Status").item(0).getTextContent());

                exchange.output[0].add(inMsg1);
            }
        };
        task[5].input[0].bind(slot[5]);
        task[5].input[1].bind(slot[6]);
        task[5].output[0].bind(slot[7]);
        addTask(task[5]);

        // **** Replicator
        task[6] = new Replicator("Replicator t6", 2);
        task[6].input[0].bind(slot[2]);
        task[6].output[0].bind(slot[8]);
        task[6].output[1].bind(slot[9]);
        addTask(task[6]);

        // **** Translator
        task[7] = new Translator("Translator t7") {
            @SuppressWarnings("unchecked")
            @Override
            public void doWork(Exchange exchange) throws TaskExecutionException {
                Message<Document> inMsg = (Message<Document>) exchange.input[0].poll();

                Document docX2 = inMsg.getBody();
                Document docX3 = XMLHandler.newDocument();

                // <DrinkRequest>
                Element root = docX3.createElement("DrinkRequest");
                docX3.appendChild(root);

                // <RequestId>
                Element requestId = docX3.createElement("RequestId");
                requestId.setTextContent(docX2.getElementsByTagName("OrderDrinkId").item(0).getTextContent());
                root.appendChild(requestId);

                // <Name>
                Element name = docX3.createElement("Name");
                name.setTextContent(docX2.getElementsByTagName("Name").item(0).getTextContent());
                root.appendChild(name);

                Message<Document> outMsg = new Message<Document>(inMsg);
                outMsg.setBody(docX3);
                exchange.output[0].add(outMsg);
            }
        };
        task[7].input[0].bind(slot[9]);
        task[7].output[0].bind(solicitorPortHD.getInterSlotIn());
        addTask(task[7]);

        // **** Correlator
        task[8] = new Correlator("Correlator t8", 2, 2) {
            @SuppressWarnings("unchecked")
            @Override
            public void doWork(Exchange exchange) throws TaskExecutionException {
                Message<Document> inMsg0 = (Message<Document>) exchange.input[0].poll();
                Message<Document> inMsg1 = (Message<Document>) exchange.input[1].poll();

                Document docX4 = inMsg1.getBody();
                String replyTo = docX4.getElementsByTagName("ReplyTo").item(0).getTextContent();

                Document docX2 = inMsg0.getBody();
                String orderDrinkId = docX2.getElementsByTagName("OrderDrinkId").item(0).getTextContent();

                if (replyTo.equals(orderDrinkId)) {
                    exchange.output[0].add(inMsg0);
                    exchange.output[1].add(inMsg1);
                }
            }
        };
        task[8].input[0].bind(slot[8]);
        task[8].input[1].bind(solicitorPortHD.getInterSlotOut());
        task[8].output[0].bind(slot[10]);
        task[8].output[1].bind(slot[11]);
        addTask(task[8]);

        // **** Context Content Enricher
        task[9] = new ContextBasedContentEnricher("ContextContentEnricher t9") {
            @SuppressWarnings("unchecked")
            @Override
            public void doWork(Exchange exchange) throws TaskExecutionException {
                Message<Document> inMsg0 = (Message<Document>) exchange.input[0].poll();
                Message<Document> inMsg1 = (Message<Document>) exchange.input[1].poll(); // Context

                Document docX4 = inMsg1.getBody();
                Document docX2 = inMsg0.getBody();

                docX2.getElementsByTagName("Status").item(0).setTextContent(docX4.getElementsByTagName("Status").item(0).getTextContent());

                exchange.output[0].add(inMsg0);
            }
        };
        task[9].input[0].bind(slot[10]);
        task[9].input[1].bind(slot[11]);
        task[9].output[0].bind(slot[12]);
        addTask(task[9]);

        // **** Merger
        task[10] = new Merger("Merger t10", 2);
        task[10].input[0].bind(slot[7]);
        task[10].input[1].bind(slot[12]);
        task[10].output[0].bind(slot[13]);
        addTask(task[10]);

        // **** Aggregator		
        task[11] = new Aggregator("Aggregator t11") {
            @SuppressWarnings("unchecked")
            @Override
            public void doWork(Exchange exchange) throws TaskExecutionException {

                Document docX5 = XMLHandler.newDocument();

                // <CafeDeliver>
                Element root = docX5.createElement("CafeDeliver");
                docX5.appendChild(root);

                // <OrderId>
                Element orderId = docX5.createElement("OrderId");
                root.appendChild(orderId);

                // <Drinks>
                Element drinks = docX5.createElement("Drinks");
                root.appendChild(drinks);

                Message<Document> outMsg = new Message<Document>();

                Document docX2 = null;
                long time = 0;
                while (!exchange.input[0].isEmpty()) {

                    Message<Document> m = (Message<Document>) exchange.input[0].poll();

                    time = (long) m.getHeader().getDynamicAttribute("enter");

                    outMsg.addParent(m);

                    docX2 = m.getBody();

                    // <Drink>
                    Element d = docX5.createElement("Drink");
                    drinks.appendChild(d);

                    // <Name>
                    Element n = docX5.createElement("Name");
                    n.setTextContent(docX2.getElementsByTagName("Name").item(0).getTextContent());
                    d.appendChild(n);

                    // <Type>
                    Element t = docX5.createElement("Type");
                    t.setTextContent(docX2.getElementsByTagName("Type").item(0).getTextContent());
                    d.appendChild(t);

                    // <Status>
                    Element s = docX5.createElement("Status");
                    s.setTextContent(docX2.getElementsByTagName("Status").item(0).getTextContent());
                    d.appendChild(s);
                }

                // <OrderId> value
                String[] id = docX2.getElementsByTagName("OrderDrinkId").item(0).getTextContent().split("#");
                orderId.setTextContent(id[0]);

                outMsg.getHeader().addDynamicAttribute("enter", time);

                outMsg.setBody(docX5);
                exchange.output[0].add(outMsg);

            }
        };
        task[11].input[0].bind(slot[13]);
        task[11].output[0].bind(exitPortWaiter.getInterSlot());
        addTask(task[11]);

    }

}
