package gca.main;

import java.util.ArrayList;

public class DataSet {

    public ArrayList<Long> messages = new ArrayList<>();
    public ArrayList<Long> workunits = new ArrayList<>();
    public ArrayList<Makespan> makespans = new ArrayList<>();

    private long getMakespanAverageRepetitions() {
        long total = 0;
        for (Makespan n : makespans) {
            total = total + n.getAverage();
        }
        return total;
    }

    private long getMakespanMinRepetitions() {
        long total = 0;
        for (Makespan n : makespans) {
            total = total + n.getMin();
        }
        return total;
    }

    private long getMakespanMaxRepetitions() {
        long total = 0;
        for (Makespan n : makespans) {
            total = total + n.getMax();
        }
        return total;
    }

    public long getTotalWorkUnits() {
        long total = 0;
        for (Long n : workunits) {
            total = total + n;
        }
        return total;
    }

    public long getTotalMessages() {
        long total = 0;
        for (Long n : messages) {
            total = total + n;
        }
        return total;
    }

    public long getAverageMakespanRepetitions() {
        if (makespans.size() > 0) {
            return getMakespanAverageRepetitions() / makespans.size();
        }
        return 0L;
    }

    public long getAverageMakespanMinRepetitions() {
        if (makespans.size() > 0) {
            return getMakespanMinRepetitions() / makespans.size();
        }
        return 0L;
    }

    public long getAverageMakespanMaxRepetitions() {
        if (makespans.size() > 0) {
            return getMakespanMaxRepetitions() / makespans.size();
        }
        return 0L;
    }

    public long getAverageMessagesRepetitions() {
        if (messages.size() > 0) {
            return getTotalMessages() / messages.size();
        }
        return 0L;
    }

    public long getAverageWokUnitsRepetitions() {
        if (workunits.size() > 0) {
            return getTotalWorkUnits() / workunits.size();
        }
        return 0L;
    }

}
