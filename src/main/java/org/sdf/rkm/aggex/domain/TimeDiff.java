package org.sdf.rkm.aggex.domain;

import java.io.Serializable;

public class TimeDiff implements Serializable {
    private long epochSeconds;
    private double differenceFromAverage;

    public TimeDiff() {}

    public TimeDiff(long epochSeconds, double differenceFromAverage) {
        this.epochSeconds = epochSeconds;
        this.differenceFromAverage = differenceFromAverage;
    }

    public long getEpochSeconds() {
        return epochSeconds;
    }

    public double getDifferenceFromAverage() {
        return differenceFromAverage;
    }
}
