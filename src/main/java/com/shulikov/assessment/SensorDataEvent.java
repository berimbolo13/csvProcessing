package com.shulikov.assessment;

import org.apache.spark.sql.Row;

import java.io.Serializable;

public class SensorDataEvent implements Serializable {

    private String timestamp;

    private String sensorIdentifier;

    private int validationValue;

    public static SensorDataEvent of(Row row) {
        return new SensorDataEvent(
                row.getString(0),
                row.getString(2),
                resolveValidationValue(row.getDouble(5)));
    }

    public SensorDataEvent() {
    }

    public SensorDataEvent(String timestamp, String sensorIdentifier, int validationValue) {
        this.timestamp = timestamp;
        this.sensorIdentifier = sensorIdentifier;
        this.validationValue = validationValue;
    }

    public String getSensorIdentifier() {
        return sensorIdentifier;
    }

    public void setSensorIdentifier(String sensorIdentifier) {
        this.sensorIdentifier = sensorIdentifier;
    }

    public int getValidationValue() {
        return validationValue;
    }

    public void setValidationValue(int validationValue) {
        this.validationValue = validationValue;
    }


    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    private static int resolveValidationValue(double valueNominal) {
        return 0 <= valueNominal && valueNominal <= 100 ? 1 : 0;
    }

    @Override
    public String toString() {
        return "SensorDataEvent{" +
                "timestamp='" + timestamp + '\'' +
                ", sensorIdentifier='" + sensorIdentifier + '\'' +
                ", validationValue=" + validationValue +
                '}';
    }
}

