package org.muks.insider.utils;

public enum SparkDbConnectorTypes {
    CASSANDRA("org.apache.spark.sql.cassandra");

    private final String text;

    /**
     * @param text
     */
    private SparkDbConnectorTypes(final String text) {
        this.text = text;
    }

    /* (non-Javadoc)
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString() {
        return text;
    }
}
