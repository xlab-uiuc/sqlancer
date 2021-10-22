package sqlancer.sparksql;

import java.util.Optional;

import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public final class SparkSQLCompoundDataType {

    private final SparkSQLDataType dataType;
    private final SparkSQLCompoundDataType elemType;
    private final Integer size;

    private SparkSQLCompoundDataType(SparkSQLDataType dataType, SparkSQLCompoundDataType elemType, Integer size) {
        this.dataType = dataType;
        this.elemType = elemType;
        this.size = size;
    }

    public SparkSQLDataType getDataType() {
        return dataType;
    }

    public SparkSQLCompoundDataType getElemType() {
        if (elemType == null) {
            throw new AssertionError();
        }
        return elemType;
    }

    public Optional<Integer> getSize() {
        if (size == null) {
            return Optional.empty();
        } else {
            return Optional.of(size);
        }
    }

    public static SparkSQLCompoundDataType create(SparkSQLDataType type, int size) {
        return new SparkSQLCompoundDataType(type, null, size);
    }

    public static SparkSQLCompoundDataType create(SparkSQLDataType type) {
        return new SparkSQLCompoundDataType(type, null, null);
    }
}