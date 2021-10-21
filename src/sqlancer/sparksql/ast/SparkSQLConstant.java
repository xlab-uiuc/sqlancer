package sqlancer.sparksql.ast;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import java.sql.Date;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
import sqlancer.sparksql.ast.SparkSQLExpression;

import java.math.BigDecimal;

public abstract class SparkSQLConstant implements SparkSQLExpression {

    private SparkSQLConstant() {
    }

    public static class SparkSQLNaNConstant extends SparkSQLConstant {

        @Override
        public String toString() {
            return "NaN";
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLNaNConstant)
                return true;
            else
                return false;
        }

        public boolean isLessThan(SparkSQLConstant rightVal) {
            return false;
        }

    }

    // Numeric types

    public static class SparkSQLByteConstant extends SparkSQLConstant {

        private final byte value;

        public SparkSQLByteConstant(byte value) {
            this.value = value;
        }

        public byte getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value == ((SparkSQLByteConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLShortConstant)
                return value == ((SparkSQLShortConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLIntConstant)
                return value == ((SparkSQLIntConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLLongConstant)
                return value == ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value == ((SparkSQLFloatConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value == ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return ((SparkSQLDecimalConstant) rightVal).getValue().equals(new BigDecimal(value));
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value < ((SparkSQLByteConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLShortConstant)
                return value < ((SparkSQLShortConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLIntConstant)
                return value < ((SparkSQLIntConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLLongConstant)
                return value < ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value < ((SparkSQLFloatConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value < ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return (new BigDecimal(value)).compareTo(((SparkSQLDecimalConstant) rightVal).getValue()) == -1;
            else
                return false;
        }

    }

    public static class SparkSQLShortConstant extends SparkSQLConstant {

        private final short value;

        public SparkSQLShortConstant(short value) {
            this.value = value;
        }

        public short getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value == ((SparkSQLByteConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLShortConstant)
                return value == ((SparkSQLShortConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLIntConstant)
                return value == ((SparkSQLIntConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLLongConstant)
                return value == ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value == ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value == ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return ((SparkSQLDecimalConstant) rightVal).getValue().equals(new BigDecimal(value));
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value < ((SparkSQLByteConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLShortConstant)
                return value < ((SparkSQLShortConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLIntConstant)
                return value < ((SparkSQLIntConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLLongConstant)
                return value < ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value < ((SparkSQLFloatConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value < ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return (new BigDecimal(value)).compareTo(((SparkSQLDecimalConstant) rightVal).getValue()) == -1;
            else
                return false;
        }

    }

    public static class SparkSQLIntConstant extends SparkSQLConstant {

        private final int value;

        public SparkSQLIntConstant(int value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value == ((SparkSQLByteConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLShortConstant)
                return value == ((SparkSQLShortConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLIntConstant)
                return value == ((SparkSQLIntConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLLongConstant)
                return value == ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value == ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value == ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return ((SparkSQLDecimalConstant) rightVal).getValue().equals(new BigDecimal(value));
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value < ((SparkSQLByteConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLShortConstant)
                return value < ((SparkSQLShortConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLIntConstant)
                return value < ((SparkSQLIntConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLLongConstant)
                return value < ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value < ((SparkSQLFloatConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value < ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return (new BigDecimal(value)).compareTo(((SparkSQLDecimalConstant) rightVal).getValue()) == -1;
            else
                return false;
        }

    }

    public static class SparkSQLLongConstant extends SparkSQLConstant {

        private final long value;

        public SparkSQLLongConstant(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value == ((SparkSQLByteConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLShortConstant)
                return value == ((SparkSQLShortConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLIntConstant)
                return value == ((SparkSQLIntConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLLongConstant)
                return value == ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value == ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value == ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return ((SparkSQLDecimalConstant) rightVal).getValue().equals(new BigDecimal(value));
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value < ((SparkSQLByteConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLShortConstant)
                return value < ((SparkSQLShortConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLIntConstant)
                return value < ((SparkSQLIntConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLLongConstant)
                return value < ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value < ((SparkSQLFloatConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value < ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return (new BigDecimal(value)).compareTo(((SparkSQLDecimalConstant) rightVal).getValue()) == -1;
            else
                return false;
        }

    }

    public static class SparkSQLFloatConstant extends SparkSQLConstant {

        private final float value;

        public SparkSQLFloatConstant(float value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Float.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Float.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return String.valueOf(value);
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value == ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLShortConstant)
                return value == ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLIntConstant)
                return value == ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLLongConstant)
                return value == ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value == ((SparkSQLFloatConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value == ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return ((SparkSQLDecimalConstant) rightVal).getValue().equals(new BigDecimal(value));
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value < ((SparkSQLByteConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLShortConstant)
                return value < ((SparkSQLShortConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLIntConstant)
                return value < ((SparkSQLIntConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLLongConstant)
                return value < ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value < ((SparkSQLFloatConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value < ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return (new BigDecimal(value)).compareTo(((SparkSQLDecimalConstant) rightVal).getValue()) == -1;
            else
                return false;
        }

    }

    public static class SparkSQLDoubleConstant extends SparkSQLConstant {

        private final double value;

        public SparkSQLDoubleConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return String.valueOf(value);
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value == ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLShortConstant)
                return value == ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLIntConstant)
                return value == ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLLongConstant)
                return value == ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value == ((SparkSQLFloatConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value == ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return ((SparkSQLDecimalConstant) rightVal).getValue().equals(new BigDecimal(value));
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value < ((SparkSQLByteConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLShortConstant)
                return value < ((SparkSQLShortConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLIntConstant)
                return value < ((SparkSQLIntConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLLongConstant)
                return value < ((SparkSQLLongConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value < ((SparkSQLFloatConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value < ((SparkSQLDoubleConstant) rightVal).getValue();
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return (new BigDecimal(value)).compareTo(((SparkSQLDecimalConstant) rightVal).getValue()) == -1;
            else
                return false;
        }

    }

    public static class SparkSQLDecimalConstant extends SparkSQLConstant {

        private final BigDecimal value;

        public SparkSQLDecimalConstant(String value) {
            this.value = new BigDecimal(value);
        }

        public BigDecimal getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value.equals(new BigDecimal(((SparkSQLByteConstant) rightVal).getValue()));
            else if (rightVal instanceof SparkSQLShortConstant)
                return value.equals(new BigDecimal(((SparkSQLShortConstant) rightVal).getValue()));
            else if (rightVal instanceof SparkSQLIntConstant)
                return value.equals(new BigDecimal(((SparkSQLIntConstant) rightVal).getValue()));
            else if (rightVal instanceof SparkSQLLongConstant)
                return value.equals(new BigDecimal(((SparkSQLLongConstant) rightVal).getValue()));
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value.equals(new BigDecimal(((SparkSQLFloatConstant) rightVal).getValue()));
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value.equals(new BigDecimal(((SparkSQLDoubleConstant) rightVal).getValue()));
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return value.equals(((SparkSQLDecimalConstant) rightVal).getValue());
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLByteConstant)
                return value.compareTo(new BigDecimal(((SparkSQLByteConstant) rightVal).getValue())) == -1;
            else if (rightVal instanceof SparkSQLShortConstant)
                return value.compareTo(new BigDecimal(((SparkSQLShortConstant) rightVal).getValue())) == -1;
            else if (rightVal instanceof SparkSQLIntConstant)
                return value.compareTo(new BigDecimal(((SparkSQLIntConstant) rightVal).getValue())) == -1;
            else if (rightVal instanceof SparkSQLLongConstant)
                return value.compareTo(new BigDecimal(((SparkSQLLongConstant) rightVal).getValue())) == -1;
            else if (rightVal instanceof SparkSQLFloatConstant)
                return value.compareTo(new BigDecimal(((SparkSQLFloatConstant) rightVal).getValue())) == -1;
            else if (rightVal instanceof SparkSQLDoubleConstant)
                return value.compareTo(new BigDecimal(((SparkSQLDoubleConstant) rightVal).getValue())) == -1;
            else if (rightVal instanceof SparkSQLDecimalConstant)
                return value.compareTo(((SparkSQLDecimalConstant) rightVal).getValue()) == -1;
            else
                return false;
        }

    }

    // String types

    public static class SparkSQLStringConstant extends SparkSQLConstant {

        private final String value;

        public SparkSQLStringConstant(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLStringConstant)
                return value.equals(((SparkSQLStringConstant) rightVal).getValue());
            else if (rightVal instanceof SparkSQLVarCharConstant)
                return value.equals(((SparkSQLVarCharConstant) rightVal).getValue());
            else if (rightVal instanceof SparkSQLCharConstant)
                return value.equals(((SparkSQLCharConstant) rightVal).getValue());
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLStringConstant)
                return value.compareTo(((SparkSQLStringConstant) rightVal).getValue()) < 0;
            else if (rightVal instanceof SparkSQLVarCharConstant)
                return value.compareTo(((SparkSQLVarCharConstant) rightVal).getValue()) < 0;
            else if (rightVal instanceof SparkSQLCharConstant)
                return value.compareTo(((SparkSQLCharConstant) rightVal).getValue()) < 0;
            else
                return false;
        }

    }

    public static class SparkSQLVarCharConstant extends SparkSQLConstant {

        private final String value;
        private final int length;

        public SparkSQLVarCharConstant(String value, int length) throws SQLException {
            this.value = value;
            this.length = length;
            if (value.length() > length)
                throw new SQLException();
        }

        public String getValue() {
            return value;
        }

        public int getLength() {
            return length;
        }

        @Override
        public String toString() {
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLStringConstant)
                return value.equals(((SparkSQLStringConstant) rightVal).getValue());
            else if (rightVal instanceof SparkSQLVarCharConstant)
                return value.equals(((SparkSQLVarCharConstant) rightVal).getValue());
            else if (rightVal instanceof SparkSQLCharConstant)
                return value.equals(((SparkSQLCharConstant) rightVal).getValue());
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLStringConstant)
                return value.compareTo(((SparkSQLStringConstant) rightVal).getValue()) < 0;
            else if (rightVal instanceof SparkSQLVarCharConstant)
                return value.compareTo(((SparkSQLVarCharConstant) rightVal).getValue()) < 0;
            else if (rightVal instanceof SparkSQLCharConstant)
                return value.compareTo(((SparkSQLCharConstant) rightVal).getValue()) < 0;
            else
                return false;
        }

    }

    public static class SparkSQLCharConstant extends SparkSQLConstant {

        private final String value;
        private final int length;

        public SparkSQLCharConstant(String value, int length) throws SQLException {
            this.value = value;
            this.length = length;
            if (value.length() != length)
                throw new SQLException();
        }

        public String getValue() {
            return value;
        }

        public int getLength() {
            return length;
        }

        @Override
        public String toString() {
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLStringConstant)
                return value.equals(((SparkSQLStringConstant) rightVal).getValue());
            else if (rightVal instanceof SparkSQLVarCharConstant)
                return value.equals(((SparkSQLVarCharConstant) rightVal).getValue());
            else if (rightVal instanceof SparkSQLCharConstant)
                return value.equals(((SparkSQLCharConstant) rightVal).getValue());
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLStringConstant)
                return value.compareTo(((SparkSQLStringConstant) rightVal).getValue()) < 0;
            else if (rightVal instanceof SparkSQLVarCharConstant)
                return value.compareTo(((SparkSQLVarCharConstant) rightVal).getValue()) < 0;
            else if (rightVal instanceof SparkSQLCharConstant)
                return value.compareTo(((SparkSQLCharConstant) rightVal).getValue()) < 0;
            else
                return false;
        }

    }

    // Binary type

    public static class SparkSQLBinaryConstant extends SparkSQLConstant {

        private final byte[] value;

        public SparkSQLBinaryConstant(byte[] input) {
            this.value = input.clone();
        }

        public byte[] getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLBinaryConstant) {
                byte[] temp = ((SparkSQLBinaryConstant) rightVal).getValue();
                if (temp.length != value.length)
                    return false;
                for (int i = 0; i < value.length; i ++) {
                    if (value[i] != temp[i])
                        return false;
                }
                return true;
            }
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLBinaryConstant) {
                byte[] temp = ((SparkSQLBinaryConstant) rightVal).getValue();
                int N = value.length;
                if (temp.length < N)
                    N = temp.length;
                for (int i = 0; i < N; i ++) {
                    if (value[i] < temp[i])
                        return true;
                    else if (value[i] > temp[i])
                        return false;
                }
                if (value.length < temp.length)
                    return true;
                return false;
            }
            else
                return false;
        }

    }

    // Boolean type

    public static class SparkSQLBooleanConstant extends SparkSQLConstant {

        private final boolean value;

        public SparkSQLBooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLBooleanConstant) {
                return value == ((SparkSQLBooleanConstant) rightVal).getValue();
            }
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            // this is not meaningful
            return false;
        }

    }

    // Datetime types

    public static class SparkSQLTimestampConstant extends SparkSQLConstant {

        public String textRepr;

        public SparkSQLTimestampConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s'", textRepr);
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLTimestampConstant) {
                return textRepr.equals(((SparkSQLTimestampConstant) rightVal).getValue());
            }
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLTimestampConstant) {
                return textRepr.compareTo(((SparkSQLTimestampConstant) rightVal).getValue()) < 0;
            }
            else
                return false;
        }

    }

    public static class SparkSQLDateConstant extends SparkSQLConstant {

        public String textRepr;

        public SparkSQLDateConstant(long val) {
            Date timestamp = new Date(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("DATE '%s'", textRepr);
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLDateConstant) {
                return textRepr == ((SparkSQLDateConstant) rightVal).getValue();
            }
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLTimestampConstant) {
                return textRepr.compareTo(((SparkSQLTimestampConstant) rightVal).getValue()) < 0;
            }
            else
                return false;
        }

    }

    // Complex types

    public static class SparkSQLArrayConstant<T>  extends SparkSQLConstant {

        List<T> array;

        public SparkSQLArrayConstant(List<T> array) {
            this.array = new ArrayList<T>();
            this.array.addAll(array);
        }

        public List<T> getValue() {
            return array;
        }

        @Override
        public String toString() {
            return array.toString();
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLArrayConstant<?>) {
                List<T> temp = ((SparkSQLArrayConstant<T>) rightVal).getValue();
                if (temp.size() != array.size())
                    return false;
                for (int i = 0; i < array.size(); i ++) {
                    if (array.get(i) != temp.get(i))
                        return false;
                }
                return true;
            }
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            // this is not meaningful
            return false;
        }

    }

    public static class SparkSQLMapConstant<T,S>  extends SparkSQLConstant {

        Map<T, S> map;

        public SparkSQLMapConstant(Map<T, S> map) {
            this.map = new HashMap<T, S>();
            this.map.putAll(map);
        }

        public Map<T, S> getValue() {
            return map;
        }

        @Override
        public String toString() {
            return map.toString();
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLMapConstant<?,?>) {
                Map<T,S> temp = ((SparkSQLMapConstant<T,S>) rightVal).getValue();
                if (temp.size() != map.size())
                    return false;
                for (Map.Entry<T,S> e: map.entrySet()) {
                    if (!temp.containsKey(e.getKey()) || !temp.get(e.getKey()).equals(e.getValue())) {
                        return false;
                    }
                }
                return true;
            }
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            // this is not meaningful
            return false;
        }

    }

    public static class SparkSQLStructConstant<T>  extends SparkSQLConstant {

        Map<String, T> map;

        public SparkSQLStructConstant(List<SparkSQLStructFieldConstant<T>> fields) {
            map = new HashMap<>();
            for (int i = 0; i < fields.size(); i ++) {
                map.put(fields.get(i).getName(), fields.get(i).getValue());
            }
        }

        public Map<String, T> getValue() {
            return map;
        }

        @Override
        public String toString() {
            return map.toString();
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLMapConstant<?,?>) {
                Map<String,T> temp = ((SparkSQLStructConstant<T>) rightVal).getValue();
                if (temp.size() != map.size())
                    return false;
                for (Map.Entry<String,T> e: map.entrySet()) {
                    if (!temp.containsKey(e.getKey()) || !temp.get(e.getKey()).equals(e.getValue())) {
                        return false;
                    }
                }
                return true;
            }
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            // this is not meaningful
            return false;
        }

    }

    public static class SparkSQLStructFieldConstant<T>  extends SparkSQLConstant {

        String name;
        T value;

        public SparkSQLStructFieldConstant(String name, T value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public T getValue() {
            return value;
        }

        @Override
        public String toString() {
            return name.toString() + ": " + value.toString();
        }

        @Override
        public boolean equals(SparkSQLConstant rightVal) {
            if (rightVal instanceof SparkSQLStructFieldConstant<?>) {
                return (name == ((SparkSQLStructFieldConstant<T>) rightVal).getName() && value == ((SparkSQLStructFieldConstant<T>) rightVal).getValue());
            }
            else
                return false;
        }

        @Override
        public boolean isLessThan(SparkSQLConstant rightVal) {
            // this is not meaningful
            return false;
        }

    }

    // TODO: implement constant classes

    public abstract boolean equals(SparkSQLConstant rightVal);

    // public abstract SparkSQLConstant castAs(CastType type);

    // public abstract String castAsString();

    // public static SparkSQLConstant createStringConstant(String string) {
    //     return new SparkSQLTextConstant(string);
    // }

    public String getType() {
        return ((Object) this).getClass().getSimpleName();
    }

    public abstract boolean isLessThan(SparkSQLConstant rightVal);

}
