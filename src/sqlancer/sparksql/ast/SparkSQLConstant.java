package sqlancer.sparksql.ast;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.RowFactory;

import sqlancer.IgnoreMeException;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public abstract class SparkSQLConstant implements SparkSQLExpression {

    public abstract String getTextRepresentation();

    public abstract String getUnquotedTextRepresentation();

    public abstract SparkSQLConstant isEquals(SparkSQLConstant rightVal);

    protected abstract SparkSQLConstant isLessThan(SparkSQLConstant rightVal);

    public abstract SparkSQLConstant cast(SparkSQLDataType type);

//    public static class SparkSQLNaNConstant extends SparkSQLConstant {
//
//        @Override
//        public String getTextRepresentation() {
//            return "NaN";
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.NAN;
//        }
//
//        @Override
//        public boolean isNaN() {
//            return true;
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isNaN()) {
//                return SparkSQLConstant.createTrue();
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            return SparkSQLConstant.createFalse();
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
//            return SparkSQLConstant.createNaNConstant();
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return getTextRepresentation();
//        }
//
//    }

    public static class SparkSQLNullConstant extends SparkSQLConstant {

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public SparkSQLDataType getExpressionType() {
            return null;
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
            return SparkSQLConstant.createNullConstant();
        }

        @Override
        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
            return SparkSQLConstant.createNullConstant();
        }

        @Override
        public SparkSQLConstant cast(SparkSQLDataType type) {
            return SparkSQLConstant.createNullConstant();
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    // Numeric types

//    public static class SparkSQLByteConstant extends SparkSQLConstant {
//
//        private final byte val;
//
//        public SparkSQLByteConstant(byte val) {
//            this.val = val;
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.valueOf(val);
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.BYTE;
//        }
//
//        @Override
//        public byte asByte() {
//            return val;
//        }
//
//        @Override
//        public boolean isByte() {
//            return true;
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isNaN()) {
//                return SparkSQLConstant.createFalse();
//            } else if (rightVal.isBoolean()) {
//                return cast(SparkSQLDataType.BOOLEAN).isEquals(rightVal);
//            } else if (rightVal.isByte()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asByte());
//            } else if (rightVal.isShort()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asShort());
//            } else if (rightVal.isInt()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asInt());
//            } else if (rightVal.isLong()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asLong());
//            } else if (rightVal.isFloat()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asFloat());
//            } else if (rightVal.isDouble()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asDouble());
//            } else if (rightVal.isDecimal()) {
//                return SparkSQLConstant.createBooleanConstant(rightVal.asDecimal().equals(new BigDecimal(val)));
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.INT).asLong());
////            } else if (rightVal.isVarChar()) {
////                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.LONG).asLong());
////            } else if (rightVal.isChar()) {
////                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.LONG).asLong());
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isNaN()) {
//                return SparkSQLConstant.createTrue();
//            } else if (rightVal.isByte()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asByte());
//            } else if (rightVal.isShort()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asShort());
//            } else if (rightVal.isInt()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asInt());
//            } else if (rightVal.isLong()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asLong());
//            } else if (rightVal.isFloat()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asFloat());
//            } else if (rightVal.isDouble()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asDouble());
//            } else if (rightVal.isDecimal()) {
//                return SparkSQLConstant.createBooleanConstant(rightVal.asDecimal().compareTo(new BigDecimal(val)) == 1);
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.LONG).asLong());
//            } else if (rightVal.isVarChar()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.LONG).asLong());
//            } else if (rightVal.isChar()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.LONG).asLong());
//            } else if (rightVal.isBoolean()) {
//                throw new AssertionError(rightVal);
//            } else {
//                throw new IgnoreMeException();
//            }
//
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
//            switch (type) {
//            case BOOLEAN:
//                return SparkSQLConstant.createBooleanConstant(val != 0);
////            case BYTE:
////                return this;
////            case SHORT:
////                return SparkSQLConstant.createShortConstant(val);
//            case INT:
//                return SparkSQLConstant.createIntConstant(val);
////            case LONG:
////                return SparkSQLConstant.createLongConstant(val);
//            case FLOAT:
//                return SparkSQLConstant.createFloatConstant(val);
//            case DOUBLE:
//                return SparkSQLConstant.createDoubleConstant(val);
//            case DECIMAL:
//                return SparkSQLConstant.createDecimalConstant(new BigDecimal(val));
//            case STRING:
//                return SparkSQLConstant.createStringConstant(String.valueOf(val));
////            case VARCHAR:
////                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
////            case CHAR:
////                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
//            default:
//                return null;
//            }
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return getTextRepresentation();
//        }
//
//    }

//    public static class SparkSQLShortConstant extends SparkSQLConstant {
//
//        private final short val;
//
//        public SparkSQLShortConstant(short val) {
//            this.val = val;
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.valueOf(val);
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.SHORT;
//        }
//
//        @Override
//        public short asShort() {
//            return val;
//        }
//
//        @Override
//        public boolean isShort() {
//            return true;
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isNaN()) {
//                return SparkSQLConstant.createFalse();
//            } else if (rightVal.isBoolean()) {
//                return cast(SparkSQLDataType.BOOLEAN).isEquals(rightVal);
//            } else if (rightVal.isByte()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asByte());
//            } else if (rightVal.isShort()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asShort());
//            } else if (rightVal.isInt()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asInt());
//            } else if (rightVal.isLong()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asLong());
//            } else if (rightVal.isFloat()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asFloat());
//            } else if (rightVal.isDouble()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asDouble());
//            } else if (rightVal.isDecimal()) {
//                return SparkSQLConstant.createBooleanConstant(rightVal.asDecimal().equals(new BigDecimal(val)));
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.INT).asLong());
////            } else if (rightVal.isVarChar()) {
////                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.LONG).asLong());
////            } else if (rightVal.isChar()) {
////                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.LONG).asLong());
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isNaN()) {
//                return SparkSQLConstant.createTrue();
//            } else if (rightVal.isByte()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asByte());
//            } else if (rightVal.isShort()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asShort());
//            } else if (rightVal.isInt()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asInt());
//            } else if (rightVal.isLong()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asLong());
//            } else if (rightVal.isFloat()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asFloat());
//            } else if (rightVal.isDouble()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asDouble());
//            } else if (rightVal.isDecimal()) {
//                return SparkSQLConstant.createBooleanConstant(rightVal.asDecimal().compareTo(new BigDecimal(val)) == 1);
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.INT).asLong());
////            } else if (rightVal.isVarChar()) {
////                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.LONG).asLong());
////            } else if (rightVal.isChar()) {
////                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.LONG).asLong());
//            } else if (rightVal.isBoolean()) {
//                throw new AssertionError(rightVal);
//            } else {
//                throw new IgnoreMeException();
//            }
//
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
//            switch (type) {
//            case BOOLEAN:
//                return SparkSQLConstant.createBooleanConstant(val != 0);
////            case SHORT:
////                return this;
//            case INT:
//                return SparkSQLConstant.createIntConstant(val);
////            case LONG:
////                return SparkSQLConstant.createLongConstant(val);
//            case FLOAT:
//                return SparkSQLConstant.createFloatConstant(val);
//            case DOUBLE:
//                return SparkSQLConstant.createDoubleConstant(val);
//            case DECIMAL:
//                return SparkSQLConstant.createDecimalConstant(new BigDecimal(val));
//            case STRING:
//                return SparkSQLConstant.createStringConstant(String.valueOf(val));
////            case VARCHAR:
////                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
////            case CHAR:
////                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
//            default:
//                return null;
//            }
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return getTextRepresentation();
//        }
//
//    }

    public static class SparkSQLIntConstant extends SparkSQLConstant {

        private final long val;

        public SparkSQLIntConstant(long val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public SparkSQLDataType getExpressionType() {
            return SparkSQLDataType.INT;
        }

        @Override
        public long asInt() {
            return val;
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return SparkSQLConstant.createNullConstant();
            } else if (rightVal.isNaN()) {
                return SparkSQLConstant.createFalse();
            } else if (rightVal.isBoolean()) {
                return cast(SparkSQLDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isByte()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asByte());
            } else if (rightVal.isShort()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asShort());
            } else if (rightVal.isInt()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asInt());
            } else if (rightVal.isLong()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asLong());
            } else if (rightVal.isFloat()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asFloat());
            } else if (rightVal.isDouble()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asDouble());
            } else if (rightVal.isDecimal()) {
                return SparkSQLConstant.createBooleanConstant(rightVal.asDecimal().equals(new BigDecimal(val)));
            } else if (rightVal.isString()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.INT).asLong());
//            } else if (rightVal.isVarChar()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.LONG).asLong());
//            } else if (rightVal.isChar()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.LONG).asLong());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return SparkSQLConstant.createNullConstant();
            } else if (rightVal.isNaN()) {
                return SparkSQLConstant.createTrue();
            } else if (rightVal.isByte()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asByte());
            } else if (rightVal.isShort()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asShort());
            } else if (rightVal.isInt()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asInt());
            } else if (rightVal.isLong()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asLong());
            } else if (rightVal.isFloat()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asFloat());
            } else if (rightVal.isDouble()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asDouble());
            } else if (rightVal.isDecimal()) {
                return SparkSQLConstant.createBooleanConstant(rightVal.asDecimal().compareTo(new BigDecimal(val)) == 1);
            } else if (rightVal.isString()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.INT).asLong());
//            } else if (rightVal.isVarChar()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.LONG).asLong());
//            } else if (rightVal.isChar()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.LONG).asLong());
            } else if (rightVal.isBoolean()) {
                throw new AssertionError(rightVal);
            } else {
                throw new IgnoreMeException();
            }

        }

        @Override
        public SparkSQLConstant cast(SparkSQLDataType type) {
            switch (type) {
            case BOOLEAN:
                return SparkSQLConstant.createBooleanConstant(val != 0);
            case INT:
                return this;
//            case LONG:
//                return SparkSQLConstant.createLongConstant(val);
            case FLOAT:
                return SparkSQLConstant.createFloatConstant(val);
            case DOUBLE:
                return SparkSQLConstant.createDoubleConstant(val);
            case DECIMAL:
                return SparkSQLConstant.createDecimalConstant(new BigDecimal(val));
            case STRING:
                return SparkSQLConstant.createStringConstant(String.valueOf(val));
//            case VARCHAR:
//                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
//            case CHAR:
//                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

//    public static class SparkSQLLongConstant extends SparkSQLConstant {
//
//        private final long val;
//
//        public SparkSQLLongConstant(long val) {
//            this.val = val;
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.valueOf(val);
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.LONG;
//        }
//
//        @Override
//        public long asLong() {
//            return val;
//        }
//
//        @Override
//        public boolean isLong() {
//            return true;
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isNaN()) {
//                return SparkSQLConstant.createFalse();
//            } else if (rightVal.isBoolean()) {
//                return cast(SparkSQLDataType.BOOLEAN).isEquals(rightVal);
//            } else if (rightVal.isByte()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asByte());
//            } else if (rightVal.isShort()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asShort());
//            } else if (rightVal.isInt()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asInt());
//            } else if (rightVal.isLong()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asLong());
//            } else if (rightVal.isFloat()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asFloat());
//            } else if (rightVal.isDouble()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asDouble());
//            } else if (rightVal.isDecimal()) {
//                return SparkSQLConstant.createBooleanConstant(rightVal.asDecimal().equals(new BigDecimal(val)));
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.LONG).asLong());
//            } else if (rightVal.isVarChar()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.LONG).asLong());
//            } else if (rightVal.isChar()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.LONG).asLong());
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isNaN()) {
//                return SparkSQLConstant.createTrue();
//            } else if (rightVal.isByte()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asByte());
//            } else if (rightVal.isShort()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asShort());
//            } else if (rightVal.isInt()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asInt());
//            } else if (rightVal.isLong()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asLong());
//            } else if (rightVal.isFloat()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asFloat());
//            } else if (rightVal.isDouble()) {
//                return SparkSQLConstant.createBooleanConstant(val < rightVal.asDouble());
//            } else if (rightVal.isDecimal()) {
//                return SparkSQLConstant.createBooleanConstant(rightVal.asDecimal().compareTo(new BigDecimal(val)) == 1);
////            } else if (rightVal.isString()) {
////                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.LONG).asLong());
////            } else if (rightVal.isVarChar()) {
////                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.LONG).asLong());
////            } else if (rightVal.isChar()) {
////                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.LONG).asLong());
//            } else if (rightVal.isBoolean()) {
//                throw new AssertionError(rightVal);
//            } else {
//                throw new IgnoreMeException();
//            }
//
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
//            switch (type) {
//            case BOOLEAN:
//                return SparkSQLConstant.createBooleanConstant(val != 0);
////            case LONG:
////                return this;
//            case FLOAT:
//                return SparkSQLConstant.createFloatConstant(val);
//            case DOUBLE:
//                return SparkSQLConstant.createDoubleConstant(val);
//            case DECIMAL:
//                return SparkSQLConstant.createDecimalConstant(new BigDecimal(val));
//            case STRING:
//                return SparkSQLConstant.createStringConstant(String.valueOf(val));
////            case VARCHAR:
////                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
////            case CHAR:
////                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
//            default:
//                return null;
//            }
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return getTextRepresentation();
//        }
//
//    }

    public static class SparkSQLFloatConstant extends SparkSQLConstant {

        private final float val;

        public SparkSQLFloatConstant(float val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            if (Double.isFinite(val)) {
                return String.valueOf(val);
            } else {
                return "'" + val + "'";
            }
        }

        @Override
        public SparkSQLDataType getExpressionType() {
            return SparkSQLDataType.FLOAT;
        }

        @Override
        public float asFloat() {
            return val;
        }

        @Override
        public boolean isFloat() {
            return true;
        }

        @Override
        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return SparkSQLConstant.createNullConstant();
            } else if (rightVal.isNaN()) {
                return SparkSQLConstant.createFalse();
            } else if (rightVal.isBoolean()) {
                return cast(SparkSQLDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isByte()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asByte());
            } else if (rightVal.isShort()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asShort());
            } else if (rightVal.isInt()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asInt());
            } else if (rightVal.isLong()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asLong());
            } else if (rightVal.isFloat()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asFloat());
            } else if (rightVal.isDouble()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asDouble());
            } else if (rightVal.isDecimal()) {
                return SparkSQLConstant.createBooleanConstant(rightVal.asDecimal().equals(new BigDecimal(val)));
            } else if (rightVal.isString()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.DOUBLE).asDouble());
            } else if (rightVal.isVarChar()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.DOUBLE).asDouble());
            } else if (rightVal.isChar()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.DOUBLE).asDouble());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return SparkSQLConstant.createNullConstant();
            } else if (rightVal.isNaN()) {
                return SparkSQLConstant.createTrue();
            } else if (rightVal.isByte()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asByte());
            } else if (rightVal.isShort()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asShort());
            } else if (rightVal.isInt()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asInt());
            } else if (rightVal.isLong()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asLong());
            } else if (rightVal.isFloat()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asFloat());
            } else if (rightVal.isDouble()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asDouble());
            } else if (rightVal.isDecimal()) {
                return SparkSQLConstant.createBooleanConstant(rightVal.asDecimal().compareTo(new BigDecimal(val)) == 1);
            } else if (rightVal.isString()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.DOUBLE).asDouble());
            } else if (rightVal.isVarChar()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.DOUBLE).asDouble());
            } else if (rightVal.isChar()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.DOUBLE).asDouble());
            } else if (rightVal.isBoolean()) {
                throw new AssertionError(rightVal);
            } else {
                throw new IgnoreMeException();
            }

        }

        @Override
        public SparkSQLConstant cast(SparkSQLDataType type) {
            switch (type) {
            case BOOLEAN:
                return SparkSQLConstant.createBooleanConstant(val != 0);
            case FLOAT:
                return this;
            case DOUBLE:
                return SparkSQLConstant.createDoubleConstant(val);
            case DECIMAL:
                return SparkSQLConstant.createDecimalConstant(new BigDecimal(val));
            case STRING:
                return SparkSQLConstant.createStringConstant(String.valueOf(val));
//            case VARCHAR:
//                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
//            case CHAR:
//                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class SparkSQLDoubleConstant extends SparkSQLConstant {

        private final double val;

        public SparkSQLDoubleConstant(double val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            if (Double.isFinite(val)) {
                return String.valueOf(val);
            } else {
                return "'" + val + "'";
            }
        }

        @Override
        public SparkSQLDataType getExpressionType() {
            return SparkSQLDataType.FLOAT;
        }

        @Override
        public double asDouble() {
            return val;
        }

        @Override
        public boolean isDouble() {
            return true;
        }

        @Override
        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return SparkSQLConstant.createNullConstant();
            } else if (rightVal.isNaN()) {
                return SparkSQLConstant.createFalse();
            } else if (rightVal.isBoolean()) {
                return cast(SparkSQLDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isByte()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asByte());
            } else if (rightVal.isShort()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asShort());
            } else if (rightVal.isInt()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asInt());
            } else if (rightVal.isLong()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asLong());
            } else if (rightVal.isFloat()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asFloat());
            } else if (rightVal.isDouble()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.asDouble());
            } else if (rightVal.isDecimal()) {
                return SparkSQLConstant.createBooleanConstant(rightVal.asDecimal().equals(new BigDecimal(val)));
            } else if (rightVal.isString()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.DOUBLE).asDouble());
            } else if (rightVal.isVarChar()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.DOUBLE).asDouble());
            } else if (rightVal.isChar()) {
                return SparkSQLConstant.createBooleanConstant(val == rightVal.cast(SparkSQLDataType.DOUBLE).asDouble());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return SparkSQLConstant.createNullConstant();
            } else if (rightVal.isNaN()) {
                return SparkSQLConstant.createTrue();
            } else if (rightVal.isByte()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asByte());
            } else if (rightVal.isShort()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asShort());
            } else if (rightVal.isInt()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asInt());
            } else if (rightVal.isLong()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asLong());
            } else if (rightVal.isFloat()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asFloat());
            } else if (rightVal.isDouble()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.asDouble());
            } else if (rightVal.isDecimal()) {
                return SparkSQLConstant.createBooleanConstant(rightVal.asDecimal().compareTo(new BigDecimal(val)) == 1);
            } else if (rightVal.isString()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.DOUBLE).asDouble());
            } else if (rightVal.isVarChar()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.DOUBLE).asDouble());
            } else if (rightVal.isChar()) {
                return SparkSQLConstant.createBooleanConstant(val < rightVal.cast(SparkSQLDataType.DOUBLE).asDouble());
            } else if (rightVal.isBoolean()) {
                throw new AssertionError(rightVal);
            } else {
                throw new IgnoreMeException();
            }

        }

        @Override
        public SparkSQLConstant cast(SparkSQLDataType type) {
            switch (type) {
            case BOOLEAN:
                return SparkSQLConstant.createBooleanConstant(val != 0);
            case DOUBLE:
                return this;
            case DECIMAL:
                return SparkSQLConstant.createDecimalConstant(new BigDecimal(val));
            case STRING:
                return SparkSQLConstant.createStringConstant(String.valueOf(val));
//            case VARCHAR:
//                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
//            case CHAR:
//                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class SparkSQLDecimalConstant extends SparkSQLConstant {

        private final BigDecimal val;

        public SparkSQLDecimalConstant(BigDecimal val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public SparkSQLDataType getExpressionType() {
            return SparkSQLDataType.FLOAT;
        }

        @Override
        public BigDecimal asDecimal() {
            return val;
        }

        @Override
        public boolean isDecimal() {
            return true;
        }

        @Override
        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return SparkSQLConstant.createNullConstant();
            } else if (rightVal.isNaN()) {
                return SparkSQLConstant.createFalse();
            } else if (rightVal.isBoolean()) {
                return cast(SparkSQLDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isByte()) {
                return SparkSQLConstant.createBooleanConstant(val.equals(new BigDecimal(rightVal.asByte())));
            } else if (rightVal.isShort()) {
                return SparkSQLConstant.createBooleanConstant(val.equals(new BigDecimal(rightVal.asShort())));
            } else if (rightVal.isInt()) {
                return SparkSQLConstant.createBooleanConstant(val.equals(new BigDecimal(rightVal.asInt())));
            } else if (rightVal.isLong()) {
                return SparkSQLConstant.createBooleanConstant(val.equals(new BigDecimal(rightVal.asLong())));
            } else if (rightVal.isFloat()) {
                return SparkSQLConstant.createBooleanConstant(val.equals(new BigDecimal(rightVal.asFloat())));
            } else if (rightVal.isDouble()) {
                return SparkSQLConstant.createBooleanConstant(val.equals(new BigDecimal(rightVal.asDouble())));
            } else if (rightVal.isDecimal()) {
                return SparkSQLConstant.createBooleanConstant(val.equals(rightVal.asDecimal()));
            } else if (rightVal.isString()) {
                return SparkSQLConstant.createBooleanConstant(val.equals(rightVal.cast(SparkSQLDataType.DECIMAL).asDecimal()));
            } else if (rightVal.isVarChar()) {
                return SparkSQLConstant.createBooleanConstant(val.equals(rightVal.cast(SparkSQLDataType.DECIMAL).asDecimal()));
            } else if (rightVal.isChar()) {
                return SparkSQLConstant.createBooleanConstant(val.equals(rightVal.cast(SparkSQLDataType.DECIMAL).asDecimal()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return SparkSQLConstant.createNullConstant();
            } else if (rightVal.isNaN()) {
                return SparkSQLConstant.createTrue();
            } else if (rightVal.isByte()) {
                return SparkSQLConstant.createBooleanConstant(val.compareTo(new BigDecimal(rightVal.asByte())) == -1);
            } else if (rightVal.isShort()) {
                return SparkSQLConstant.createBooleanConstant(val.compareTo(new BigDecimal(rightVal.asShort())) == -1);
            } else if (rightVal.isInt()) {
                return SparkSQLConstant.createBooleanConstant(val.compareTo(new BigDecimal(rightVal.asInt())) == -1);
            } else if (rightVal.isLong()) {
                return SparkSQLConstant.createBooleanConstant(val.compareTo(new BigDecimal(rightVal.asLong())) == -1);
            } else if (rightVal.isFloat()) {
                return SparkSQLConstant.createBooleanConstant(val.compareTo(new BigDecimal(rightVal.asFloat())) == -1);
            } else if (rightVal.isDouble()) {
                return SparkSQLConstant.createBooleanConstant(val.compareTo(new BigDecimal(rightVal.asDouble())) == -1);
            } else if (rightVal.isDecimal()) {
                return SparkSQLConstant.createBooleanConstant(val.compareTo(rightVal.asDecimal()) == -1);
            } else if (rightVal.isString()) {
                return SparkSQLConstant.createBooleanConstant(val.compareTo(new BigDecimal(rightVal.asString())) == -1);
            } else if (rightVal.isVarChar()) {
                return SparkSQLConstant.createBooleanConstant(val.compareTo(new BigDecimal(rightVal.asVarChar())) == -1);
            } else if (rightVal.isChar()) {
                return SparkSQLConstant.createBooleanConstant(val.compareTo(new BigDecimal(rightVal.asChar())) == -1);
            } else if (rightVal.isBoolean()) {
                throw new AssertionError(rightVal);
            } else {
                throw new IgnoreMeException();
            }

        }

        @Override
        public SparkSQLConstant cast(SparkSQLDataType type) {
            switch (type) {
            case BOOLEAN:
                return SparkSQLConstant.createBooleanConstant(!val.equals(0));
            case DECIMAL:
                return this;
            case STRING:
                return SparkSQLConstant.createStringConstant(String.valueOf(val));
//            case VARCHAR:
//                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
//            case CHAR:
//                return SparkSQLConstant.createVarCharConstant(String.valueOf(val), String.valueOf(val).length());
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    // String types

    public static class SparkSQLStringConstant extends SparkSQLConstant {

        private final String value;

        public SparkSQLStringConstant(String value) {
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("'%s'", value.replace("'", "''"));
        }

        @Override
        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isByte()) {
//                return cast(SparkSQLDataType.BYTE).isEquals(rightVal.cast(SparkSQLDataType.BYTE));
//            } else if (rightVal.isShort()) {
//                return cast(SparkSQLDataType.SHORT).isEquals(rightVal.cast(SparkSQLDataType.SHORT));
            } else if (rightVal.isInt()) {
                return cast(SparkSQLDataType.INT).isEquals(rightVal.cast(SparkSQLDataType.INT));
//            } else if (rightVal.isLong()) {
//                return cast(SparkSQLDataType.LONG).isEquals(rightVal.cast(SparkSQLDataType.LONG));
            } else if (rightVal.isFloat()) {
                return cast(SparkSQLDataType.FLOAT).isEquals(rightVal.cast(SparkSQLDataType.FLOAT));
            } else if (rightVal.isDouble()) {
                return cast(SparkSQLDataType.DOUBLE).isEquals(rightVal.cast(SparkSQLDataType.DOUBLE));
            } else if (rightVal.isDecimal()) {
                return cast(SparkSQLDataType.DECIMAL).isEquals(rightVal.cast(SparkSQLDataType.DECIMAL));
            } else if (rightVal.isBoolean()) {
                return cast(SparkSQLDataType.BOOLEAN).isEquals(rightVal.cast(SparkSQLDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return SparkSQLConstant.createBooleanConstant(value.contentEquals(rightVal.asString()));
            } else if (rightVal.isVarChar()) {
                return SparkSQLConstant.createBooleanConstant(value.contentEquals(rightVal.asVarChar()));
            } else if (rightVal.isChar()) {
                return SparkSQLConstant.createBooleanConstant(value.contentEquals(rightVal.asChar()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isByte()) {
//                return cast(SparkSQLDataType.BYTE).isLessThan(rightVal.cast(SparkSQLDataType.BYTE));
//            } else if (rightVal.isShort()) {
//                return cast(SparkSQLDataType.SHORT).isLessThan(rightVal.cast(SparkSQLDataType.SHORT));
            } else if (rightVal.isInt()) {
                return cast(SparkSQLDataType.INT).isLessThan(rightVal.cast(SparkSQLDataType.INT));
//            } else if (rightVal.isLong()) {
//                return cast(SparkSQLDataType.LONG).isLessThan(rightVal.cast(SparkSQLDataType.LONG));
            } else if (rightVal.isFloat()) {
                return cast(SparkSQLDataType.FLOAT).isLessThan(rightVal.cast(SparkSQLDataType.FLOAT));
            } else if (rightVal.isDouble()) {
                return cast(SparkSQLDataType.DOUBLE).isLessThan(rightVal.cast(SparkSQLDataType.DOUBLE));
            } else if (rightVal.isDecimal()) {
                return cast(SparkSQLDataType.DECIMAL).isLessThan(rightVal.cast(SparkSQLDataType.DECIMAL));
            } else if (rightVal.isBoolean()) {
                return cast(SparkSQLDataType.BOOLEAN).isLessThan(rightVal.cast(SparkSQLDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return SparkSQLConstant.createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
            } else if (rightVal.isVarChar()) {
                return SparkSQLConstant.createBooleanConstant(value.compareTo(rightVal.asVarChar()) < 0);
            } else if (rightVal.isChar()) {
                return SparkSQLConstant.createBooleanConstant(value.compareTo(rightVal.asChar()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public SparkSQLConstant cast(SparkSQLDataType type) {
            if (type == SparkSQLDataType.STRING) {
                return this;
            }
            String s = value.trim();
            switch (type) {
            case BOOLEAN:
                try {
                    return SparkSQLConstant.createBooleanConstant(Long.parseLong(s) != 0);
                } catch (NumberFormatException e) {
                }
                switch (s.toUpperCase()) {
                case "T":
                case "TR":
                case "TRU":
                case "TRUE":
                case "1":
                case "YES":
                case "YE":
                case "Y":
                case "ON":
                    return SparkSQLConstant.createTrue();
                case "F":
                case "FA":
                case "FAL":
                case "FALS":
                case "FALSE":
                case "N":
                case "NO":
                case "OF":
                case "OFF":
                default:
                    return SparkSQLConstant.createFalse();
                }
//            case BYTE:
//                try {
//                    return SparkSQLConstant.createByteConstant(Byte.parseByte(s));
//                } catch (NumberFormatException e) {
//                    return SparkSQLConstant.createByteConstant((byte) -1);
//                }
//            case SHORT:
//                try {
//                    return SparkSQLConstant.createShortConstant(Short.parseShort(s));
//                } catch (NumberFormatException e) {
//                    return SparkSQLConstant.createShortConstant((short) -1);
//                }
            case INT:
                try {
                    return SparkSQLConstant.createIntConstant(Integer.parseInt(s));
                } catch (NumberFormatException e) {
                    return SparkSQLConstant.createIntConstant(-1);
                }
//            case LONG:
//                try {
//                    return SparkSQLConstant.createLongConstant(Long.parseLong(s));
//                } catch (NumberFormatException e) {
//                    return SparkSQLConstant.createLongConstant(-1);
//                }
            case FLOAT:
                try {
                    return SparkSQLConstant.createFloatConstant(Float.parseFloat(s));
                } catch (NumberFormatException e) {
                    return SparkSQLConstant.createFloatConstant(-1);
                }
            case DOUBLE:
                try {
                    return SparkSQLConstant.createDoubleConstant(Double.parseDouble(s));
                } catch (NumberFormatException e) {
                    return SparkSQLConstant.createDoubleConstant(-1.0);
                }
            case DECIMAL:
                try {
                    return SparkSQLConstant.createDecimalConstant(new BigDecimal(s));
                } catch (NumberFormatException e) {
                    return SparkSQLConstant.createDoubleConstant(-1.0);
                }
            case STRING:
                return this;
//            case VARCHAR:
//                return SparkSQLConstant.createVarCharConstant(s, value.length());
//            case CHAR:
//                return SparkSQLConstant.createCharConstant(s, s.length());
            default:
                return null;
            }
        }

        @Override
        public SparkSQLDataType getExpressionType() {
            return SparkSQLDataType.STRING;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public String asString() {
            return value;
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return value;
        }

    }

//    public static class SparkSQLVarCharConstant extends SparkSQLConstant {
//
//        private final String value;
//
//        public SparkSQLVarCharConstant(String value, int length) {
//            this.value = value;
//            if (value.length() > length)
//                throw new AssertionError(value);
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.format("'%s'", value.replace("'", "''"));
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
////            } else if (rightVal.isByte()) {
////                return cast(SparkSQLDataType.BYTE).isEquals(rightVal.cast(SparkSQLDataType.BYTE));
////            } else if (rightVal.isShort()) {
////                return cast(SparkSQLDataType.SHORT).isEquals(rightVal.cast(SparkSQLDataType.SHORT));
//            } else if (rightVal.isInt()) {
//                return cast(SparkSQLDataType.INT).isEquals(rightVal.cast(SparkSQLDataType.INT));
////            } else if (rightVal.isLong()) {
////                return cast(SparkSQLDataType.LONG).isEquals(rightVal.cast(SparkSQLDataType.LONG));
//            } else if (rightVal.isFloat()) {
//                return cast(SparkSQLDataType.FLOAT).isEquals(rightVal.cast(SparkSQLDataType.FLOAT));
//            } else if (rightVal.isDouble()) {
//                return cast(SparkSQLDataType.DOUBLE).isEquals(rightVal.cast(SparkSQLDataType.DOUBLE));
//            } else if (rightVal.isDecimal()) {
//                return cast(SparkSQLDataType.DECIMAL).isEquals(rightVal.cast(SparkSQLDataType.DECIMAL));
//            } else if (rightVal.isBoolean()) {
//                return cast(SparkSQLDataType.BOOLEAN).isEquals(rightVal.cast(SparkSQLDataType.BOOLEAN));
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant.createBooleanConstant(value.contentEquals(rightVal.asString()));
//            } else if (rightVal.isVarChar()) {
//                return SparkSQLConstant.createBooleanConstant(value.contentEquals(rightVal.asVarChar()));
//            } else if (rightVal.isChar()) {
//                return SparkSQLConstant.createBooleanConstant(value.contentEquals(rightVal.asChar()));
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
////            } else if (rightVal.isByte()) {
////                return cast(SparkSQLDataType.BYTE).isLessThan(rightVal.cast(SparkSQLDataType.BYTE));
////            } else if (rightVal.isShort()) {
////                return cast(SparkSQLDataType.SHORT).isLessThan(rightVal.cast(SparkSQLDataType.SHORT));
//            } else if (rightVal.isInt()) {
//                return cast(SparkSQLDataType.INT).isLessThan(rightVal.cast(SparkSQLDataType.INT));
////            } else if (rightVal.isLong()) {
////                return cast(SparkSQLDataType.LONG).isLessThan(rightVal.cast(SparkSQLDataType.LONG));
//            } else if (rightVal.isFloat()) {
//                return cast(SparkSQLDataType.FLOAT).isLessThan(rightVal.cast(SparkSQLDataType.FLOAT));
//            } else if (rightVal.isDouble()) {
//                return cast(SparkSQLDataType.DOUBLE).isLessThan(rightVal.cast(SparkSQLDataType.DOUBLE));
//            } else if (rightVal.isDecimal()) {
//                return cast(SparkSQLDataType.DECIMAL).isLessThan(rightVal.cast(SparkSQLDataType.DECIMAL));
//            } else if (rightVal.isBoolean()) {
//                return cast(SparkSQLDataType.BOOLEAN).isLessThan(rightVal.cast(SparkSQLDataType.BOOLEAN));
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant.createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
//            } else if (rightVal.isVarChar()) {
//                return SparkSQLConstant.createBooleanConstant(value.compareTo(rightVal.asVarChar()) < 0);
//            } else if (rightVal.isChar()) {
//                return SparkSQLConstant.createBooleanConstant(value.compareTo(rightVal.asChar()) < 0);
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
////            if (type == SparkSQLDataType.VARCHAR) {
////                return this;
////            }
//            String s = value.trim();
//            switch (type) {
//            case BOOLEAN:
//                try {
//                    return SparkSQLConstant.createBooleanConstant(Long.parseLong(s) != 0);
//                } catch (NumberFormatException e) {
//                }
//                switch (s.toUpperCase()) {
//                case "T":
//                case "TR":
//                case "TRU":
//                case "TRUE":
//                case "1":
//                case "YES":
//                case "YE":
//                case "Y":
//                case "ON":
//                    return SparkSQLConstant.createTrue();
//                case "F":
//                case "FA":
//                case "FAL":
//                case "FALS":
//                case "FALSE":
//                case "N":
//                case "NO":
//                case "OF":
//                case "OFF":
//                default:
//                    return SparkSQLConstant.createFalse();
//                }
////            case BYTE:
////                try {
////                    return SparkSQLConstant.createByteConstant(Byte.parseByte(s));
////                } catch (NumberFormatException e) {
////                    return SparkSQLConstant.createByteConstant((byte) -1);
////                }
////            case SHORT:
////                try {
////                    return SparkSQLConstant.createShortConstant(Short.parseShort(s));
////                } catch (NumberFormatException e) {
////                    return SparkSQLConstant.createShortConstant((short) -1);
////                }
//            case INT:
//                try {
//                    return SparkSQLConstant.createIntConstant(Integer.parseInt(s));
//                } catch (NumberFormatException e) {
//                    return SparkSQLConstant.createIntConstant(-1);
//                }
////            case LONG:
////                try {
////                    return SparkSQLConstant.createLongConstant(Long.parseLong(s));
////                } catch (NumberFormatException e) {
////                    return SparkSQLConstant.createLongConstant(-1);
////                }
//            case FLOAT:
//                try {
//                    return SparkSQLConstant.createFloatConstant(Float.parseFloat(s));
//                } catch (NumberFormatException e) {
//                    return SparkSQLConstant.createFloatConstant(-1);
//                }
//            case DOUBLE:
//                try {
//                    return SparkSQLConstant.createDoubleConstant(Double.parseDouble(s));
//                } catch (NumberFormatException e) {
//                    return SparkSQLConstant.createDoubleConstant(-1.0);
//                }
//            case DECIMAL:
//                try {
//                    return SparkSQLConstant.createDecimalConstant(new BigDecimal(s));
//                } catch (NumberFormatException e) {
//                    return SparkSQLConstant.createDoubleConstant(-1.0);
//                }
//            case STRING:
//                return SparkSQLConstant.createStringConstant(s);
////            case VARCHAR:
////                return this;
////            case CHAR:
////                return SparkSQLConstant.createCharConstant(s, s.length());
//            default:
//                return null;
//            }
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.VARCHAR;
//        }
//
//        @Override
//        public boolean isVarChar() {
//            return true;
//        }
//
//        @Override
//        public String asVarChar() {
//            return value;
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return value;
//        }
//
//    }
//
//    public static class SparkSQLCharConstant extends SparkSQLConstant {
//
//        private final String value;
//
//        public SparkSQLCharConstant(String value, int length) {
//            this.value = value;
//            if (value.length() > length)
//                throw new AssertionError(value);
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.format("'%s'", value.replace("'", "''"));
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.CHAR;
//        }
//
//        @Override
//        public boolean isChar() {
//            return true;
//        }
//
//        @Override
//        public String asChar() {
//            return value;
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return value;
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
////            } else if (rightVal.isByte()) {
////                return cast(SparkSQLDataType.BYTE).isEquals(rightVal.cast(SparkSQLDataType.BYTE));
////            } else if (rightVal.isShort()) {
////                return cast(SparkSQLDataType.SHORT).isEquals(rightVal.cast(SparkSQLDataType.SHORT));
//            } else if (rightVal.isInt()) {
//                return cast(SparkSQLDataType.INT).isEquals(rightVal.cast(SparkSQLDataType.INT));
////            } else if (rightVal.isLong()) {
////                return cast(SparkSQLDataType.LONG).isEquals(rightVal.cast(SparkSQLDataType.LONG));
//            } else if (rightVal.isFloat()) {
//                return cast(SparkSQLDataType.FLOAT).isEquals(rightVal.cast(SparkSQLDataType.FLOAT));
//            } else if (rightVal.isDouble()) {
//                return cast(SparkSQLDataType.DOUBLE).isEquals(rightVal.cast(SparkSQLDataType.DOUBLE));
//            } else if (rightVal.isDecimal()) {
//                return cast(SparkSQLDataType.DECIMAL).isEquals(rightVal.cast(SparkSQLDataType.DECIMAL));
//            } else if (rightVal.isBoolean()) {
//                return cast(SparkSQLDataType.BOOLEAN).isEquals(rightVal.cast(SparkSQLDataType.BOOLEAN));
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant.createBooleanConstant(value.contentEquals(rightVal.asString()));
//            } else if (rightVal.isVarChar()) {
//                return SparkSQLConstant.createBooleanConstant(value.contentEquals(rightVal.asVarChar()));
//            } else if (rightVal.isChar()) {
//                return SparkSQLConstant.createBooleanConstant(value.contentEquals(rightVal.asChar()));
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
////            } else if (rightVal.isByte()) {
////                return cast(SparkSQLDataType.BYTE).isLessThan(rightVal.cast(SparkSQLDataType.BYTE));
////            } else if (rightVal.isShort()) {
////                return cast(SparkSQLDataType.SHORT).isLessThan(rightVal.cast(SparkSQLDataType.SHORT));
//            } else if (rightVal.isInt()) {
//                return cast(SparkSQLDataType.INT).isLessThan(rightVal.cast(SparkSQLDataType.INT));
////            } else if (rightVal.isLong()) {
////                return cast(SparkSQLDataType.LONG).isLessThan(rightVal.cast(SparkSQLDataType.LONG));
//            } else if (rightVal.isFloat()) {
//                return cast(SparkSQLDataType.FLOAT).isLessThan(rightVal.cast(SparkSQLDataType.FLOAT));
//            } else if (rightVal.isDouble()) {
//                return cast(SparkSQLDataType.DOUBLE).isLessThan(rightVal.cast(SparkSQLDataType.DOUBLE));
//            } else if (rightVal.isBoolean()) {
//                return cast(SparkSQLDataType.BOOLEAN).isLessThan(rightVal.cast(SparkSQLDataType.BOOLEAN));
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant.createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
//            } else if (rightVal.isVarChar()) {
//                return SparkSQLConstant.createBooleanConstant(value.compareTo(rightVal.asVarChar()) < 0);
//            } else if (rightVal.isChar()) {
//                return SparkSQLConstant.createBooleanConstant(value.compareTo(rightVal.asChar()) < 0);
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
////            if (type == SparkSQLDataType.CHAR) {
////                return this;
////            }
//            String s = value.trim();
//            switch (type) {
//            case BOOLEAN:
//                try {
//                    return SparkSQLConstant.createBooleanConstant(Long.parseLong(s) != 0);
//                } catch (NumberFormatException e) {
//                }
//                switch (s.toUpperCase()) {
//                case "T":
//                case "TR":
//                case "TRU":
//                case "TRUE":
//                case "1":
//                case "YES":
//                case "YE":
//                case "Y":
//                case "ON":
//                    return SparkSQLConstant.createTrue();
//                case "F":
//                case "FA":
//                case "FAL":
//                case "FALS":
//                case "FALSE":
//                case "N":
//                case "NO":
//                case "OF":
//                case "OFF":
//                default:
//                    return SparkSQLConstant.createFalse();
//                }
////            case BYTE:
////                try {
////                    return SparkSQLConstant.createByteConstant(Byte.parseByte(s));
////                } catch (NumberFormatException e) {
////                    return SparkSQLConstant.createByteConstant((byte) -1);
////                }
////            case SHORT:
////                try {
////                    return SparkSQLConstant.createShortConstant(Short.parseShort(s));
////                } catch (NumberFormatException e) {
////                    return SparkSQLConstant.createShortConstant((short) -1);
////                }
//            case INT:
//                try {
//                    return SparkSQLConstant.createIntConstant(Integer.parseInt(s));
//                } catch (NumberFormatException e) {
//                    return SparkSQLConstant.createIntConstant(-1);
//                }
////            case LONG:
////                try {
////                    return SparkSQLConstant.createLongConstant(Long.parseLong(s));
////                } catch (NumberFormatException e) {
////                    return SparkSQLConstant.createLongConstant(-1);
////                }
//            case FLOAT:
//                try {
//                    return SparkSQLConstant.createFloatConstant(Float.parseFloat(s));
//                } catch (NumberFormatException e) {
//                    return SparkSQLConstant.createFloatConstant(-1);
//                }
//            case DOUBLE:
//                try {
//                    return SparkSQLConstant.createDoubleConstant(Double.parseDouble(s));
//                } catch (NumberFormatException e) {
//                    return SparkSQLConstant.createDoubleConstant(-1.0);
//                }
//            case DECIMAL:
//                try {
//                    return SparkSQLConstant.createDecimalConstant(new BigDecimal(s));
//                } catch (NumberFormatException e) {
//                    return SparkSQLConstant.createDoubleConstant(-1.0);
//                }
//            case STRING:
//                return SparkSQLConstant.createStringConstant(s);
////            case VARCHAR:
////                return SparkSQLConstant.createVarCharConstant(s, value.length());
////            case CHAR:
////                return this;
//            default:
//                return null;
//            }
//        }
//
//    }

    // Binary type

//    public static class SparkSQLBinaryConstant extends SparkSQLConstant {
//
//        private final long val;
//
//        public SparkSQLBinaryConstant(long val) {
//            this.val = val;
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.format("B'%s'", Long.toBinaryString(val));
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.BINARY;
//        }
//
//        @Override
//        public long asBinary() {
//            return val;
//        }
//
//        @Override
//        public boolean isBinary() {
//            return true;
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return Long.toBinaryString(val);
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isBinary()) {
//                return SparkSQLConstant.createBooleanConstant(val == rightVal.asBinary());
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant
//                        .createBooleanConstant(getUnquotedTextRepresentation().equals(rightVal.asString()));
//            } else if (rightVal.isVarChar()) {
//                return SparkSQLConstant
//                        .createBooleanConstant(getUnquotedTextRepresentation().equals(rightVal.asVarChar()));
//            } else if (rightVal.isChar()) {
//                return SparkSQLConstant
//                        .createBooleanConstant(getUnquotedTextRepresentation().equals(rightVal.asChar()));
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant
//                        .createBooleanConstant(getUnquotedTextRepresentation().compareTo(rightVal.asString()) < 0);
//            } else if (rightVal.isVarChar()) {
//                return SparkSQLConstant
//                        .createBooleanConstant(getUnquotedTextRepresentation().compareTo(rightVal.asString()) < 0);
//            } else if (rightVal.isChar()) {
//                return SparkSQLConstant
//                        .createBooleanConstant(getUnquotedTextRepresentation().compareTo(rightVal.asString()) < 0);
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
//            return null;
//        }
//
//    }

    // Boolean type

    public static class SparkSQLBooleanConstant extends SparkSQLConstant {

        private final boolean value;

        public SparkSQLBooleanConstant(boolean value) {
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return value ? "TRUE" : "FALSE";
        }

        @Override
        public SparkSQLDataType getExpressionType() {
            return SparkSQLDataType.BOOLEAN;
        }

        @Override
        public boolean asBoolean() {
            return value;
        }

        @Override
        public boolean isBoolean() {
            return true;
        }

        @Override
        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return SparkSQLConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return SparkSQLConstant.createBooleanConstant(value == rightVal.asBoolean());
            } else if (rightVal.isString()) {
                return SparkSQLConstant
                        .createBooleanConstant(value == rightVal.cast(SparkSQLDataType.BOOLEAN).asBoolean());
            } else if (rightVal.isVarChar()) {
                return SparkSQLConstant
                        .createBooleanConstant(value == rightVal.cast(SparkSQLDataType.BOOLEAN).asBoolean());
            } else if (rightVal.isChar()) {
                return SparkSQLConstant
                        .createBooleanConstant(value == rightVal.cast(SparkSQLDataType.BOOLEAN).asBoolean());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
            if (rightVal.isNull()) {
                return SparkSQLConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return isLessThan(rightVal.cast(SparkSQLDataType.BOOLEAN));
            } else if (rightVal.isVarChar()) {
                return isLessThan(rightVal.cast(SparkSQLDataType.BOOLEAN));
            } else if (rightVal.isChar()) {
                return isLessThan(rightVal.cast(SparkSQLDataType.BOOLEAN));
            } else {
                assert rightVal.isBoolean();
                return SparkSQLConstant.createBooleanConstant((value ? 1 : 0) < (rightVal.asBoolean() ? 1 : 0));
            }
        }

        @Override
        public SparkSQLConstant cast(SparkSQLDataType type) {
            switch (type) {
            case BOOLEAN:
                return this;
//            case BYTE:
//                return SparkSQLConstant.createByteConstant((byte) (value ? 1 : 0));
//            case SHORT:
//                return SparkSQLConstant.createShortConstant((short) (value ? 1 : 0));
            case INT:
                return SparkSQLConstant.createIntConstant(value ? 1 : 0);
//            case LONG:
//                return SparkSQLConstant.createLongConstant(value ? 1 : 0);
            case STRING:
                return SparkSQLConstant.createStringConstant(value ? "true" : "false");
//            case VARCHAR:
//                return SparkSQLConstant.createVarCharConstant(value ? "true" : "false", (value ? "true" : "false").length());
//            case CHAR:
//                return SparkSQLConstant.createCharConstant(value ? "true" : "false", (value ? "true" : "false").length());
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    // Datetime types

//    public static class SparkSQLTimestampConstant extends SparkSQLConstant {
//
//        private final String textRepr;
//
//        public SparkSQLTimestampConstant(long val) {
//            Timestamp timestamp = new Timestamp(val);
//            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            textRepr = dateFormat.format(timestamp);
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.format("'%s'", textRepr);
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.TIMESTAMP;
//        }
//
//        @Override
//        public String asTimestamp() {
//            return textRepr;
//        }
//
//        @Override
//        public boolean isTimestamp() {
//            return true;
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isTimestamp()) {
//                return SparkSQLConstant.createBooleanConstant(textRepr.equals(rightVal.asTimestamp()));
//            } else if (rightVal.isDate()) {
//                return SparkSQLConstant.createBooleanConstant(textRepr.equals(rightVal.cast(SparkSQLDataType.TIMESTAMP).asTimestamp()));
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant
//                        .createBooleanConstant(textRepr.equals(rightVal.cast(SparkSQLDataType.TIMESTAMP).asTimestamp()));
//            } else if (rightVal.isVarChar()) {
//                return SparkSQLConstant
//                        .createBooleanConstant(textRepr.equals(rightVal.cast(SparkSQLDataType.TIMESTAMP).asTimestamp()));
//            } else if (rightVal.isChar()) {
//                return SparkSQLConstant
//                        .createBooleanConstant(textRepr.equals(rightVal.cast(SparkSQLDataType.TIMESTAMP).asTimestamp()));
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isTimestamp()) {
//                return SparkSQLConstant.createBooleanConstant(textRepr.compareTo(rightVal.asTimestamp()) < 0);
//            } else if (rightVal.isDate()) {
//                return SparkSQLConstant.createBooleanConstant(textRepr.compareTo(rightVal.cast(SparkSQLDataType.TIMESTAMP).asTimestamp()) < 0);
////            } else if (rightVal.isString()) {
////                return isLessThan(rightVal.cast(SparkSQLDataType.TIMESTAMP));
////            } else if (rightVal.isVarChar()) {
////                return isLessThan(rightVal.cast(SparkSQLDataType.TIMESTAMP));
////            } else if (rightVal.isChar()) {
////                return isLessThan(rightVal.cast(SparkSQLDataType.TIMESTAMP));
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
//            switch (type) {
////            case TIMESTAMP:
////                return this;
//            case STRING:
//                return SparkSQLConstant.createStringConstant(textRepr);
////            case VARCHAR:
////                return SparkSQLConstant.createVarCharConstant(textRepr, textRepr.length());
////            case CHAR:
////                return SparkSQLConstant.createCharConstant(textRepr, textRepr.length());
//            default:
//                return null;
//            }
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return String.format("%s", textRepr);
//        }
//
//    }
//
//    public static class SparkSQLDateConstant extends SparkSQLConstant {
//
//        private final String textRepr;
//        private final long val;
//
//        public SparkSQLDateConstant(long val) {
//            this.val = val;
//            Date timestamp = new Date(val);
//            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//            textRepr = dateFormat.format(timestamp);
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.format("'%s'", textRepr);
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.DATE;
//        }
//
//        @Override
//        public String asDate() {
//            return textRepr;
//        }
//
//        @Override
//        public boolean isDate() {
//            return true;
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isTimestamp()) {
//                return SparkSQLConstant.createBooleanConstant(this.cast(SparkSQLDataType.TIMESTAMP).asTimestamp().equals(rightVal.asTimestamp()));
//            } else if (rightVal.isDate()) {
//                return SparkSQLConstant.createBooleanConstant(textRepr.equals(rightVal.asDate()));
//            } else if (rightVal.isString()) {
//                return SparkSQLConstant
//                        .createBooleanConstant(textRepr.equals(rightVal.cast(SparkSQLDataType.TIMESTAMP).asTimestamp()));
//            } else if (rightVal.isVarChar()) {
//                return SparkSQLConstant
//                        .createBooleanConstant(textRepr.equals(rightVal.cast(SparkSQLDataType.TIMESTAMP).asTimestamp()));
//            } else if (rightVal.isChar()) {
//                return SparkSQLConstant
//                        .createBooleanConstant(textRepr.equals(rightVal.cast(SparkSQLDataType.TIMESTAMP).asTimestamp()));
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isTimestamp()) {
//                return SparkSQLConstant.createBooleanConstant(this.cast(SparkSQLDataType.TIMESTAMP).asTimestamp().compareTo(rightVal.asTimestamp()) < 0);
//            } else if (rightVal.isDate()) {
//                return SparkSQLConstant.createBooleanConstant(textRepr.compareTo(rightVal.cast(SparkSQLDataType.DATE).asDate()) < 0);
////            } else if (rightVal.isString()) {
////                return isLessThan(rightVal.cast(SparkSQLDataType.DATE));
////            } else if (rightVal.isVarChar()) {
////                return isLessThan(rightVal.cast(SparkSQLDataType.DATE));
////            } else if (rightVal.isChar()) {
////                return isLessThan(rightVal.cast(SparkSQLDataType.DATE));
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
//            switch (type) {
////            case TIMESTAMP:
////                return SparkSQLConstant.createTimestampConstant(val);
////            case DATE:
////                return this;
//            case STRING:
//                return SparkSQLConstant.createStringConstant(textRepr);
////            case VARCHAR:
////                return SparkSQLConstant.createVarCharConstant(textRepr, textRepr.length());
////            case CHAR:
////                return SparkSQLConstant.createCharConstant(textRepr, textRepr.length());
//            default:
//                return null;
//            }
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return String.format("%s", textRepr);
//        }
//
//    }

    // Complex types

//    public static class SparkSQLArrayConstant  extends SparkSQLConstant {
//
//        private final List<SparkSQLConstant> array;
//
//        public SparkSQLArrayConstant(List<SparkSQLConstant> array, SparkSQLConstant containsNull) {
//            this.array = new ArrayList<SparkSQLConstant>();
//            this.array.addAll(array);
//            if (!containsNull.asBoolean()) {
//                for (SparkSQLConstant e: array) {
//                    if (e.isNull()) {
//                        throw new AssertionError(e);
//                    }
//                }
//            }
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.format("%s", array);
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.ARRAY;
//        }
//
//        @Override
//        public List<SparkSQLConstant> asArray() {
//            return array;
//        }
//
//        @Override
//        public boolean isArray() {
//            return true;
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isArray()) {
//                List<SparkSQLConstant> temp = rightVal.asArray();
//                if (temp.size() != array.size())
//                    return SparkSQLConstant.createFalse();
//                for (int i = 0; i < array.size(); i ++) {
//                    if (array.get(i) != temp.get(i))
//                        return SparkSQLConstant.createFalse();
//                }
//                return SparkSQLConstant.createTrue();
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
//            switch (type) {
//            case ARRAY:
//                return this;
//            case STRING:
//                return SparkSQLConstant.createStringConstant(getUnquotedTextRepresentation());
//            case VARCHAR:
//                return SparkSQLConstant.createVarCharConstant(getUnquotedTextRepresentation(), getUnquotedTextRepresentation().length());
//            case CHAR:
//                return SparkSQLConstant.createCharConstant(getUnquotedTextRepresentation(), getUnquotedTextRepresentation().length());
//            default:
//                return null;
//            }
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return getTextRepresentation();
//        }
//
//    }
//
//    public static class SparkSQLMapConstant  extends SparkSQLConstant {
//
//        private final Map<SparkSQLConstant, SparkSQLConstant> map;
//
//        public SparkSQLMapConstant(Map<SparkSQLConstant, SparkSQLConstant> map, SparkSQLConstant valueContainsNull) {
//            this.map = new HashMap<SparkSQLConstant, SparkSQLConstant>();
//            this.map.putAll(map);
//            for (Map.Entry<SparkSQLConstant, SparkSQLConstant> e: map.entrySet()) {
//                if (e.getKey().isNull()) {
//                    throw new AssertionError(e.getKey());
//                }
//                if (!valueContainsNull.asBoolean() && e.getValue().isNull()) {
//                    throw new AssertionError(e.getValue());
//                }
//            }
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.format("%s", map);
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.MAP;
//        }
//
//        @Override
//        public Map<SparkSQLConstant, SparkSQLConstant> asMap() {
//            return map;
//        }
//
//        @Override
//        public boolean isMap() {
//            return true;
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isMap()) {
//                Map<SparkSQLConstant, SparkSQLConstant> temp = rightVal.asMap();
//                if (temp.size() != map.size())
//                    return SparkSQLConstant.createFalse();
//                for (Map.Entry<SparkSQLConstant, SparkSQLConstant> e: map.entrySet()) {
//                    if (!temp.containsKey(e.getKey()) || !temp.get(e.getKey()).equals(e.getValue())) {
//                        return SparkSQLConstant.createFalse();
//                    }
//                }
//                return SparkSQLConstant.createTrue();
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
//            switch (type) {
//            case ARRAY:
//                return this;
//            case STRING:
//                return SparkSQLConstant.createStringConstant(getUnquotedTextRepresentation());
//            case VARCHAR:
//                return SparkSQLConstant.createVarCharConstant(getUnquotedTextRepresentation(), getUnquotedTextRepresentation().length());
//            case CHAR:
//                return SparkSQLConstant.createCharConstant(getUnquotedTextRepresentation(), getUnquotedTextRepresentation().length());
//            default:
//                return null;
//            }
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return getTextRepresentation();
//        }
//
//    }

//    public static class SparkSQLStructConstant  extends SparkSQLConstant {
//
//        private final Row struct;
//
//        public SparkSQLStructConstant(List<SparkSQLStructFieldConstant> fields) {
//            struct = RowFactory.create(fields);
//            for (int i = 0; i < struct.size(); i ++) {
//                for (int j = i + 1; j < struct.size(); j ++) {
//                    if (((SparkSQLConstant) struct.getAs(i)).isEquals((SparkSQLConstant) struct.getAs(j)).asBoolean()) {
//                        throw new AssertionError();
//                    }
//                }
//            }
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.format("%s", struct);
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.STRUCT;
//        }
//
//        @Override
//        public Row asStruct() {
//            return struct;
//        }
//
//        @Override
//        public boolean isStruct() {
//            return true;
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isStruct()) {
//                return SparkSQLConstant.createBooleanConstant(struct.equals(rightVal.asStruct()));
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
//            switch (type) {
//            case STRUCT:
//                return this;
//            case STRING:
//                return SparkSQLConstant.createStringConstant(getUnquotedTextRepresentation());
//            case VARCHAR:
//                return SparkSQLConstant.createVarCharConstant(getUnquotedTextRepresentation(), getUnquotedTextRepresentation().length());
//            case CHAR:
//                return SparkSQLConstant.createCharConstant(getUnquotedTextRepresentation(), getUnquotedTextRepresentation().length());
//            default:
//                return null;
//            }
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return getTextRepresentation();
//        }
//
//    }

//    public static class SparkSQLStructFieldConstant  extends SparkSQLConstant {
//
//        private final SparkSQLStringConstant name;
//        private final SparkSQLConstant value;
//
//        public SparkSQLStructFieldConstant(SparkSQLStringConstant name, SparkSQLConstant value, SparkSQLConstant nullable) {
//            if (!nullable.asBoolean() && value.isNull()) {
//                throw new AssertionError(value);
//            }
//            this.name = name;
//            this.value = value;
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.format("%s: %s", name.getTextRepresentation(), value.getTextRepresentation());
//        }
//
//        @Override
//        public SparkSQLDataType getExpressionType() {
//            return SparkSQLDataType.STRUCT;
//        }
//
//        public SparkSQLStringConstant getName() {
//            return name;
//        }
//
//        @Override
//        public SparkSQLConstant asStructField() {
//            return value;
//        }
//
//        @Override
//        public boolean isStruct() {
//            return true;
//        }
//
//        @Override
//        public SparkSQLConstant isEquals(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else if (rightVal.isStructField()) {
//                return SparkSQLConstant.createBooleanConstant(name.isEquals(((SparkSQLStructFieldConstant) rightVal).getName()).asBoolean() && value.isEquals(rightVal).asBoolean());
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant isLessThan(SparkSQLConstant rightVal) {
//            if (rightVal.isNull()) {
//                return SparkSQLConstant.createNullConstant();
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public SparkSQLConstant cast(SparkSQLDataType type) {
//            switch (type) {
//            case STRUCT_FIELD:
//                return this;
////            case STRUCT:
////                List<SparkSQLStructFieldConstant> field = new ArrayList<SparkSQLStructFieldConstant>();
////                field.add(this);
////                return SparkSQLConstant.createStructConstant(field);
//            case STRING:
//                return SparkSQLConstant.createStringConstant(getUnquotedTextRepresentation());
//            case VARCHAR:
//                return SparkSQLConstant.createVarCharConstant(getUnquotedTextRepresentation(), getUnquotedTextRepresentation().length());
//            case CHAR:
//                return SparkSQLConstant.createCharConstant(getUnquotedTextRepresentation(), getUnquotedTextRepresentation().length());
//            default:
//                return null;
//            }
//        }
//
//        @Override
//        public String getUnquotedTextRepresentation() {
//            return getTextRepresentation();
//        }
//
//    }

    // create constant methods

//    public static SparkSQLConstant createNaNConstant() {
//        return new SparkSQLNaNConstant();
//    }

    public static SparkSQLConstant createNullConstant() {
        return new SparkSQLNullConstant();
    }

//    public static SparkSQLConstant createByteConstant(byte val) {
//        return new SparkSQLByteConstant(val);
//    }
//
//    public static SparkSQLConstant createShortConstant(short val) {
//        return new SparkSQLShortConstant(val);
//    }

    public static SparkSQLConstant createIntConstant(long val) {
        return new SparkSQLIntConstant(val);
    }

//    public static SparkSQLConstant createLongConstant(long val) {
//        return new SparkSQLLongConstant(val);
//    }

    public static SparkSQLConstant createFloatConstant(float val) {
        return new SparkSQLFloatConstant(val);
    }

    public static SparkSQLConstant createDoubleConstant(double val) {
        return new SparkSQLDoubleConstant(val);
    }

    public static SparkSQLConstant createDecimalConstant(BigDecimal bigDecimal) {
        return new SparkSQLDecimalConstant(bigDecimal);
    }

    public static SparkSQLConstant createStringConstant(String string) {
        return new SparkSQLStringConstant(string);
    }

//    public static SparkSQLConstant createVarCharConstant(String string, int length) {
//        return new SparkSQLVarCharConstant(string, length);
//    }
//
//    public static SparkSQLConstant createCharConstant(String string, int length) {
//        return new SparkSQLCharConstant(string, length);
//    }

//    public static SparkSQLConstant createBinaryConstant(long val) {
//        return new SparkSQLBinaryConstant(val);
//    }

    public static SparkSQLConstant createBooleanConstant(boolean val) {
        return new SparkSQLBooleanConstant(val);
    }

    public static SparkSQLConstant createFalse() {
        return createBooleanConstant(false);
    }

    public static SparkSQLConstant createTrue() {
        return createBooleanConstant(true);
    }

//    public static SparkSQLConstant createTimestampConstant(long val) {
//        return new SparkSQLTimestampConstant(val);
//    }
//
//    public static SparkSQLConstant createDateConstant(long val) {
//        return new SparkSQLDateConstant(val);
//    }

//    public static SparkSQLConstant createArrayConstant(List<SparkSQLConstant> array) {
//        return new SparkSQLArrayConstant(array, (SparkSQLBooleanConstant) createTrue());
//    }
//
//    public static SparkSQLConstant createArrayConstant(List<SparkSQLConstant> array, SparkSQLBooleanConstant containsNull) {
//        return new SparkSQLArrayConstant(array, containsNull);
//    }
//
//    public static SparkSQLConstant createMapConstant(Map<SparkSQLConstant, SparkSQLConstant> map) {
//        return new SparkSQLMapConstant(map, (SparkSQLBooleanConstant) createTrue());
//    }
//
//    public static SparkSQLConstant createMapConstant(Map<SparkSQLConstant, SparkSQLConstant> map, SparkSQLBooleanConstant valueContainsNull) {
//        return new SparkSQLMapConstant(map, valueContainsNull);
//    }
//
//    public static SparkSQLConstant createStructConstant(List<SparkSQLStructFieldConstant> fields) {
//        return new SparkSQLStructConstant(fields);
//    }
//
//    public static SparkSQLConstant createStructFieldConstant(SparkSQLStringConstant name, SparkSQLConstant value) {
//        return new SparkSQLStructFieldConstant(name, value, createTrue());
//    }
//
//    public static SparkSQLConstant createStructFieldConstant(SparkSQLStringConstant name, SparkSQLConstant value, SparkSQLBooleanConstant nullable) {
//        return new SparkSQLStructFieldConstant(name, value, nullable);
//    }

    // check type methods

    public boolean isNaN() {
        return false;
    }

    public boolean isNull() {
        return false;
    }

    public boolean isByte() {
        return false;
    }

    public boolean isShort() {
        return false;
    }

    public boolean isInt() {
        return false;
    }

    public boolean isLong() {
        return false;
    }

    public boolean isFloat() {
        return false;
    }

    public boolean isDouble() {
        return false;
    }

    public boolean isDecimal() {
        return false;
    }

    public boolean isString() {
        return false;
    }

    public boolean isVarChar() {
        return false;
    }

    public boolean isChar() {
        return false;
    }

    public boolean isBinary() {
        return false;
    }

    public boolean isBoolean() {
        return false;
    }

    public boolean isTimestamp() {
        return false;
    }

    public boolean isDate() {
        return false;
    }

    public boolean isArray() {
        return false;
    }

    public boolean isMap() {
        return false;
    }

    public boolean isStruct() {
        return false;
    }

    public boolean isStructField() {
        return false;
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        return this;
    }

    // get value methods

    public byte asByte() {
        throw new UnsupportedOperationException(this.toString());
    }

    public short asShort() {
        throw new UnsupportedOperationException(this.toString());
    }

    public long asInt() {
        throw new UnsupportedOperationException(this.toString());
    }

    public long asLong() {
        throw new UnsupportedOperationException(this.toString());
    }

    public float asFloat() {
        throw new UnsupportedOperationException(this.toString());
    }

    public double asDouble() {
        throw new UnsupportedOperationException(this.toString());
    }

    public BigDecimal asDecimal() {
        throw new UnsupportedOperationException(this.toString());
    }

    public String asString() {
        throw new UnsupportedOperationException(this.toString());
    }

    public String asVarChar() {
        throw new UnsupportedOperationException(this.toString());
    }

    public String asChar() {
        throw new UnsupportedOperationException(this.toString());
    }

    public long asBinary() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean asBoolean() {
        throw new UnsupportedOperationException(this.toString());
    }

    public String asTimestamp() {
        throw new UnsupportedOperationException(this.toString());
    }

    public String asDate() {
        throw new UnsupportedOperationException(this.toString());
    }

    public List<SparkSQLConstant> asArray() {
        throw new UnsupportedOperationException(this.toString());
    }

    public Map<SparkSQLConstant,SparkSQLConstant> asMap() {
        throw new UnsupportedOperationException(this.toString());
    }

//    public Row asStruct() {
//        throw new UnsupportedOperationException(this.toString());
//    }

    public SparkSQLConstant asStructField() {
        throw new UnsupportedOperationException(this.toString());
    }

    @Override
    public String toString() {
        return getTextRepresentation();
    }

}