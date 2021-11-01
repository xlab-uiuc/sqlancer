package sqlancer.sparksql.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;

public class SparkSQLPostfixOperation implements SparkSQLExpression {

    private final SparkSQLExpression expr;
    private final PostfixOperator op;
    private final String operatorTextRepresentation;

    public enum PostfixOperator implements Operator {
        IS_NULL("IS NULL", "ISNULL") {
            @Override
            public SparkSQLConstant apply(SparkSQLConstant expectedValue) {
                return SparkSQLConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public SparkSQLDataType[] getInputDataTypes() {
                return SparkSQLDataType.values();
            }

        },
        IS_UNKNOWN("IS UNKNOWN") {
            @Override
            public SparkSQLConstant apply(SparkSQLConstant expectedValue) {
                return SparkSQLConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public SparkSQLDataType[] getInputDataTypes() {
                return new SparkSQLDataType[] { SparkSQLDataType.BOOLEAN };
            }
        },

        IS_NOT_NULL("IS NOT NULL", "NOTNULL") {

            @Override
            public SparkSQLConstant apply(SparkSQLConstant expectedValue) {
                return SparkSQLConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public SparkSQLDataType[] getInputDataTypes() {
                return SparkSQLDataType.values();
            }

        },
        IS_NOT_UNKNOWN("IS NOT UNKNOWN") {
            @Override
            public SparkSQLConstant apply(SparkSQLConstant expectedValue) {
                return SparkSQLConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public SparkSQLDataType[] getInputDataTypes() {
                return new SparkSQLDataType[] { SparkSQLDataType.BOOLEAN };
            }
        },
        IS_TRUE("IS TRUE") {

            @Override
            public SparkSQLConstant apply(SparkSQLConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return SparkSQLConstant.createFalse();
                } else {
                    return SparkSQLConstant
                            .createBooleanConstant(expectedValue.cast(SparkSQLDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public SparkSQLDataType[] getInputDataTypes() {
                return new SparkSQLDataType[] { SparkSQLDataType.BOOLEAN };
            }

        },
        IS_FALSE("IS FALSE") {

            @Override
            public SparkSQLConstant apply(SparkSQLConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return SparkSQLConstant.createFalse();
                } else {
                    return SparkSQLConstant
                            .createBooleanConstant(!expectedValue.cast(SparkSQLDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public SparkSQLDataType[] getInputDataTypes() {
                return new SparkSQLDataType[] { SparkSQLDataType.BOOLEAN };
            }

        };

        private String[] textRepresentations;

        PostfixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract SparkSQLConstant apply(SparkSQLConstant expectedValue);

        public abstract SparkSQLDataType[] getInputDataTypes();

        public static PostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public SparkSQLPostfixOperation(SparkSQLExpression expr, PostfixOperator op) {
        this.expr = expr;
        this.operatorTextRepresentation = Randomly.fromOptions(op.textRepresentations);
        this.op = op;
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return SparkSQLDataType.BOOLEAN;
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        SparkSQLConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.apply(expectedValue);
    }

    public String getOperatorTextRepresentation() {
        return operatorTextRepresentation;
    }

    public static SparkSQLExpression create(SparkSQLExpression expr, PostfixOperator op) {
        return new SparkSQLPostfixOperation(expr, op);
    }

    public SparkSQLExpression getExpression() {
        return expr;
    }

}
