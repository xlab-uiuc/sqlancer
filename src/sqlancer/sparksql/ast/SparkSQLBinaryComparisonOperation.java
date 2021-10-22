package sqlancer.sparksql.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
import sqlancer.sparksql.ast.SparkSQLBinaryComparisonOperation.SparkSQLBinaryComparisonOperator;

public class SparkSQLBinaryComparisonOperation
        extends BinaryOperatorNode<SparkSQLExpression, SparkSQLBinaryComparisonOperator> implements SparkSQLExpression {

    public enum SparkSQLBinaryComparisonOperator implements Operator {
        EQUALS("=") {
            @Override
            public SparkSQLConstant getExpectedValue(SparkSQLConstant leftVal, SparkSQLConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }
        },
        IS_DISTINCT("IS DISTINCT FROM") {
            @Override
            public SparkSQLConstant getExpectedValue(SparkSQLConstant leftVal, SparkSQLConstant rightVal) {
                return SparkSQLConstant
                        .createBooleanConstant(!IS_NOT_DISTINCT.getExpectedValue(leftVal, rightVal).asBoolean());
            }
        },
        IS_NOT_DISTINCT("IS NOT DISTINCT FROM") {
            @Override
            public SparkSQLConstant getExpectedValue(SparkSQLConstant leftVal, SparkSQLConstant rightVal) {
                if (leftVal.isNull()) {
                    return SparkSQLConstant.createBooleanConstant(rightVal.isNull());
                } else if (rightVal.isNull()) {
                    return SparkSQLConstant.createFalse();
                } else {
                    return leftVal.isEquals(rightVal);
                }
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public SparkSQLConstant getExpectedValue(SparkSQLConstant leftVal, SparkSQLConstant rightVal) {
                SparkSQLConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.isBoolean()) {
                    return SparkSQLConstant.createBooleanConstant(!isEquals.asBoolean());
                }
                return isEquals;
            }
        },
        LESS("<") {

            @Override
            public SparkSQLConstant getExpectedValue(SparkSQLConstant leftVal, SparkSQLConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }
        },
        LESS_EQUALS("<=") {

            @Override
            public SparkSQLConstant getExpectedValue(SparkSQLConstant leftVal, SparkSQLConstant rightVal) {
                SparkSQLConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan.isBoolean() && !lessThan.asBoolean()) {
                    return leftVal.isEquals(rightVal);
                } else {
                    return lessThan;
                }
            }
        },
        GREATER(">") {
            @Override
            public SparkSQLConstant getExpectedValue(SparkSQLConstant leftVal, SparkSQLConstant rightVal) {
                SparkSQLConstant equals = leftVal.isEquals(rightVal);
                if (equals.isBoolean() && equals.asBoolean()) {
                    return SparkSQLConstant.createFalse();
                } else {
                    SparkSQLConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return SparkSQLConstant.createNullConstant();
                    }
                    return SparkSQLPrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
                }
            }
        },
        GREATER_EQUALS(">=") {

            @Override
            public SparkSQLConstant getExpectedValue(SparkSQLConstant leftVal, SparkSQLConstant rightVal) {
                SparkSQLConstant equals = leftVal.isEquals(rightVal);
                if (equals.isBoolean() && equals.asBoolean()) {
                    return SparkSQLConstant.createTrue();
                } else {
                    SparkSQLConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return SparkSQLConstant.createNullConstant();
                    }
                    return SparkSQLPrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
                }
            }

        };

        private final String textRepresentation;

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        SparkSQLBinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public abstract SparkSQLConstant getExpectedValue(SparkSQLConstant leftVal, SparkSQLConstant rightVal);

        public static SparkSQLBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(SparkSQLBinaryComparisonOperator.values());
        }

    }

    public SparkSQLBinaryComparisonOperation(SparkSQLExpression left, SparkSQLExpression right,
            SparkSQLBinaryComparisonOperator op) {
        super(left, right, op);
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        SparkSQLConstant leftExpectedValue = getLeft().getExpectedValue();
        SparkSQLConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().getExpectedValue(leftExpectedValue, rightExpectedValue);
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return SparkSQLDataType.BOOLEAN;
    }

}