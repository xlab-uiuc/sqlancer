
  
package sqlancer.sparksql.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
import sqlancer.sparksql.ast.SparkSQLBinaryLogicalOperation.BinaryLogicalOperator;

public class SparkSQLBinaryLogicalOperation extends BinaryOperatorNode<SparkSQLExpression, BinaryLogicalOperator>
        implements SparkSQLExpression {

    public enum BinaryLogicalOperator implements Operator {
        AND {
            @Override
            public SparkSQLConstant apply(SparkSQLConstant left, SparkSQLConstant right) {
                SparkSQLConstant leftBool = left.cast(SparkSQLDataType.BOOLEAN);
                SparkSQLConstant rightBool = right.cast(SparkSQLDataType.BOOLEAN);
                if (leftBool.isNull()) {
                    if (rightBool.isNull()) {
                        return SparkSQLConstant.createNullConstant();
                    } else {
                        if (rightBool.asBoolean()) {
                            return SparkSQLConstant.createNullConstant();
                        } else {
                            return SparkSQLConstant.createFalse();
                        }
                    }
                } else if (!leftBool.asBoolean()) {
                    return SparkSQLConstant.createFalse();
                }
                assert leftBool.asBoolean();
                if (rightBool.isNull()) {
                    return SparkSQLConstant.createNullConstant();
                } else {
                    return SparkSQLConstant.createBooleanConstant(rightBool.isBoolean() && rightBool.asBoolean());
                }
            }
        },
        OR {
            @Override
            public SparkSQLConstant apply(SparkSQLConstant left, SparkSQLConstant right) {
                SparkSQLConstant leftBool = left.cast(SparkSQLDataType.BOOLEAN);
                SparkSQLConstant rightBool = right.cast(SparkSQLDataType.BOOLEAN);
                if (leftBool.isBoolean() && leftBool.asBoolean()) {
                    return SparkSQLConstant.createTrue();
                }
                if (rightBool.isBoolean() && rightBool.asBoolean()) {
                    return SparkSQLConstant.createTrue();
                }
                if (leftBool.isNull() || rightBool.isNull()) {
                    return SparkSQLConstant.createNullConstant();
                }
                return SparkSQLConstant.createFalse();
            }
        };

        public abstract SparkSQLConstant apply(SparkSQLConstant left, SparkSQLConstant right);

        public static BinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public SparkSQLBinaryLogicalOperation(SparkSQLExpression left, SparkSQLExpression right, BinaryLogicalOperator op) {
        super(left, right, op);
    }

    @Override
    public SparkSQLDataType getExpressionType() {
        return SparkSQLDataType.BOOLEAN;
    }

    @Override
    public SparkSQLConstant getExpectedValue() {
        SparkSQLConstant leftExpectedValue = getLeft().getExpectedValue();
        SparkSQLConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().apply(leftExpectedValue, rightExpectedValue);
    }

}