package sqlancer.sparksql.ast;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.FunctionNode;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLDataType;
import sqlancer.sparksql.ast.SparkSQLAggregate.SparkSQLAggregateFunction;

/**
 * @see <a href="https://www.sqlite.org/lang_aggfunc.html">Built-in Aggregate Functions</a>
 */
public class SparkSQLAggregate extends FunctionNode<SparkSQLAggregateFunction, SparkSQLExpression>
        implements SparkSQLExpression {

    public enum SparkSQLAggregateFunction {
        AVG(SparkSQLDataType.INT, SparkSQLDataType.FLOAT, SparkSQLDataType.DOUBLE, SparkSQLDataType.DECIMAL),
        BIT_AND(SparkSQLDataType.INT), BIT_OR(SparkSQLDataType.INT), BOOL_AND(SparkSQLDataType.BOOLEAN),
        BOOL_OR(SparkSQLDataType.BOOLEAN), COUNT(SparkSQLDataType.INT), EVERY(SparkSQLDataType.BOOLEAN), MAX, MIN,
        // STRING_AGG
        SUM(SparkSQLDataType.INT, SparkSQLDataType.FLOAT, SparkSQLDataType.DOUBLE, SparkSQLDataType.DECIMAL);

        private SparkSQLDataType[] supportedReturnTypes;

        SparkSQLAggregateFunction(SparkSQLDataType... supportedReturnTypes) {
            this.supportedReturnTypes = supportedReturnTypes.clone();
        }

        public List<SparkSQLDataType> getTypes(SparkSQLDataType returnType) {
            return Arrays.asList(returnType);
        }

        public boolean supportsReturnType(SparkSQLDataType returnType) {
            return Arrays.asList(supportedReturnTypes).stream().anyMatch(t -> t == returnType)
                    || supportedReturnTypes.length == 0;
        }

        public static List<SparkSQLAggregateFunction> getAggregates(SparkSQLDataType type) {
            return Arrays.asList(values()).stream().filter(p -> p.supportsReturnType(type))
                    .collect(Collectors.toList());
        }

        public SparkSQLDataType getRandomReturnType() {
            if (supportedReturnTypes.length == 0) {
                return Randomly.fromOptions(SparkSQLDataType.getRandomType());
            } else {
                return Randomly.fromOptions(supportedReturnTypes);
            }
        }

    }

    public SparkSQLAggregate(List<SparkSQLExpression> args, SparkSQLAggregateFunction func) {
        super(func, args);
    }

}