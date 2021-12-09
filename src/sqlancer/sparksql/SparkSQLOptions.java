package sqlancer.sparksql;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.sparksql.oracle.SparkSQLPivotedQuerySynthesisOracle;
import sqlancer.sparksql.SparkSQLProvider.SparkSQLGlobalState;
import sqlancer.sparksql.SparkSQLOptions.SparkSQLOracleFactory;
import sqlancer.sparksql.oracle.SparkSQLNoRECOracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "SparkSQL (default port: " + SparkSQLOptions.DEFAULT_PORT
        + ", default host: " + SparkSQLOptions.DEFAULT_HOST)
public class SparkSQLOptions implements DBMSSpecificOptions<SparkSQLOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 10000;

    @Parameter(names = "--oracle")
    public List<SparkSQLOracleFactory> oracles = Arrays.asList(SparkSQLOracleFactory.NOREC);

    // TODO: add all test oracles here
    public enum SparkSQLOracleFactory implements OracleFactory<SparkSQLGlobalState> {
        NOREC {
            @Override
            public TestOracle create(SparkSQLGlobalState globalState) throws SQLException {
                return new SparkSQLNoRECOracle(globalState);
            }
        },
        PQS {
            @Override
            public TestOracle create(SparkSQLGlobalState globalState) throws SQLException {
                return new SparkSQLPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        }
    }

    @Override
    public List<SparkSQLOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
