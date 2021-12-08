package sqlancer.sparksql;

import sqlancer.*;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.sparksql.gen.SparkSQLTableGenerator;
import sqlancer.sparksql.gen.SparkSQLInsertGenerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SparkSQLProvider extends SQLProviderAdapter<sqlancer.sparksql.SparkSQLProvider.SparkSQLGlobalState, SparkSQLOptions> {

    public SparkSQLProvider() {
        super(SparkSQLGlobalState.class, SparkSQLOptions.class);
    }

    // TODO: implement
    public enum Action implements AbstractAction<SparkSQLGlobalState> {

        INSERT(SparkSQLInsertGenerator::insert),
        CREATE_TABLE((g) -> {
            // TODO refactor
            String tableName = DBMSCommon.createTableName(g.getSchema().getDatabaseTables().size());
            return SparkSQLTableGenerator.generate(g, tableName);
        });
//        ADD_JAR((g) -> {
//            // works for a file test.jar in sqlancer's folder "input"
//            return new SQLQueryAdapter("ADD JAR " + System.getProperty("user.dir") + "/input/test.jar");
//        });

        private final SQLQueryProvider<SparkSQLGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<SparkSQLGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(SparkSQLGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(SparkSQLGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case CREATE_TABLE:
            return r.getInteger(0, 1);
        default:
            throw new AssertionError(a);
        }
    }

    // TODO: implement
    public static class SparkSQLGlobalState extends SQLGlobalState<SparkSQLOptions, SparkSQLSchema> {

        @Override
        protected SparkSQLSchema readSchema() throws SQLException {
            return SparkSQLSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    // TODO: implement
    @Override
    public void generateDatabase(SparkSQLGlobalState globalState) throws Exception {
        createTables(globalState, Randomly.fromOptions(4, 5, 6));
        prepareTables(globalState);
    }

    // TODO: implement
    @Override
    public SQLConnection createDatabase(SparkSQLGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = SparkSQLOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = SparkSQLOptions.DEFAULT_PORT;
        }
        String databaseName = globalState.getDatabaseName();
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName + " CASCADE");
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);
        String url = String.format("jdbc:hive2://%s:%d", host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName + " CASCADE");
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }
        return new SQLConnection(con);
    }

    protected void createTables(SparkSQLGlobalState globalState, int numTables) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < numTables) {
            try {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = SparkSQLTableGenerator.generate(globalState, tableName);
                globalState.executeStatement(createTable);
            } catch (IgnoreMeException e) {

            }
        }
    }

    protected void prepareTables(SparkSQLGlobalState globalState) throws Exception {
        StatementExecutor<SparkSQLGlobalState, SparkSQLProvider.Action> se = new StatementExecutor<>(globalState, SparkSQLProvider.Action.values(),
                SparkSQLProvider::mapActions, (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();
    }

    // TODO: implement
    @Override
    public String getDBMSName() {
        return "sparksql";
    }

}
