package sqlancer.sparksql.gen;

import sqlancer.Randomly;
import sqlancer.common.gen.AbstractInsertGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sparksql.SparkSQLProvider.SparkSQLGlobalState;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLColumn;
import sqlancer.sparksql.SparkSQLSchema.SparkSQLTable;
import sqlancer.sparksql.gen.SparkSQLExpressionGenerator;

import java.util.List;
import java.util.stream.Collectors;

public class SparkSQLInsertGenerator extends AbstractInsertGenerator<SparkSQLColumn> {

    private final SparkSQLGlobalState globalState;
    private final ExpectedErrors errors = new ExpectedErrors();

    public SparkSQLInsertGenerator(SparkSQLGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(SparkSQLGlobalState globalState) {
        return new SparkSQLInsertGenerator(globalState).generate();
    }

    // TODO: implement
    private SQLQueryAdapter generate() {
        return null;
    }
    
    // TODO: implement
    @Override
    protected void insertValue(SparkSQLColumn tiDBColumn) {
        
    }

}
