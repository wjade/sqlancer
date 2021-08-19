package sqlancer.synapsesql;

import java.sql.DriverManager;
import java.sql.SQLException;

import sqlancer.AbstractAction;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.gen.SynapseSqlDeleteGenerator;
import sqlancer.synapsesql.gen.SynapseSqlIndexGenerator;
import sqlancer.synapsesql.gen.SynapseSqlInsertGenerator;
import sqlancer.synapsesql.gen.SynapseSqlRandomQuerySynthesizer;
import sqlancer.synapsesql.gen.SynapseSqlTableGenerator;
import sqlancer.synapsesql.gen.SynapseSqlUpdateGenerator;
import sqlancer.synapsesql.gen.SynapseSqlViewGenerator;

public class SynapseSqlProvider extends SQLProviderAdapter<SynapseSqlGlobalState, SynapseSqlOptions> {

    public SynapseSqlProvider() {
        super(SynapseSqlGlobalState.class, SynapseSqlOptions.class);
    }

    public enum Action implements AbstractAction<SynapseSqlGlobalState> {

        INSERT(SynapseSqlInsertGenerator::getQuery), //
        CREATE_INDEX(SynapseSqlIndexGenerator::getQuery), //
        VACUUM((g) -> new SQLQueryAdapter("VACUUM;")), //
        ANALYZE((g) -> new SQLQueryAdapter("ANALYZE;")), //
        DELETE(SynapseSqlDeleteGenerator::generate), //
        UPDATE(SynapseSqlUpdateGenerator::getQuery), //
        CREATE_VIEW(SynapseSqlViewGenerator::generate), //
        EXPLAIN((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            SynapseSqlErrors.addExpressionErrors(errors);
            SynapseSqlErrors.addGroupByErrors(errors);
            return new SQLQueryAdapter(
                    "EXPLAIN " + SynapseSqlToStringVisitor
                            .asString(SynapseSqlRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)),
                    errors);
        });

        private final SQLQueryProvider<SynapseSqlGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<SynapseSqlGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(SynapseSqlGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(SynapseSqlGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case CREATE_INDEX:
            if (!globalState.getDbmsSpecificOptions().testIndexes) {
                return 0;
            }
            // fall through
        case UPDATE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
        case VACUUM: // seems to be ignored
        case ANALYZE: // seems to be ignored
        case EXPLAIN:
            return r.getInteger(0, 2);
        case DELETE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1);
        case CREATE_VIEW:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumViews + 1);
        default:
            throw new AssertionError(a);
        }
    }

    public static class SynapseSqlGlobalState extends SQLGlobalState<SynapseSqlOptions, SynapseSqlSchema> {

        @Override
        protected SynapseSqlSchema readSchema() throws SQLException {
            return SynapseSqlSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(SynapseSqlGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new SynapseSqlTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }
        StatementExecutor<SynapseSqlGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                SynapseSqlProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(SynapseSqlGlobalState globalState) throws SQLException{
        //String url = "jdbc:sqlserver://jade-121.sql.azuresynapse.net:1433;database=jade121;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;";
        String url = "jdbc:sqlserver://jade-121.sql.azuresynapse.net:1433;database=jade121";
        try{
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch(Exception e) {
            //throw new IgnoreMeException();
        }

        return new SQLConnection(DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword()));
    }

    @Override
    public String getDBMSName() {
        return "sqlserver";
    }

}
