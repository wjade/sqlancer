package sqlancer.synapsesql.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.synapsesql.SynapseSqlErrors;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlTable;
import sqlancer.synapsesql.SynapseSqlToStringVisitor;

public final class SynapseSqlDeleteGenerator {

    private SynapseSqlDeleteGenerator() {
    }

    public static SQLQueryAdapter generate(SynapseSqlGlobalState globalState) {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        ExpectedErrors errors = new ExpectedErrors();
        SynapseSqlTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(SynapseSqlToStringVisitor.asString(
                    new SynapseSqlExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
        SynapseSqlErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
