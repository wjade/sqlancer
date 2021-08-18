package sqlancer.synapsesql.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.synapsesql.SynapseSqlErrors;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlToStringVisitor;

public final class SynapseSqlViewGenerator {

    private SynapseSqlViewGenerator() {
    }

    public static SQLQueryAdapter generate(SynapseSqlGlobalState globalState) {
        int nrColumns = Randomly.smallNumber() + 1;
        StringBuilder sb = new StringBuilder("CREATE ");
        sb.append("VIEW ");
        sb.append(globalState.getSchema().getFreeViewName());
        sb.append("(");
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("c");
            sb.append(i);
        }
        sb.append(") AS ");
        sb.append(SynapseSqlToStringVisitor.asString(SynapseSqlRandomQuerySynthesizer.generateSelect(globalState, nrColumns)));
        ExpectedErrors errors = new ExpectedErrors();
        SynapseSqlErrors.addExpressionErrors(errors);
        SynapseSqlErrors.addGroupByErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
