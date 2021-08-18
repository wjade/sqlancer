package sqlancer.synapsesql.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.synapsesql.SynapseSqlErrors;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlToStringVisitor;

public class SynapseSqlQueryPartitioningDistinctTester extends SynapseSqlQueryPartitioningBase {

    public SynapseSqlQueryPartitioningDistinctTester(SynapseSqlGlobalState state) {
        super(state);
        SynapseSqlErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setDistinct(true);
        select.setWhereClause(null);
        String originalQueryString = SynapseSqlToStringVisitor.asString(select);

        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        if (Randomly.getBoolean()) {
            select.setDistinct(false);
        }
        select.setWhereClause(predicate);
        String firstQueryString = SynapseSqlToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = SynapseSqlToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = SynapseSqlToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

}
