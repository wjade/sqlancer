package sqlancer.synapsesql.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewCaseOperatorNode;
import sqlancer.common.ast.newast.NewPostfixTextNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.synapsesql.SynapseSqlErrors;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlColumn;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlCompositeDataType;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlDataType;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlTable;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlTables;
import sqlancer.synapsesql.SynapseSqlToStringVisitor;
import sqlancer.synapsesql.ast.SynapseSqlConstant;
import sqlancer.synapsesql.ast.SynapseSqlExpression;
import sqlancer.synapsesql.ast.SynapseSqlJoin;
import sqlancer.synapsesql.ast.SynapseSqlSelect;
import sqlancer.synapsesql.gen.SynapseSqlExpressionGenerator;
import sqlancer.synapsesql.gen.SynapseSqlExpressionGenerator.SynapseSqlCastOperation;

public class SynapseSqlNoRECOracle extends NoRECBase<SynapseSqlGlobalState> implements TestOracle {

    private final SynapseSqlSchema s;

    public SynapseSqlNoRECOracle(SynapseSqlGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        SynapseSqlErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        SynapseSqlTables randomTables = s.getRandomTableNonEmptyTables();
        List<SynapseSqlColumn> columns = randomTables.getColumns();
        SynapseSqlExpressionGenerator gen = new SynapseSqlExpressionGenerator(state).setColumns(columns);
        Node<SynapseSqlExpression> randomWhereCondition = gen.generateExpression();
        List<SynapseSqlTable> tables = randomTables.getTables();
        List<TableReferenceNode<SynapseSqlExpression, SynapseSqlTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<SynapseSqlExpression, SynapseSqlTable>(t)).collect(Collectors.toList());
        List<Node<SynapseSqlExpression>> joins = SynapseSqlJoin.getJoins(tableList, state);
        int secondCount = getSecondQuery(tableList.stream().collect(Collectors.toList()), randomWhereCondition, joins);
        int firstCount = getFirstQueryCount(con, tableList.stream().collect(Collectors.toList()), columns,
                randomWhereCondition, joins);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            throw new AssertionError(
                    optimizedQueryString + "; -- " + firstCount + "\n" + unoptimizedQueryString + " -- " + secondCount);
        }
    }

    private int getSecondQuery(List<Node<SynapseSqlExpression>> tableList, Node<SynapseSqlExpression> randomWhereCondition,
            List<Node<SynapseSqlExpression>> joins) throws SQLException {
        SynapseSqlSelect select = new SynapseSqlSelect();
        // select.setGroupByClause(groupBys);
        // SynapseSqlExpression isTrue = SynapseSqlPostfixOperation.create(randomWhereCondition,
        // PostfixOperator.IS_TRUE);
        Node<SynapseSqlExpression> caseExpression = new NewCaseOperatorNode<SynapseSqlExpression>(
            null, 
            List.of(randomWhereCondition), 
            List.of(SynapseSqlConstant.createBooleanConstant(true)), 
            SynapseSqlConstant.createBooleanConstant(false));

        Node<SynapseSqlExpression> asCnt = new NewPostfixTextNode<>(caseExpression, " as cnt");

        //Node<SynapseSqlExpression> asText = new NewPostfixTextNode<>(new SynapseSqlCastOperation(
        //        new NewPostfixTextNode<SynapseSqlExpression>(randomWhereCondition,
        //               " IS NOT NULL AND " + SynapseSqlToStringVisitor.asString(randomWhereCondition)),
        //        new SynapseSqlCompositeDataType(SynapseSqlDataType.INT, 8)), "as count");
        select.setFetchColumns(Arrays.asList(asCnt));
        select.setFromList(tableList);
        // select.setSelectType(SelectType.ALL);
        select.setJoinList(joins);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(cnt) FROM (" + SynapseSqlToStringVisitor.asString(select) + ") as res";
        errors.add("canceling statement due to statement timeout");
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGetLogged(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getFirstQueryCount(SQLConnection con, List<Node<SynapseSqlExpression>> tableList,
            List<SynapseSqlColumn> columns, Node<SynapseSqlExpression> randomWhereCondition, List<Node<SynapseSqlExpression>> joins)
            throws SQLException {
        SynapseSqlSelect select = new SynapseSqlSelect();
        // select.setGroupByClause(groupBys);
        // SynapseSqlAggregate aggr = new SynapseSqlAggregate(
        List<Node<SynapseSqlExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<SynapseSqlExpression, SynapseSqlColumn>(c)).collect(Collectors.toList());
        // SynapseSqlAggregateFunction.COUNT);
        // select.setFetchColumns(Arrays.asList(aggr));
        select.setFetchColumns(allColumns);
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new SynapseSqlExpressionGenerator(state).setColumns(columns).generateOrderBys());
        }
        // select.setSelectType(SelectType.ALL);
        select.setJoinList(joins);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = SynapseSqlToStringVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
