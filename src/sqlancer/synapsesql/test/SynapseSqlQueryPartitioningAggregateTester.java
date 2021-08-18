package sqlancer.synapsesql.test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewAliasNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.synapsesql.SynapseSqlErrors;
import sqlancer.synapsesql.SynapseSqlProvider.SynapseSqlGlobalState;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlCompositeDataType;
import sqlancer.synapsesql.SynapseSqlSchema.SynapseSqlDataType;
import sqlancer.synapsesql.SynapseSqlToStringVisitor;
import sqlancer.synapsesql.ast.SynapseSqlExpression;
import sqlancer.synapsesql.ast.SynapseSqlSelect;
import sqlancer.synapsesql.gen.SynapseSqlExpressionGenerator.SynapseSqlAggregateFunction;
import sqlancer.synapsesql.gen.SynapseSqlExpressionGenerator.SynapseSqlBinaryArithmeticOperator;
import sqlancer.synapsesql.gen.SynapseSqlExpressionGenerator.SynapseSqlCastOperation;
import sqlancer.synapsesql.gen.SynapseSqlExpressionGenerator.SynapseSqlUnaryPostfixOperator;
import sqlancer.synapsesql.gen.SynapseSqlExpressionGenerator.SynapseSqlUnaryPrefixOperator;

public class SynapseSqlQueryPartitioningAggregateTester extends SynapseSqlQueryPartitioningBase implements TestOracle {

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public SynapseSqlQueryPartitioningAggregateTester(SynapseSqlGlobalState state) {
        super(state);
        SynapseSqlErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        SynapseSqlAggregateFunction aggregateFunction = Randomly.fromOptions(SynapseSqlAggregateFunction.MAX,
                SynapseSqlAggregateFunction.MIN, SynapseSqlAggregateFunction.SUM, SynapseSqlAggregateFunction.COUNT,
                SynapseSqlAggregateFunction.AVG/* , SynapseSqlAggregateFunction.STDDEV_POP */);
        NewFunctionNode<SynapseSqlExpression, SynapseSqlAggregateFunction> aggregate = gen
                .generateArgsForAggregate(aggregateFunction);
        List<Node<SynapseSqlExpression>> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add(gen.generateAggregate());
        }
        select.setFetchColumns(Arrays.asList(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        originalQuery = SynapseSqlToStringVisitor.asString(select);
        firstResult = getAggregateResult(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = getAggregateResult(metamorphicQuery);

        state.getState().getLocalState().log(
                "--" + originalQuery + ";\n--" + metamorphicQuery + "\n-- " + firstResult + "\n-- " + secondResult);
        if (firstResult == null && secondResult != null
                || firstResult != null && (!firstResult.contentEquals(secondResult)
                        && !ComparatorHelper.isEqualDouble(firstResult, secondResult))) {
            if (secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }
            throw new AssertionError();
        }

    }

    private String createMetamorphicUnionQuery(SynapseSqlSelect select,
            NewFunctionNode<SynapseSqlExpression, SynapseSqlAggregateFunction> aggregate, List<Node<SynapseSqlExpression>> from) {
        String metamorphicQuery;
        Node<SynapseSqlExpression> whereClause = gen.generateExpression();
        Node<SynapseSqlExpression> negatedClause = new NewUnaryPrefixOperatorNode<>(whereClause,
                SynapseSqlUnaryPrefixOperator.NOT);
        Node<SynapseSqlExpression> notNullClause = new NewUnaryPostfixOperatorNode<>(whereClause,
                SynapseSqlUnaryPostfixOperator.IS_NULL);
        List<Node<SynapseSqlExpression>> mappedAggregate = mapped(aggregate);
        SynapseSqlSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinList());
        SynapseSqlSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinList());
        SynapseSqlSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinList());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += SynapseSqlToStringVisitor.asString(leftSelect) + " UNION ALL "
                + SynapseSqlToStringVisitor.asString(middleSelect) + " UNION ALL "
                + SynapseSqlToStringVisitor.asString(rightSelect);
        metamorphicQuery += ") as asdf";
        return metamorphicQuery;
    }

    private String getAggregateResult(String queryString) throws SQLException {
        String resultString;
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        try (SQLancerResultSet result = q.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            if (!result.next()) {
                resultString = null;
            } else {
                resultString = result.getString(1);
            }
            return resultString;
        } catch (SQLException e) {
            if (!e.getMessage().contains("Not implemented type")) {
                throw new AssertionError(queryString, e);
            } else {
                throw new IgnoreMeException();
            }
        }
    }

    private List<Node<SynapseSqlExpression>> mapped(NewFunctionNode<SynapseSqlExpression, SynapseSqlAggregateFunction> aggregate) {
        SynapseSqlCastOperation count;
        switch (aggregate.getFunc()) {
        case COUNT:
        case MAX:
        case MIN:
        case SUM:
            return aliasArgs(Arrays.asList(aggregate));
        case AVG:
            NewFunctionNode<SynapseSqlExpression, SynapseSqlAggregateFunction> sum = new NewFunctionNode<>(aggregate.getArgs(),
                    SynapseSqlAggregateFunction.SUM);
            count = new SynapseSqlCastOperation(new NewFunctionNode<>(aggregate.getArgs(), SynapseSqlAggregateFunction.COUNT),
                    new SynapseSqlCompositeDataType(SynapseSqlDataType.FLOAT, 8));
            return aliasArgs(Arrays.asList(sum, count));
        case STDDEV_POP:
            NewFunctionNode<SynapseSqlExpression, SynapseSqlAggregateFunction> sumSquared = new NewFunctionNode<>(
                    Arrays.asList(new NewBinaryOperatorNode<>(aggregate.getArgs().get(0), aggregate.getArgs().get(0),
                            SynapseSqlBinaryArithmeticOperator.MULT)),
                    SynapseSqlAggregateFunction.SUM);
            count = new SynapseSqlCastOperation(
                    new NewFunctionNode<SynapseSqlExpression, SynapseSqlAggregateFunction>(aggregate.getArgs(),
                            SynapseSqlAggregateFunction.COUNT),
                    new SynapseSqlCompositeDataType(SynapseSqlDataType.FLOAT, 8));
            NewFunctionNode<SynapseSqlExpression, SynapseSqlAggregateFunction> avg = new NewFunctionNode<>(aggregate.getArgs(),
                    SynapseSqlAggregateFunction.AVG);
            return aliasArgs(Arrays.asList(sumSquared, count, avg));
        default:
            throw new AssertionError(aggregate.getFunc());
        }
    }

    private List<Node<SynapseSqlExpression>> aliasArgs(List<Node<SynapseSqlExpression>> originalAggregateArgs) {
        List<Node<SynapseSqlExpression>> args = new ArrayList<>();
        int i = 0;
        for (Node<SynapseSqlExpression> expr : originalAggregateArgs) {
            args.add(new NewAliasNode<SynapseSqlExpression>(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(NewFunctionNode<SynapseSqlExpression, SynapseSqlAggregateFunction> aggregate) {
        switch (aggregate.getFunc()) {
        case STDDEV_POP:
            return "sqrt(SUM(agg0)/SUM(agg1)-SUM(agg2)*SUM(agg2))";
        case AVG:
            return "SUM(agg0::FLOAT)/SUM(agg1)::FLOAT";
        case COUNT:
            return SynapseSqlAggregateFunction.SUM.toString() + "(agg0)";
        default:
            return aggregate.getFunc().toString() + "(agg0)";
        }
    }

    private SynapseSqlSelect getSelect(List<Node<SynapseSqlExpression>> aggregates, List<Node<SynapseSqlExpression>> from,
            Node<SynapseSqlExpression> whereClause, List<Node<SynapseSqlExpression>> joinList) {
        SynapseSqlSelect leftSelect = new SynapseSqlSelect();
        leftSelect.setFetchColumns(aggregates);
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinList(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            leftSelect.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        return leftSelect;
    }

}
