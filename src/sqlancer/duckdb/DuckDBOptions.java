package sqlancer.duckdb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.CompositeTestOracle;
import sqlancer.MainOptions.DBMSConverter;
import sqlancer.TestOracle;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.test.DuckDBNoRECOracle;
import sqlancer.duckdb.test.DuckDBQueryPartitioningAggregateTester;
import sqlancer.duckdb.test.DuckDBQueryPartitioningDistinctTester;
import sqlancer.duckdb.test.DuckDBQueryPartitioningHavingTester;
import sqlancer.duckdb.test.DuckDBQueryPartitioningWhereTester;

@Parameters
public class DuckDBOptions {
	
	@Parameter(names = "--test-collate", arity = 1)
	public boolean testCollate = true;
	
	@Parameter(names = "--test-check", description="Allow generating CHECK constraints in tables", arity = 1)
	public boolean testCheckConstraints = true;
	
	@Parameter(names = "--test-default-values", description="Allow generating DEFAULT values in tables", arity = 1)
	public boolean testDefaultValues = true;
	
	@Parameter(names = "--test-not-null", description="Allow generating NOT NULL constraints in tables", arity = 1)
	public boolean testNotNullConstraints = true;
	
	@Parameter(names = "--test-functions", description="Allow generating functions in expressions", arity = 1)
	public boolean testFunctions = true;
	
	@Parameter(names = "--test-casts", description="Allow generating casts in expressions", arity = 1)
	public boolean testCasts = true;
	
	@Parameter(names = "--test-between", description="Allow generating the BETWEEN operator in expressions", arity = 1)
	public boolean testBetween = true;
	
	@Parameter(names = "--test-in", description="Allow generating the IN operator in expressions", arity = 1)
	public boolean testIn = true;
	
	@Parameter(names = "--test-case", description="Allow generating the CASE operator in expressions", arity = 1)
	public boolean testCase = true;
	
	@Parameter(names = "--test-binary-logicals", description="Allow generating AND and OR in expressions", arity = 1)
	public boolean testBinaryLogicals = true;
	
	@Parameter(names = "--test-int-constants", description="Allow generating INTEGER constants", arity = 1)
	public boolean testIntConstants = true;
	
	@Parameter(names = "--test-varchar-constants", description="Allow generating VARCHAR constants", arity = 1)
	public boolean testStringConstants = true;
	
	@Parameter(names = "--test-float-constants", description="Allow generating floating-point constants", arity = 1)
	public boolean testFloatConstants = true;
	
	@Parameter(names = "--test-boolean-constants", description="Allow generating boolean constants", arity = 1)
	public boolean testBooleanConstants = true;
	
	@Parameter(names = "--test-binary-comparisons", description="Allow generating binary comparison operators (e.g., >= or LIKE)", arity = 1)
	public boolean testBinaryComparisons = true;
	
	@Parameter(names = "--test-indexes", description="Allow explicit (i.e. CREATE INDEX) and implicit (i.e., UNIQUE and PRIMARY KEY) indexes", arity = 1)
	public boolean testIndexes = true;
	
	@Parameter(names = "--max-num-views", description="The maximum number of views that can be generated for a database", arity = 1)
	public int maxNumViews = 1;
	
	@Parameter(names = "--max-num-deletes", description="The maximum number of DELETE statements that are issued for a database", arity = 1)
	public int maxNumDeletes = 1;
	
	@Parameter(names = "--max-num-updates", description="The maximum number of UPDATE statements that are issued for a database", arity = 1)
	public int maxNumUpdates = 5;
	
	@Parameter(names = "--oracle", converter = DBMSConverter.class)
	public List<DuckDBOracle> oracle = Arrays.asList(DuckDBOracle.QUERY_PARTITIONING);

	public static enum DuckDBOracle {
		NOREC {

			@Override
			public TestOracle create(DuckDBGlobalState globalState) throws SQLException {
				return new DuckDBNoRECOracle(globalState);
			}
			
		},
		HAVING {
			@Override
			public TestOracle create(DuckDBGlobalState globalState) throws SQLException {
				return new DuckDBQueryPartitioningHavingTester(globalState);
			}
		},
		WHERE {
			@Override
			public TestOracle create(DuckDBGlobalState globalState) throws SQLException {
				return new DuckDBQueryPartitioningWhereTester(globalState);
			}
		},
		AGGREGATE {

			@Override
			public TestOracle create(DuckDBGlobalState globalState) throws SQLException {
				return new DuckDBQueryPartitioningAggregateTester(globalState);
			}
			
		},
		DISTINCT {
			@Override
			public TestOracle create(DuckDBGlobalState globalState) throws SQLException {
				return new DuckDBQueryPartitioningDistinctTester(globalState);
			}
		},	
		QUERY_PARTITIONING {
			@Override
			public TestOracle create(DuckDBGlobalState globalState) throws SQLException {
				List<TestOracle> oracles = new ArrayList<>();
				oracles.add(new DuckDBQueryPartitioningWhereTester(globalState));
				oracles.add(new DuckDBQueryPartitioningHavingTester(globalState));
				oracles.add(new DuckDBQueryPartitioningAggregateTester(globalState));
				oracles.add(new DuckDBQueryPartitioningDistinctTester(globalState));
				return new CompositeTestOracle(oracles);
			}
		};

		public abstract TestOracle create(DuckDBGlobalState globalState) throws SQLException;

	}


}
