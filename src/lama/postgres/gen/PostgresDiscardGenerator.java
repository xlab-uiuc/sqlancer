package lama.postgres.gen;

import java.util.Arrays;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.postgres.PostgresSchema;
import lama.postgres.PostgresSchema.PostgresTable.TableType;

public class PostgresDiscardGenerator {

	public static Query create(PostgresSchema s) {
		StringBuilder sb = new StringBuilder();
		sb.append("DISCARD ");
		// prevent that DISCARD discards all tables (if they are TEMP tables)
		boolean hasNonTempTables = s.getDatabaseTables().stream().anyMatch(t -> t.getTableType() == TableType.STANDARD);
		String what;
		if (hasNonTempTables) {
			what = Randomly.fromOptions("ALL", "PLANS", "SEQUENCES", "TEMPORARY", "TEMP");
		} else {
			what = Randomly.fromOptions("PLANS", "SEQUENCES");
		}
		sb.append(what);
		return new QueryAdapter(sb.toString(), Arrays.asList("cannot run inside a transaction block")) {

			@Override
			public boolean couldAffectSchema() {
				return canDiscardTemporaryTables(what);
			}
		};
	}

	private static boolean canDiscardTemporaryTables(String what) {
		return what.contentEquals("TEMPORARY") || what.contentEquals("TEMP") || what.contentEquals("ALL");
	}
}