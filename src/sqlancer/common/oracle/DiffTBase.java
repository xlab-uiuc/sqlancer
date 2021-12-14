package sqlancer.common.oracle;

import sqlancer.Main.StateLogger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;

import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLancerResultSet;

public abstract class DiffTBase<S extends SQLGlobalState<?, ?>> implements TestOracle {

    public static final class ResultLogger {

        private static final File RES_DIRECTORY = new File("res");
        private File curFile;
        public FileWriter currentFileWriter;

        public ResultLogger(String databaseName) {
            ensureExistsAndIsEmpty(RES_DIRECTORY);
            curFile = new File(RES_DIRECTORY, databaseName + "-cur.log");
        }

        private void ensureExistsAndIsEmpty(File dir) {
            if (!dir.exists()) {
                try {
                    Files.createDirectories(dir.toPath());
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            File[] listFiles = dir.listFiles();
            assert listFiles != null : "directory was just created, so it should exist";
            for (File file : listFiles) {
                if (!file.isDirectory()) {
                    file.delete();
                }
            }
        }

        public FileWriter getFileWriter() {
            if (currentFileWriter == null) {
                try {
                    currentFileWriter = new FileWriter(curFile, false);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            return currentFileWriter;
        }

        public void write(SQLancerResultSet rs) {
            printResults(getFileWriter(), rs);
            try {
                currentFileWriter.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void write(String queryInfo) {
            printInfo(getFileWriter(), queryInfo);
            try {
                currentFileWriter.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void printResults(FileWriter writer, SQLancerResultSet rs) {
            StringBuilder sb = new StringBuilder();
            int i = 1;
            try {
                while (rs.next()) {
                    sb.append(rs.getString(i));
                    sb.append('\n');
                    i ++;
                }
            } catch (SQLException e) {
                System.err.println(e.getStackTrace());
            }
            
            try {
                writer.write(sb.toString());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        private void printInfo(FileWriter writer, String queryInfo) {
            try {
                writer.write(queryInfo + "\n");
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

    }

    protected final S state;
    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final StateLogger logger;
    protected final ResultLogger rsLogger;
    protected final MainOptions options;
    protected final SQLConnection con;
    protected String queryString;

    public DiffTBase(S state) {
        this.state = state;
        this.con = state.getConnection();
        this.logger = state.getLogger();
        this.options = state.getOptions();
        this.rsLogger = new ResultLogger(state.getDatabaseName());
    }

}
