package org.mholford.pyingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import com.neo4j.harness.EnterpriseNeo4jBuilders;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.logging.shaded.log4j.core.lookup.StrSubstitutor;
import org.neo4j.test.rule.TestDirectory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Integration tests for the Python script (ingest.py).  To test various scenarios,
 * there are multiple folder in test/resources with separate config.yml.  The IT works by:
 * <ul>
 *   <li>Starting up an in-proc embedded Neo4j server</li>
 *   <li>Rewriting the config.yml using the boltURI of the embedded server (this is indeterminate
 *   until it starts) and the absolute file paths of the test files.  The rewritten config
 *   (altered-config.yml) should not be changed and it is not checked into the project repo</li>
 *   <li>Running the test python ingest script in a separate Process using the rewritten config</li>
 *   <li>Inspecting the ingested data in the embedded server</li>
 * </ul>
 */
public class IngestPyIT {
  private Neo4j server;
  private Driver driver;
  private final AuthToken BASIC_AUTH = AuthTokens.basic("neo4j", "neo4j");

  @Rule
  public final TestDirectory testDirectory = TestDirectory.testDirectory();

  @Before
  public void setup() {
    //File directory = testDirectory.directory();
    server = EnterpriseNeo4jBuilders.newInProcessBuilder().build();
    driver = GraphDatabase.driver(server.boltURI(), BASIC_AUTH);
  }

  @Test
  public void testMultiDb() {
    var folder = "multi-db";
    try {
      var dbName = findDatabase(folder);
      createTestDatabase(dbName);
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0, dbName);
      checkResults("BLabel", "b", 10, 0, dbName);
      checkResults("CLabel", "c", 10, 0, dbName);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testBasepath() {
    var folder = "basepath";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testRegularIngestCSV() {
    String folder = "plain-ingest-csv";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testRegularIngestJSON() {
    String folder = "plain-ingest-json";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testGzipIngestCSV() {
    String folder = "gzip-ingest-csv";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testGzipIngestJSON() {
    String folder = "gzip-ingest-json";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testZipIngestCSV() {
    String folder = "zip-ingest-csv";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testZipIngestJSON() {
    String folder = "zip-ingest-json";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSkipFileIngest() {
    String folder = "skip-file-ingest";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 0, 0);
      checkResults("CLabel", "c", 10, 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSkipLinesIngestCSV() {
    String folder = "skip-lines-ingest-csv";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 7, 3);
      checkResults("BLabel", "b", 5, 5);
      checkResults("CLabel", "c", 2, 8);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSkipLinesIngestJSON() {
    String folder = "skip-lines-ingest-json";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 7, 3);
      checkResults("BLabel", "b", 5, 5);
      checkResults("CLabel", "c", 2, 8);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testChunksizeIngestCSV() {
    String folder = "chunksize-ingest-csv";
    try {
      rewriteConfig(folder);
      String in = runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
      assertEquals(3, countMatchesIn(in, ".*b\\.csv.*"));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testChunksizeIngestJSON() {
    String folder = "chunksize-ingest-json";
    try {
      rewriteConfig(folder);
      String in = runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
      assertEquals(3, countMatchesIn(in, ".*b\\.json.*"));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testFieldsepIngest() {
    String folder = "fieldsep-ingest";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testPrePostIngest() {
    String folder = "prepost-ingest";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
      checkResults("DLabel", "d", 1, 0);
      checkResults("ELabel", "e", 1, 0);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testS3IngestCSV() {
    String folder = "s3-ingest-csv";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testS3IngestJSON() {
    String folder = "s3-ingest-json";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testExplicitTypeIngest() {
    String folder = "explicit-type-ingest";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);
      checkResults("ALabel", "a", 10, 0);
      checkResults("BLabel", "b", 10, 0);
      checkResults("CLabel", "c", 10, 0);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testNumericValues() {
    String folder = "numbers-ingest";
    try {
      rewriteConfig(folder);
      runPythonIngest(folder);

      Session session = driver.session();
      Record rec = session.run("MATCH (x:ALabel) return x").list().get(0);
      assertEquals("47", rec.get("x").get("p1").asString());
      assertEquals("5.23456", rec.get("x").get("p2").asString());
      assertEquals("123456789012345", rec.get("x").get("p3").asString());

      rec = session.run("MATCH (x:BLabel) return x").list().get(0);
      assertEquals("47", rec.get("x").get("p1").asString());
      assertEquals("5.23456", rec.get("x").get("p2").asString());
      assertEquals("123456789012345", rec.get("x").get("p3").asString());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @After
  public void tearDown() {
    server.close();
  }

  private void checkResults(String label, String prefix, int numRecs, int start) {
    checkResults(label, prefix, numRecs, start, null);
  }

  private void createTestDatabase(String dbName) {
    var dms = server.databaseManagementService();
    dms.createDatabase(dbName);
    dms.startDatabase(dbName);
  }

  private void checkResults(String label, String prefix, int numRecs, int start, String database) {
    Session session;
    if (!Strings.isNullOrEmpty(database)) {
      SessionConfig scon = SessionConfig.builder().withDatabase(database).build();
      session = driver.session(scon);
    } else {
      session = driver.session();
    }
    String q = fmt("MATCH (x:%s) return x order by x.p1", label);
    List<Record> results = session.run(q).list();
    assertEquals(numRecs, results.size());
    for (int i = 0; i < numRecs; i++) {
      Record rec = results.get(i);
      for (int j = 0; j < 3; j++) {
        assertEquals(fmt("%s%d%d", prefix, start+i, j), rec.get("x").get(fmt("p%d", j+1)).asString());
      }
    }
  }

  private String runPythonIngest(String folder) throws IOException, InterruptedException {
    Process proc = new ProcessBuilder("python3", "main/ingest.py",
            fmt("test/resources/%s/altered-config.yml", folder))
            .directory(new File("src")).redirectErrorStream(true).start();
    proc.waitFor();
    InputStream procStream = proc.getInputStream();

    StringWriter writer = new StringWriter();
    IOUtils.copy(procStream, writer, Charset.forName("UTF-8"));
    String msg = writer.toString();
    System.out.println(msg);
    return msg;
  }

  private int countMatchesIn(String in, String match) {
    Pattern pattern = Pattern.compile(match);
    int count = 0;
    try (BufferedReader br = new BufferedReader(new StringReader(in))) {
      String s;
      while ((s = br.readLine()) != null){
        Matcher matcher = pattern.matcher(s);
        if (matcher.matches()) {
          count++;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail();
    }
    return count;
  }

  private void rewriteConfig(String folder) throws IOException {
    String userDir = System.getProperty("user.dir");
    String config = fileToString(fmt("%s/src/test/resources/%s/config.yml", userDir, folder));
    Map<String, String> subs = Map.of("boltURI", server.boltURI().toString(),
            "testResourceDir", Path.of("src/test/resources").toAbsolutePath().toString());
    config = new StrSubstitutor(subs).replace(config);
    Files.write(Path.of(fmt("%s/src/test/resources/%s/altered-config.yml", userDir, folder)),
            config.getBytes());
  }

  private String findDatabase(String folder) throws IOException {
    String userDir = System.getProperty("user.dir");
    File config = new File(fmt("%s/src/test/resources/%s/config.yml", userDir, folder));
    ObjectMapper om = new ObjectMapper(new YAMLFactory());
    var m = om.readValue(config, Map.class);
    var db = m.get("database").toString();
    return db;
  }

  private String fileToString(String path) throws IOException {
    return Files.readString(Path.of(path));
  }

  private static String fmt(String orig, Object... subs) {
    return String.format(orig, (Object[]) subs);
  }
}
