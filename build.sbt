import sbtassembly.AssemblyPlugin.autoImport.ShadeRule
import java.io.{File, FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}


name := "hive-warehouse-connector"
val versionString = sys.props.getOrElse("version", "1.0.0-SNAPSHOT")
version := versionString
organization := "com.hortonworks.hive"
scalaVersion := "2.11.8"
val scalatestVersion = "2.2.6"

sparkVersion := sys.props.getOrElse("spark.version", "2.3.2.3.1.5.0-152")

val hadoopVersion = sys.props.getOrElse("hadoop.version", "3.1.1.3.1.5.0-152")
val hiveVersion = sys.props.getOrElse("hive.version", "3.1.0.3.1.5.0-152")
val log4j2Version = sys.props.getOrElse("log4j2.version", "2.4.1")
val tezVersion = sys.props.getOrElse("tez.version", "0.9.1.3.1.5.0-152")

// for the following, maintain the same version as hive
// curator - required by llap-ext-client
// libfb303, thrift - required by streaming
val curatorVersion = sys.props.getOrElse("curator.version", "2.12.0")
val libfb303Version = sys.props.getOrElse("libfb303.version", "0.9.3")
val thriftVersion = sys.props.getOrElse("thrift.version", "0.9.3-1")

val repoUrl = sys.props.getOrElse("repourl", "https://repo1.maven.org/maven2/")

spName := "hortonworks/hive-warehouse-connector"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

spIgnoreProvided := true

checksums in update := Nil

libraryDependencies ++= Seq(

  "junit" % "junit" % "4.11" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "provided" force(),
  "org.apache.spark" %% "spark-catalyst" % testSparkVersion.value % "provided" force(),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "provided" force(),
  ("org.apache.spark" %% "spark-hive" % testSparkVersion.value % "provided" force())
    .exclude("org.apache.hive", "hive-exec")
    .exclude("org.apache.hive", "hive-service"),
  "org.apache.spark" %% "spark-yarn" % testSparkVersion.value % "provided" force(),
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5" % "compile",

  "org.scala-lang" % "scala-library" % scalaVersion.value % "provided",
  "org.scalatest" %% "scalatest" % scalatestVersion % "test",
  "org.apache.commons" % "commons-collections4" % "4.2" % "compile",

  ("org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % "provided"),

  ("org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "provided"),

  ("org.apache.hadoop" % "hadoop-yarn-registry" % hadoopVersion % "provided"),

  ("org.apache.tez" % "tez-runtime-internals" % tezVersion % "compile")
    .exclude("javax.servlet", "servlet-api")
    .exclude("stax", "stax-api")
    .exclude("commons-beanutils", "commons-beanutils-core")
    .exclude("commons-collections", "commons-collections")
    .exclude("commons-logging", "commons-logging")
    .exclude("org.apache.hadoop", "hadoop-yarn-client")
    .exclude("org.apache.hadoop", "hadoop-yarn-common")
    .exclude("org.apache.hadoop", "hadoop-yarn-api")
    .exclude("org.apache.hadoop", "hadoop-annotations")
    .exclude("org.apache.hadoop", "hadoop-auth")
    .exclude("org.apache.hadoop", "hadoop-hdfs")
    .exclude("com.fasterxml.jackson.core", "jackson-databind"),

  ("org.apache.hive" % "hive-llap-ext-client" % hiveVersion)
    .exclude("ant", "ant")
    .exclude("org.apache.ant", "ant")
    .exclude("org.apache.avro", "avro")
    .exclude("org.apache.calcite", "*")
    .exclude("com.fasterxml.jackson.core", "*")
    .exclude("org.antlr", "*")
    .exclude("org.apache.logging.log4j", "log4j-1.2-api")
    .exclude("org.apache.logging.log4j", "log4j-slf4j-impl")
    .exclude("org.apache.logging.log4j", "log4j-web")
    .exclude("org.apache.slider", "slider-core")
    .exclude("stax", "stax-api")
    .exclude("javax.servlet", "jsp-api")
    .exclude("javax.servlet", "servlet-api")
    .exclude("javax.servlet.jsp", "jsp-api")
    .exclude("javax.transaction", "jta")
    .exclude("javax.transaction", "transaction-api")
    .exclude("org.eclipse.jetty", "jetty-annotations")
    .exclude("org.eclipse.jetty", "jetty-runner")
    .exclude("org.eclipse.jetty", "jetty-xml")
    .exclude("org.mortbay.jetty", "jetty")
    .exclude("org.mortbay.jetty", "jetty-util")
    .exclude("org.mortbay.jetty", "jetty-sslengine")
    .exclude("org.mortbay.jetty", "jsp-2.1")
    .exclude("org.mortbay.jetty", "jsp-api-2.1")
    .exclude("org.mortbay.jetty", "servlet-api-2.5")
    .exclude("org.datanucleus", "datanucleus-api-jdo")
    .exclude("org.datanucleus", "datanucleus-core")
    .exclude("org.datanucleus", "datanucleus-rdbms")
    .exclude("org.datanucleus", "javax.jdo")
    .exclude("org.apache.hadoop", "hadoop-client")
    .exclude("org.apache.hadoop", "hadoop-mapreduce-client-app")
    .exclude("org.apache.hadoop", "hadoop-mapreduce-client-common")
    .exclude("org.apache.hadoop", "hadoop-mapreduce-client-shuffle")
    .exclude("org.apache.hadoop", "hadoop-mapreduce-client-jobclient")
    .exclude("org.apache.hadoop", "hadoop-distcp")
    .exclude("org.apache.hadoop", "hadoop-yarn-server-resourcemanager")
    .exclude("org.apache.hadoop", "hadoop-yarn-registry")
    .exclude("org.apache.hadoop", "hadoop-yarn-server-common")
    .exclude("org.apache.hadoop", "hadoop-yarn-server-applicationhistoryservice")
    .exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy")
    .exclude("org.apache.hadoop", "hadoop-common")
    .exclude("org.apache.hadoop", "hadoop-hdfs")
    .exclude("org.apache.hive", "hive-exec")
    .exclude("org.apache.hive", "hive-shims")
    .exclude("org.apache.hive", "hive-llap-tez")
    .exclude("org.apache.hive", "hive-storage-api")
    .exclude("org.apache.hive", "hive-common")
    .exclude("org.apache.hive", "hive-serde")
    .exclude("org.apache.hive", "hive-service-rpc")
    .exclude("org.apache.hive", "hive-service")
    .exclude("org.apache.hive", "hive-metastore")
    .exclude("org.apache.hive", "hive-standalone-metastore")
    .exclude("org.apache.hive", "hive-vector-code-gen")
    .exclude("com.github.joshelser", "*")
    .exclude("org.apache.hbase", "*")
    .exclude("commons-beanutils", "commons-beanutils-core")
    .exclude("commons-collections", "commons-collections")
    .exclude("commons-logging", "commons-logging")
    .exclude("io.netty", "netty-buffer")
    .exclude("io.netty", "netty-common")
    .exclude("com.fasterxml.jackson.core", "jackson-databind")
    .exclude("org.apache.arrow", "arrow-vector")
    .exclude("org.apache.arrow", "arrow-format")
    .exclude("org.apache.arrow", "arrow-memory"),

  ("org.apache.hive" % "hive-service" % hiveVersion).intransitive(),
  ("org.apache.hive" % "hive-standalone-metastore" % hiveVersion).intransitive(),
  ("org.apache.hive" % "hive-exec" % hiveVersion).intransitive(),
  ("org.apache.hive" % "hive-streaming" % hiveVersion).intransitive(),
  // required by llap-ext-client
  ("org.apache.curator" % "curator-recipes" % curatorVersion).intransitive(),
  // required by hive-streaming
  "org.apache.thrift" % "libfb303" % libfb303Version,
  "org.apache.thrift" % "libthrift" % thriftVersion
)

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.8.2" % "test"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.8.2" % "test"
libraryDependencies += "io.dropwizard.metrics" % "metrics-core" % "3.1.5" % "test"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.0" % "test"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.0" % "test"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.0" % "test"


excludeDependencies ++= Seq(
  "commons-cli" % "commons-cli",
  "org.apache.calcite" % "*",
  "jline" % "jline",
  "org.apache.hive.shims" % "*",
  "org.apache.hive" % "hive-serde",
  "org.apache.hive" % "hive-shims-0.20",
  "org.apache.hive" % "hive-shims-0.23",
  "org.apache.hive" % "hive-shims",
  "org.apache.hive" % "hive-shims-common",
  "org.apache.hive" % "hive-shims-scheduler",
  "org.eclipse.jetty" % "*",
  "org.apache.hive" % "hive-llap-tez",
  "com.sun.jersey" % "*",
  "org.codehaus.groovy" % "*"
)
dependencyOverrides += "com.google.guava" % "guava" % "28.0-jre"
dependencyOverrides += "commons-codec" % "commons-codec" % "1.10"
dependencyOverrides += "commons-logging" % "commons-logging" % "1.2"
dependencyOverrides += "io.netty" % "netty-all" % "4.1.17.Final"
dependencyOverrides += "org.apache.httpcomponents" % "httpclient" % "4.5.4"
dependencyOverrides += "org.apache.httpcomponents" % "httpcore" % "4.4.8"
dependencyOverrides += "org.codehaus.jackson" % "jackson-core-asl" % "1.9.13"
dependencyOverrides += "org.codehaus.jackson" % "jackson-jaxrs" % "1.9.13"
dependencyOverrides += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13"
dependencyOverrides += "org.codehaus.jackson" % "jackson-xc" % "1.9.13"
dependencyOverrides += "org.apache.commons" % "commons-lang3" % "3.5"
libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.1"


// Assembly rules for shaded JAR
assemblyShadeRules in assembly := Seq(
  ShadeRule.zap("scala.**").inAll,

  // Relocate everything in Hive except for llap and hive-streaming
  ShadeRule.rename("org.apache.hadoop.hive.ant.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.common.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.conf.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.io.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hive.common.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hive.jdbc.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hive.service.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.metastore.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.ql.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.serde.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.serde2.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.shims.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.hadoop.hive.thrift.**" -> "shadehive.@0").inAll,
  ShadeRule.rename("org.apache.curator.**" -> "shadecurator.@0").inAll,
  ShadeRule.rename("org.apache.orc.**" -> "shadeorc@0").inAll,
  ShadeRule.rename("org.apache.derby.**" -> "shadederby.@0").inAll,
  ShadeRule.rename("org.apache.thrift.**" -> "shadethrift.@0").inAll,
  ShadeRule.rename("com.facebook.fb303.**" -> "shadethrift.@0").inAll,
  ShadeRule.rename("io.netty.**" -> "shadenetty.@0")
    .inLibrary("io.netty" % "netty-all" % "4.1.17.Final")
    .inLibrary("io.netty" % "netty-buffer" % "4.1.17.Final")
    .inProject,
  ShadeRule.rename("org.jboss.netty.**" -> "shadejbossnetty.@0").inAll,
  ShadeRule.rename("org.codehaus.jackson.**" -> "shadejackson.@0").inAll,
  ShadeRule.rename("com.fasterxml.**" -> "shadefasterxml.@0").inAll,
  ShadeRule.rename("org.glassfish.jersey.**" -> "shadejersey.@0").inAll,
  ShadeRule.rename("com.sun.jersey.**" -> "shadesunjersey.@0").inAll,
  ShadeRule.rename("org.apache.httpcomponents.**" -> "shadehttpcomponents.@0").inAll,
  ShadeRule.rename("org.apache.commons.**" -> "shadecommons.@0").inAll,
//  ShadeRule.rename("commons-codec" -> "shadecommonscodec.@0").inAll,
//  ShadeRule.rename("commons-logging" -> "shadecommonslogging.@0").inAll,
  ShadeRule.rename("com.google.guava.**" -> "shadeguava.@0").inAll
)
test in assembly := {}

// SparkRHWC package
unmanagedResourceDirectories in Compile += baseDirectory.value
includeFilter in (Compile, unmanagedResources) := new SimpleFileFilter(
  f => f.getCanonicalPath.contains("R/pkg") || f.getCanonicalPath.contains("resources/META-INF"))
// Explicitly set the resource directory so that META-INF.service for credential providers
// can be loaded as well.
resourceDirectories in Compile += baseDirectory.value / "src" / "main" / "resources"

assemblyMergeStrategy in assembly := {
  case PathList("org","apache","logging","log4j","core","config","plugins","Log4j2Plugins.dat") => MergeStrategy.first
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("git.properties") => MergeStrategy.first
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.discard
  case x if x.endsWith("package-info.class") => MergeStrategy.first
  // Need to include all the implementations of TokenIdentifier
  // See https://hortonworks.jira.com/browse/BUG-122211 for more details
  case PathList("META-INF", "services", "org.apache.hadoop.security.token.TokenIdentifier") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

def pyFilesZipRecursive(source: File, destZipFile: File): Unit = {
  val destOutput = new ZipOutputStream(new FileOutputStream(destZipFile))
  addPyFilesToZipStream("", source, destOutput)
  destOutput.flush()
  destOutput.close()
}

def addPyFilesToZipStream(parent: String, source: File, output: ZipOutputStream): Unit = {
  if (source.isDirectory) {
    output.putNextEntry(new ZipEntry(parent + source.getName))
    for (file <- source.listFiles()) {
      addPyFilesToZipStream(parent + source.getName + File.separator, file, output)
    }
  } else if (source.getName.endsWith(".py")) {
    val in = new FileInputStream(source)
    output.putNextEntry(new ZipEntry(parent + source.getName))
    val buf = new Array[Byte](8192)
    var n = 0
    while (n != -1) {
      n = in.read(buf)
      if (n != -1) {
        output.write(buf, 0, n)
      }
    }
    output.closeEntry()
    in.close()
  }
}

resourceGenerators in Compile += Def.macroValueI(resourceManaged in Compile map { _: File =>
  val src = new File("./python/pyspark_llap")
  val zipFile = new File(s"./target/pyspark_hwc-$versionString.zip")
  zipFile.delete()
  pyFilesZipRecursive(src, zipFile)
  Seq.empty[File]
}).value

// SparkRHWC package
resourceGenerators in Compile += Def.macroValueI(resourceManaged in Compile map { _: File =>
  import sys.process.Process
  import org.apache.commons.io.FileUtils

  val result = Process("./R/install-dev.sh").!
  if (result != 0) {
    sys.error(s"R build was failed with exit code $result")
  }
  val srcDir = new File("./R/lib/SparkRHWC")
  val destParent = new File(s"./target/SparkRHWC-$versionString")
  FileUtils.deleteDirectory(destParent)
  assert(destParent.mkdir())
  val destDir = new File(destParent, "SparkRHWC")
  FileUtils.copyDirectory(srcDir, destDir)
  Seq.empty[File]
}).value

val assemblyLogLevelString = sys.props.getOrElse("assembly.log.level", "error")

logLevel in assembly := {
  assemblyLogLevelString match {
    case "debug" => Level.Debug
    case "info" => Level.Info
    case "warn" => Level.Warn
    case "error" => Level.Error
  }
}

// Make assembly as default artifact
publishArtifact in (Compile, packageBin) := false
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.copy(`classifier` = None)
}
addArtifact(artifact in (Compile, assembly), assembly)

// SparkRHWC package: we're going to include R package source within Jar so that
// it can be automatically distributed. See `org.apache.spark.deploy.RPackageUtils`
// in Apache Spark.
packageOptions in assembly +=
  Package.ManifestAttributes("Spark-HasRPackage" -> "true")

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
resolvers += "Additional Maven Repository" at repoUrl
resolvers += "Hortonworks Maven Repository" at "http://repo.hortonworks.com/content/groups/public/"

publishMavenStyle := true
pomIncludeRepository := { _ => false } // Remove repositories from pom
pomExtra := (
  <url>https://github.com/hortonworks-spark/spark-llap/</url>
    <licenses>
      <license>
        <name>Apache 2.0 License</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:git@github.com:hortonworks-spark/spark-llap.git</connection>
      <url>scm:git:git@github.com:hortonworks-spark/spark-llap.git</url>
      <tag>HEAD</tag>
    </scm>
    <issueManagement>
      <system>GitHub</system>
      <url>https://github.com/hortonworks-spark/spark-llap/issues</url>
    </issueManagement>)
publishArtifact in Test := false

val username = sys.props.getOrElse("user", "user")
val password = sys.props.getOrElse("password", "password")
val repourl = sys.props.getOrElse("repourl", "https://example.com")
val host = new java.net.URL(repourl).getHost

isSnapshot := true // To allow overwriting for internal nexus
credentials += Credentials("Sonatype Nexus Repository Manager", host, username, password)
publishTo := Some("Sonatype Nexus Repository Manager" at repourl)

// Get full stack trace
testOptions in Test += Tests.Argument("-oD")
fork := true
