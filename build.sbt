import com.typesafe.sbt.license.{DepModuleInfo, LicenseInfo}

organization := "org.adada"

name := "ada-server"

version := "0.8.1"

description := "Server side of Ada Discovery Analytics containing a persistence layer, stats and data import/transformation services, and util classes."

isSnapshot := false

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "Sci Java" at "https://maven.scijava.org/content/repositories/public/", // for the T-SNE lib
  Resolver.mavenLocal
)

val playVersion = "2.5.9"

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play" % playVersion,
  "com.typesafe.play" %% "play-json" % playVersion,
  "org.reactivemongo" %% "play2-reactivemongo" % "0.18.1-play25" exclude("com.typesafe.play", "play_2.11") exclude("com.typesafe.play", "play-json_2.11") exclude("com.typesafe.play", "play-iteratees_2.11") exclude("com.typesafe.play", "play-server_2.11") exclude("com.typesafe.play", "play-netty-server_2.11"),
  "org.reactivemongo" %% "reactivemongo-akkastream" % "0.18.1",
  "com.typesafe.play" %% "play-iteratees" % playVersion,
  "org.in-cal" %% "incal-access-elastic" % "0.2.4",                                                 // Elastic search
  "org.apache.ignite" % "ignite-core" % "1.6.0",
  "org.apache.ignite" % "ignite-spring" % "1.6.0",
  "org.apache.ignite" % "ignite-indexing" % "1.6.0",
  "org.apache.ignite" % "ignite-scalar" % "1.6.0",
  "org.in-cal" %% "incal-spark_ml" % "0.2.3" exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.reflections" % "reflections" % "0.9.10" exclude("com.google.code.findbugs", "annotations"),  // class finder
  "com.typesafe.play" %% "play-java-ws" % playVersion,                                              // WS
  "com.unboundid" % "unboundid-ldapsdk" % "2.3.8",                                                  // LDAP
  "com.github.lejon.T-SNE-Java" % "tsne" % "v2.5.0",                                                // t-SNE Java
  "org.scalanlp" %% "breeze" % "0.13.2",                                                            // linear algebra and stuff
  "org.scalanlp" %% "breeze-natives" % "0.13.2",                                                    // linear algebra and stuff (native)
  //  "org.scalanlp" %% "breeze-viz" % "0.13.2",    // breeze visualization
  "org.scalatest" %% "scalatest" % "3.0.0" % "test"
)

// For licenses not automatically downloaded (need to list them manually)
licenseOverrides := {
  case
    DepModuleInfo("org.apache.commons", _, _)
    | DepModuleInfo("org.apache.curator", _, _)
    | DepModuleInfo("org.apache.directory.api", _, _)
    | DepModuleInfo("org.apache.directory.server", _, _)
    | DepModuleInfo("org.apache.httpcomponents", _, _)
    | DepModuleInfo("org.apache.hadoop", _, _)
    | DepModuleInfo("org.apache.parquet", _, _)
    | DepModuleInfo("org.apache.avro", _, _)
    | DepModuleInfo("commons-beanutils", "commons-beanutils", _)
    | DepModuleInfo("commons-beanutils", "commons-beanutils-core", _)
    | DepModuleInfo("commons-cli", "commons-cli", _)
    | DepModuleInfo("commons-codec", "commons-codec", _)
    | DepModuleInfo("commons-collections", "commons-collections", _)
    | DepModuleInfo("commons-io", "commons-io", _)
    | DepModuleInfo("commons-lang", "commons-lang", _)
    | DepModuleInfo("commons-logging", "commons-logging", _)
    | DepModuleInfo("commons-net", "commons-net", _)
    | DepModuleInfo("com.google.guava", "guava", _)
    | DepModuleInfo("com.google.inject", "guice", _)
    | DepModuleInfo("com.google.inject.extensions", "guice-multibindings", _)
    | DepModuleInfo("com.google.inject.extensions", "guice-assistedinject", "4.0")
    | DepModuleInfo("io.dropwizard.metrics", _, _)
    | DepModuleInfo("org.apache.xbean", "xbean-asm5-shaded", "4.4")
    | DepModuleInfo("org.apache.ivy", "ivy", "2.4.0")
    | DepModuleInfo("org.apache.zookeeper", "zookeeper", "3.4.6")
    | DepModuleInfo("com.fasterxml.jackson.module", "jackson-module-paranamer", "2.6.5")
    | DepModuleInfo("io.netty", "netty-all", "4.0.43.Final")
    | DepModuleInfo("com.bnd-lib", _, _)
    | DepModuleInfo("org.codehaus.jettison", "jettison", "1.1")
    | DepModuleInfo("org.htrace", "htrace-core", "3.0.4")
    | DepModuleInfo("org.mortbay.jetty", "jetty-util", "6.1.26")
    | DepModuleInfo("org.objenesis", "objenesis", "2.1")
    | DepModuleInfo("com.carrotsearch", "hppc", "0.7.1")
    | DepModuleInfo("com.github.lejon.T-SNE-Java", "tsne", "v2.5.0")
    | DepModuleInfo("oauth.signpost", "signpost-commonshttp4", "1.2.1.2")
    | DepModuleInfo("oauth.signpost", "signpost-core", "1.2.1.2")
    | DepModuleInfo("org.hibernate", "hibernate-validator", "5.2.4.Final")
    | DepModuleInfo("org.json4s", "json4s-ast_2.11", "3.2.11")
    | DepModuleInfo("org.json4s", "json4s-core_2.11", "3.2.11")
    | DepModuleInfo("org.json4s", "json4s-jackson_2.11", "3.2.11")
    | DepModuleInfo("javax.cache", "cache-api", "1.0.0")
    | DepModuleInfo("oro", "oro", "2.0.8")
    | DepModuleInfo("xerces", "xercesImpl", "2.9.1")
  =>
    LicenseInfo(LicenseCategory.Apache, "Apache License v2.0", "http://www.apache.org/licenses/LICENSE-2.0")

  case
    DepModuleInfo("org.glassfish.hk2", "hk2-api", "2.4.0-b34")
    | DepModuleInfo("org.glassfish.hk2", "hk2-locator", "2.4.0-b34")
    | DepModuleInfo("org.glassfish.hk2", "hk2-utils", "2.4.0-b34")
    | DepModuleInfo("org.glassfish.hk2", "osgi-resource-locator", "1.0.1")
    | DepModuleInfo("org.glassfish.hk2.external", "aopalliance-repackaged", "2.4.0-b34")
    | DepModuleInfo("org.glassfish.hk2.external", "javax.inject", "2.4.0-b34")
    | DepModuleInfo("org.glassfish.jersey.bundles.repackaged", "jersey-guava", "2.22.2")
    | DepModuleInfo("org.glassfish.jersey.containers", "jersey-container-servlet", "2.22.2")
    | DepModuleInfo("org.glassfish.jersey.containers", "jersey-container-servlet-core", "2.22.2")
    | DepModuleInfo("org.glassfish.jersey.core", "jersey-client", "2.22.2")
    | DepModuleInfo("org.glassfish.jersey.core", "jersey-common", "2.22.2")
    | DepModuleInfo("org.glassfish.jersey.core", "jersey-server", "2.22.2")
    | DepModuleInfo("org.glassfish.jersey.media", "jersey-media-jaxb", "2.22.2")
  =>
    LicenseInfo(LicenseCategory.GPLClasspath, "CDDL + GPLv2 with classpath exception", "https://javaee.github.io/glassfish/LICENSE")

  case
    DepModuleInfo("javax.mail", "mail", "1.4.7")
  =>
    LicenseInfo(LicenseCategory.GPLClasspath, "CDDL + GPLv2 with classpath exception", "https://javaee.github.io/javamail/LICENSE")

  case
    DepModuleInfo("javax.transaction", "jta", "1.1")
  =>
    LicenseInfo(LicenseCategory.GPLClasspath, "CDDL + GPLv2 with classpath exception", "https://github.com/javaee/javax.transaction/blob/master/LICENSE")

  case
    DepModuleInfo("com.esotericsoftware", "kryo-shaded", "3.0.3")
  | DepModuleInfo("org.hamcrest", "hamcrest-core", "1.3")
  =>
    LicenseInfo(LicenseCategory.BSD, "BSD 2-clause", "https://opensource.org/licenses/BSD-2-Clause")

  case
    DepModuleInfo("com.github.fommil.netlib", "core", "1.1.2")
  | DepModuleInfo("com.github.fommil", "jniloader", "1.1")
  | DepModuleInfo("org.antlr", "antlr4-runtime", "4.5.3")
  | DepModuleInfo("org.fusesource.leveldbjni", "leveldbjni-all", "1.8")
  =>
    LicenseInfo(LicenseCategory.BSD, "BSD 3-clause", "https://opensource.org/licenses/BSD-3-Clause")

  case
    DepModuleInfo("org.codehaus.janino", "commons-compiler", "3.0.0")
  | DepModuleInfo("org.codehaus.janino", "janino", "3.0.0")
  =>
    LicenseInfo(LicenseCategory.BSD, "New BSD License", "http://www.opensource.org/licenses/bsd-license.php")

  case DepModuleInfo("org.slf4j", _, _) =>
    LicenseInfo(LicenseCategory.MIT, "MIT", "http://opensource.org/licenses/MIT")

  case DepModuleInfo("org.bouncycastle", "bcprov-jdk15on", "1.51") =>
    LicenseInfo(LicenseCategory.MIT, "Bouncy Castle Licence", "http://www.bouncycastle.org/licence.html")

  case
    DepModuleInfo("com.h2database", "h2", "1.3.175") // h2database has a dual MPL / EPL license (http://h2database.com/html/license.html), we choose EPL
  | DepModuleInfo("junit", "junit", "4.12")
  =>
    LicenseInfo(LicenseCategory.EPL, "Eclipse Public License 1.0", "http://www.eclipse.org/legal/epl-v10.html")

  case
    DepModuleInfo("com.unboundid", "unboundid-ldapsdk", "2.3.8") // LDAP SDK has a ternary GPLv2 / GPLv2.1 / UnboundID LDAP SDK Free Use license, we choose the last one
  =>
    LicenseInfo(LicenseCategory.Unrecognized, "UnboundID LDAP SDK Free Use License", "https://github.com/pingidentity/ldapsdk/blob/master/LICENSE-UnboundID-LDAPSDK.txt")
}


// POM settings for Sonatype

homepage := Some(url("https://ada-discovery.github.io"))

publishMavenStyle := true

scmInfo := Some(ScmInfo(url("https://github.com/ada-discovery/ada-server"), "scm:git@github.com:ada-discovery/ada-server.git"))

developers := List(
  Developer("bnd", "Peter Banda", "peter.banda@protonmail.com", url("https://peterbanda.net")),
  Developer("sherzinger", "Sascha Herzinger", "sascha.herzinger@uni.lu", url("https://wwwfr.uni.lu/lcsb/people/sascha_herzinger"))
)

licenses += "Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

fork in Test := true