organization := "org.adada"

name := "ada-server"

version := "0.8.1.RC.8.RC.4"

description := "Server side of Ada Discovery Analytics containing a persistence layer, stats and data import/transformation services, and util classes."

isSnapshot := false

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "JCenter" at "http://jcenter.bintray.com/",
  Resolver.mavenLocal
)

val playVersion = "2.5.9"

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play" % playVersion,
  "com.typesafe.play" %% "play-json" % playVersion,
  "org.reactivemongo" %% "play2-reactivemongo" % "0.18.1-play25" exclude("com.typesafe.play", "play_2.11") exclude("com.typesafe.play", "play-json_2.11") exclude("com.typesafe.play", "play-iteratees_2.11") exclude("com.typesafe.play", "play-server_2.11") exclude("com.typesafe.play", "play-netty-server_2.11"),
  "org.reactivemongo" %% "reactivemongo-akkastream" % "0.18.1",
  "com.typesafe.play" %% "play-iteratees" % playVersion,
  "org.in-cal" %% "incal-access-elastic" % "0.2.3",
  "org.apache.ignite" % "ignite-core" % "1.6.0",
  "org.apache.ignite" % "ignite-spring" % "1.6.0",
  "org.apache.ignite" % "ignite-indexing" % "1.6.0",
  "org.apache.ignite" % "ignite-scalar" % "1.6.0",
  "org.in-cal" %% "incal-spark_ml" % "0.2.2" exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.reflections" % "reflections" % "0.9.10" exclude("com.google.code.findbugs", "annotations"),  // class finder
  "com.typesafe.play" %% "play-java-ws" % playVersion,                                              // WS
  "com.unboundid" % "unboundid-ldapsdk" % "2.3.8",                                                  // LDAP (in-memory) - MUST BE REMOVED!!
  "com.github.lejon.T-SNE-Java" % "tsne" % "v2.5.0",                                                // t-SNE Java
  "org.scalanlp" %% "breeze" % "0.13.2",                                                            // linear algebra and stuff
  "org.scalanlp" %% "breeze-natives" % "0.13.2",                                                    // linear algebra and stuff (native)
  //  "org.scalanlp" %% "breeze-viz" % "0.13.2",    // breeze visualization
  "org.scalatest" %% "scalatest" % "3.0.0" % "test"
)

// POM settings for Sonatype
homepage := Some(url("https://ada-discovery.github.io"))

publishMavenStyle := true

scmInfo := Some(ScmInfo(url("https://github.com/ada-discovery/ada-server"), "scm:git@github.com:ada-discovery/ada-server.git"))

developers := List(Developer("bnd", "Peter Banda", "peter.banda@protonmail.com", url("https://peterbanda.net")))

licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")

publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)