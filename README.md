# Ada Server [![version](https://img.shields.io/badge/version-0.8.1-green.svg)](https://ada-discovery.github.io) [![License](https://img.shields.io/badge/License-Apache%202.0-lightgrey.svg)](https://www.apache.org/licenses/LICENSE-2.0) [![Build Status](https://travis-ci.com/ada-discovery/ada-server.svg?branch=master)](https://travis-ci.com/ada-discovery/ada-server)

<img src="https://ada-discovery.github.io/images/logo.png" width="450px">

The project serves as a server part of Ada Discovery Analytics platform providing:

* Domain classes with JSON formatters.
* Persistence layer with convenient repo abstractions for Mongo, Elastic Search, and Apache Ignite. 
* WS clients to call REDCap, Synapse, and eGaIT REST services.
* Data set importers and transformations.
* Stats calculators with Akka streaming support.
* Machine learning service providing various classification, regression, and clustering routines backed by Apache Spark.

#### Installation

If you want to use *Ada Server* in your own project all you need is **Scala 2.11**. To pull the library you have to add the following dependency to *build.sbt*

```
"org.adada" %% "ada-server" % "0.8.1"
```

or to *pom.xml* (if you use maven)

```
<dependency>
    <groupId>org.adada</groupId>
    <artifactId>ada-server_2.11</artifactId>
    <version>0.8.1</version>
</dependency>
```

#### License

The project and all its source code is distributed under the terms of the <a href="https://www.apache.org/licenses/LICENSE-2.0.txt">Apache 2.0 license</a>. 

#### Acknowledgement and Support

Development of this project has been significantly supported by

* an FNR Grant (2015-2019, 2019-ongoing): *National Centre of Excellence in Research on Parkinson's Disease (NCER-PD)*: Phase I and Phase II

* a one-year MJFF Grant (2018-2019): *Scalable Machine Learning And Reservoir Computing Platform for Analyzing Temporal Data Sets in the Context of Parkinson’s Disease and Biomedicine*
<br/>

<a href="https://wwwen.uni.lu/lcsb"><img src="https://ada-discovery.github.io/images/logos/logoLCSB-long-230x97.jpg" width="184px"></a>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;<a href="https://www.fnr.lu"><img src="https://ada-discovery.github.io/images/logos/fnr_logo-350x94.png" width="280px"></a>&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;<a href="https://www.michaeljfox.org"><img src="https://ada-discovery.github.io/images/logos/MJFF-logo-resized-300x99.jpg" width="240px"></a>
