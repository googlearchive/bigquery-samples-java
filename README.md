# Getting Started with BigQuery and the Google Java API Client library

Google's BigQuery Service features a REST-based API that allows developers to create applications to run ad-hoc queries on massive datasets. This sample Java command-line application demonstrates how to access the BigQuery API using the Google Java API Client Libraries. For more information, read the [Getting Started with BigQuery and the Google Java API Client library][1] codelab.

## Quickstart

Install [Maven](http://maven.apache.org/).

In the `BigQueryJavaGettingStarted` class, update the PROJECT_ID and CLIENTSECRETS_LOCATION values with your project number and the path to your downloaded client secrets file, as described [here](https://developers.google.com/bigquery/articles/gettingstartedwithjava).

Then build your project with:

	mvn package

You can then run it via:

	mvn exec:java

## Products
- [Google BigQuery][2]

## Language
- [Java][3]

## Dependencies
- [Google APIs Client Library for Java][4]

[1]: https://developers.google.com/bigquery/articles/gettingstartedwithjava
[2]: https://developers.google.com/bigquery
[3]: https://java.com
[4]: http://code.google.com/p/google-api-java-client/

