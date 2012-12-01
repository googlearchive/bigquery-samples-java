/*
 * Copyright (c) 2012 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.bigquery.samples;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeRequestUrl;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;


/**
 * @author Michael Manoochehri
 */
public class BigQueryJavaGettingStarted {

  /////////////////////////
  // USER GENERATED VALUES: you must fill in values specific to your application.
  //
  // Visit the Google API Console to create a Project and generate an
  // OAuth 2.0 Client ID and Secret (http://code.google.com/apis/console).
  // Then, add the Project ID below, and update the clientsecrets.json file
  // with your client_id and client_secret
  //
  /////////////////////////
  private static final String PROJECT_ID = "XXXXXXXXX";
  private static final String CLIENTSECRETS_LOCATION = "client_secrets.json";

  static GoogleClientSecrets clientSecrets = loadClientSecrets(CLIENTSECRETS_LOCATION);

  // Static variables for API scope, callback URI, and HTTP/JSON functions
  private static final List<String> SCOPES = Arrays.asList(BigqueryScopes.BIGQUERY);
  private static final String REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob";
  private static final HttpTransport TRANSPORT = new NetHttpTransport();
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  private static GoogleAuthorizationCodeFlow flow = null;

  /**
   * @param args
   * @throws IOException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    // Create a new BigQuery client authorized via OAuth 2.0 protocol
    Bigquery bigquery = createAuthorizedClient();

    // Print out available datasets in the "publicdata" project to the console
    listDatasets(bigquery, "publicdata");

    // Start a Query Job
    String querySql = "SELECT TOP(word, 50), COUNT(*) FROM publicdata:samples.shakespeare";
    JobReference jobId = startQuery(bigquery, PROJECT_ID, querySql);

    // Poll for Query Results, return result output
    Job completedJob = checkQueryResults(bigquery, PROJECT_ID, jobId);

    // Return and display the results of the Query Job
    displayQueryResults(bigquery, PROJECT_ID, completedJob);

  }

  /**
   * Creates an authorized BigQuery client service using the OAuth 2.0 protocol
   *
   * This method first creates a BigQuery authorization URL, then prompts the
   * user to visit this URL in a web browser to authorize access. The
   * application will wait for the user to paste the resulting authorization
   * code at the command line prompt.
   *
   * @return an authorized BigQuery client
   * @throws IOException
   */
  public static Bigquery createAuthorizedClient() throws IOException {

    String authorizeUrl = new GoogleAuthorizationCodeRequestUrl(
        clientSecrets,
        REDIRECT_URI,
        SCOPES).setState("").build();

    System.out.println("Paste this URL into a web browser to authorize BigQuery Access:\n" + authorizeUrl);

    System.out.println("... and type the code you received here: ");
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    String authorizationCode = in.readLine();

    // Exchange the auth code for an access token and refresh token
    Credential credential = exchangeCode(authorizationCode);

    return new Bigquery(TRANSPORT, JSON_FACTORY, credential);
  }

  /**
   * Display all BigQuery datasets associated with a project
   *
   * @param bigquery  an authorized BigQuery client
   * @param projectId a string containing the current project ID
   * @throws IOException
   */
  public static void listDatasets(Bigquery bigquery, String projectId)
      throws IOException {
    Datasets.List datasetRequest = bigquery.datasets().list(projectId);
    DatasetList datasetList = datasetRequest.execute();
    if (datasetList.getDatasets() != null) {
      List<DatasetList.Datasets> datasets = datasetList.getDatasets();
      System.out.println("Available datasets\n----------------");
      System.out.println(datasets.toString());
      for (DatasetList.Datasets dataset : datasets) {
        System.out.format("%s\n", dataset.getDatasetReference().getDatasetId());
      }
    }
  }

  /**
   * Creates a Query Job for a particular query on a dataset
   *
   * @param bigquery  an authorized BigQuery client
   * @param projectId a String containing the project ID
   * @param querySql  the actual query string
   * @return a reference to the inserted query job
   * @throws IOException
   */
  public static JobReference startQuery(Bigquery bigquery, String projectId,
                                        String querySql) throws IOException {
    System.out.format("\nInserting Query Job: %s\n", querySql);

    Job job = new Job();
    JobConfiguration config = new JobConfiguration();
    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
    config.setQuery(queryConfig);

    job.setConfiguration(config);
    queryConfig.setQuery(querySql);

    Insert insert = bigquery.jobs().insert(projectId, job);
    insert.setProjectId(projectId);
    JobReference jobId = insert.execute().getJobReference();

    System.out.format("\nJob ID of Query Job is: %s\n", jobId.getJobId());

    return jobId;
  }

  /**
   * Polls the status of a BigQuery job, returns Job reference if "Done"
   *
   * @param bigquery  an authorized BigQuery client
   * @param projectId a string containing the current project ID
   * @param jobId     a reference to an inserted query Job
   * @return a reference to the completed Job
   * @throws IOException
   * @throws InterruptedException
   */
  private static Job checkQueryResults(Bigquery bigquery, String projectId, JobReference jobId)
      throws IOException, InterruptedException {
    // Variables to keep track of total query time
    long startTime = System.currentTimeMillis();
    long elapsedTime;

    while (true) {
      Job pollJob = bigquery.jobs().get(projectId, jobId.getJobId()).execute();
      elapsedTime = System.currentTimeMillis() - startTime;
      System.out.format("Job status (%dms) %s: %s\n", elapsedTime,
          jobId.getJobId(), pollJob.getStatus().getState());
      if (pollJob.getStatus().getState().equals("DONE")) {
        return pollJob;
      }
      // Pause execution for one second before polling job status again, to
      // reduce unnecessary calls to the BigQUery API and lower overall
      // application bandwidth.
      Thread.sleep(1000);
    }
  }

  /**
   * Makes an API call to the BigQuery API
   *
   * @param bigquery     an authorized BigQuery client
   * @param projectId    a string containing the current project ID
   * @param completedJob to the completed Job
   * @throws IOException
   */
  private static void displayQueryResults(Bigquery bigquery,
                                          String projectId, Job completedJob) throws IOException {
    GetQueryResultsResponse queryResult = bigquery.jobs()
        .getQueryResults(
            projectId, completedJob
            .getJobReference()
            .getJobId()
        ).execute();
    List<TableRow> rows = queryResult.getRows();
    System.out.print("\nQuery Results:\n------------\n");
    for (TableRow row : rows) {
      for (TableCell field : row.getF()) {
      System.out.printf("%-50s", field.getV());
       }
      System.out.println();
    }
  }


  /**
   *  Helper to load client ID/Secret from file.
   *
   * @param clientSecretsLocation a path to a client_secrets.json file
   * @return a ClientSecrets object created from a client_secrets.json file
   */
  private static GoogleClientSecrets loadClientSecrets(String clientSecretsLocation) {
    try {
      clientSecrets = GoogleClientSecrets.load(new JacksonFactory(),
          BigQueryJavaGettingStarted.class.getResourceAsStream(clientSecretsLocation));
    } catch (Exception e) {
      System.out.println("Could not load client_secrets.json");
      e.printStackTrace();
    }
    return clientSecrets;
  }


  /**
   * Exchange the authorization code for OAuth 2.0 credentials.
   *
   * @return an authorized Google Auth flow
   */
  static Credential exchangeCode(String authorizationCode) throws IOException {
    GoogleAuthorizationCodeFlow flow = getFlow();
    GoogleTokenResponse response =
        flow.newTokenRequest(authorizationCode).setRedirectUri(REDIRECT_URI).execute();
    return flow.createAndStoreCredential(response, null);
  }


  /**
   * Build an authorization flow and store it as a static class attribute.
   *
   * @return a Google Auth flow object
   */
  static GoogleAuthorizationCodeFlow getFlow() {
    if (flow == null) {
      HttpTransport httpTransport = new NetHttpTransport();
      JacksonFactory jsonFactory = new JacksonFactory();

      flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport,
          jsonFactory,
          clientSecrets,
          SCOPES)
          .setAccessType("offline").setApprovalPrompt("force").build();
    }
    return flow;
  }


}