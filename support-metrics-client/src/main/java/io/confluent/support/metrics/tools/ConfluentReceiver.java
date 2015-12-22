/**
 * Copyright 2015 Confluent Inc.
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
package io.confluent.support.metrics.tools;


import java.io.IOException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import io.confluent.support.metrics.SupportConfig;
import io.confluent.support.metrics.SupportKafkaMetricsBasic;
import io.confluent.support.metrics.SupportKafkaMetricsEnhanced;
import io.confluent.support.metrics.serde.AvroDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.net.InetAddress;
import java.util.Set;
import java.util.HashSet;
import java.util.Calendar;
import java.text.SimpleDateFormat;

/**
 * Connects to S3 bucket and provides various stats on the data
 * Note persona running the application must have AWS credentials set up
 * otherwise access to S3 will be denied
 */
public class ConfluentReceiver {
  private static String bucketName = null;
  private final AmazonS3 s3client;
  private List<SupportKafkaMetricsEnhanced> fullMetricsContainer = new ArrayList<>();
  private List<SupportKafkaMetricsBasic> basicMetricsContainer = new ArrayList<>();
  private Set<String> anonymousHostNames = new HashSet<>();
  private Map<String, List<SupportKafkaMetricsEnhanced>> customerMap = new HashMap<>();

  ConfluentReceiver() {
    s3client = new AmazonS3Client(new ProfileCredentialsProvider());
  }

  /**
   * Gets the date from the key.
   * Assumes key of the form customerID/ip/year/month/day/rest-of-key
   * @param key
   * @return
   */
  static protected Calendar getDate(String key) {
    if (key == null || key.isEmpty()) {
      return null;
    }
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    Calendar date = Calendar.getInstance();
    String[] keyParts = key.split("/");
    if (keyParts.length < 5) {
      return null;
    }
    String year = keyParts[2];
    String month = keyParts[3];
    String day = keyParts[4];
    try {
      date.setTime(format.parse(year + "-" + month + "-" + day));
    } catch (java.text.ParseException e) {
      System.err.println("ParseException: " + e.getMessage());
      return null;
    }
    return date;
  }

  /**
   * Returns the ip address (v4) from a key
   * Assumes key of the form customerID/ip/year/month/day/rest-of-key
   * @param key S3 bucket key
   * @return IP address or empty string if no IP found
   */
  static private String getIpAddress(String key) {
    if (key == null || key.isEmpty()) {
      return "";
    }
    String[] keyParts = key.split("/");
    if (keyParts.length < 2) {
      return "";
    }
    String ip = keyParts[1];
    if (InetAddressValidator.getInstance().isValid(ip)) {
      return ip;
    } else {
      return "";
    }
  }

  /**
   * Returns the hostname from a key
   * Assumes key of the form customerID/ip/year/month/day/rest-of-key
   * @param key S3 bucket key
   * @return hostname or empty string if no hostname can be found
   */
  static protected String getHostname(String key) {
    if (key == null || key.isEmpty()) {
      return "";
    }
    String ip = getIpAddress(key);
    String hostname = "";
    if (ip != null && !ip.isEmpty()) {
      try {
        hostname = InetAddress.getByName(ip).getHostName();
      } catch (java.net.UnknownHostException e) {
        return "";
      }
    }
    return hostname;
  }

  /**
   * Gets the customer ID form a key
   * Assumes key of the form customerID/ip/year/month/day/rest-of-key
   * @param key
   * @return Customer ID or empty string if ID cannot be found
   */
  static protected String getCustomerID(String key) {
    if (key == null || key.isEmpty()) {
      return "";
    }
    String[] keyParts = key.split("/");
    if (SupportConfig.isSyntacticallyCorrectCustomerId(keyParts[0])) {
      return keyParts[0];
    } else {
      return "";
    }
  }


  /**
   * Go over all anonymous metrics and print stats
   * Note that processMatchingObjects should be called first to collect all the metrics from S3
   */
  private void printAnonymousStats() {
    Iterator listIt = basicMetricsContainer.iterator();
    Map<String, Long> kafkaVersions = new HashMap<>();
    Map<String, Long> confluentVersions = new HashMap<>();
    System.out.println("Customer:\t" + SupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT);
    while (listIt.hasNext()) {
      SupportKafkaMetricsBasic metric = (SupportKafkaMetricsBasic) listIt.next();
      long prevValue = 0;
      if (kafkaVersions.containsKey(metric.getKafkaVersion())) {
        prevValue = kafkaVersions.get(metric.getKafkaVersion());
      }
      kafkaVersions.put(metric.getKafkaVersion(), prevValue + 1);
      prevValue = 0;
      if (confluentVersions.containsKey(metric.getConfluentPlatformVersion())) {
        prevValue = confluentVersions.get(metric.getConfluentPlatformVersion());
      }

      confluentVersions.put(metric.getConfluentPlatformVersion(), prevValue + 1);
    }
    Iterator kIt = kafkaVersions.keySet().iterator();
    while(kIt.hasNext()) {
      String key = (String)kIt.next();
      System.out.println("\t\tKafka version " + key + ": " + kafkaVersions.get(key) + " instances");
    }
    Iterator cIt = confluentVersions.keySet().iterator();
    while(cIt.hasNext()) {
      String key = (String)cIt.next();
      System.out.println("\t\tConfluent version " + key + ": " + confluentVersions.get(key) + " instances");
    }
    Iterator hIt = anonymousHostNames.iterator();
    System.out.println("\t\tHostnames: " + anonymousHostNames.size() + " unique values");
    while(hIt.hasNext()) {
      String hostname = (String)hIt.next();
      System.out.println("\t\t\t" + hostname);
    }
  }

  /**
   * Go over all customer metrics and print stats
   * Note that processMatchingObjects should be called first to collect all the metrics from S3
   * Prints:
   * - number of customers
   * - for each customer
   *  - max number of topics
   *  - max bytes written
   */
  private void printCustomerStats() {
    Iterator it = customerMap.keySet().iterator();
    int numberCustomers = 0;
    while (it.hasNext()) {
      numberCustomers++;
      long maxTopics = 0L;
      long maxBytesWritten = 0L;
      long maxBytesRead = 0L;
      String customerID = (String)it.next();
      System.out.println("Customer:\t" + customerID);
      List<SupportKafkaMetricsEnhanced> metricsList = (List<SupportKafkaMetricsEnhanced>)customerMap.get(customerID);
      Iterator listIt = metricsList.iterator();
      while (listIt.hasNext()) {
        SupportKafkaMetricsEnhanced metric = (SupportKafkaMetricsEnhanced)listIt.next();
        if (metric.getBrokerMetrics().getBrokerStatistics().getWrittenBytes() > maxBytesWritten) {
          maxBytesWritten = metric.getBrokerMetrics().getBrokerStatistics().getWrittenBytes();
        }
        if (metric.getClusterMetrics().getNumberTopicsZk() > maxTopics) {
          maxTopics = metric.getClusterMetrics().getNumberTopicsZk();
        }
        if (metric.getBrokerMetrics().getBrokerStatistics().getReadBytes() > maxBytesRead) {
          maxBytesRead = metric.getBrokerMetrics().getBrokerStatistics().getReadBytes();
        }
      }
      System.out.println("\t\tMax topics: " + maxTopics);
      System.out.println("\t\tMax bytes written: " + maxBytesWritten);
      System.out.println("\t\tMax bytes read: " + maxBytesRead);
    }
    System.out.println("\nTotal # customers: " + numberCustomers);
  }

  /**
   * Adds a single basic metric to a list of collected metrics
   * @param key S3 bucket key
   * @param metrics
   * @throws IOException
   */
  private void processBasicMetrics(String key, byte[] metrics) throws IOException {
    AvroDeserializer decoder = new AvroDeserializer();
    String hostName = getHostname(key);
    if (!hostName.isEmpty()) {
      anonymousHostNames.add(hostName);
    }
    SupportKafkaMetricsBasic[] container = decoder.deserialize(SupportKafkaMetricsBasic.class, metrics);
    basicMetricsContainer.add(container[0]);
  }

  /**
   * Adds a single enhanced metric to a list of collected metrics
   * @param key S3 bucket key
   * @param metrics
   * @throws IOException
   */
  private void processFullMetrics(String key, byte[] metrics) throws IOException {
    String customerId = getCustomerID(key);
    if (customerId.isEmpty()) {
      return;
    }
    AvroDeserializer decoder = new AvroDeserializer();
    List<SupportKafkaMetricsEnhanced> list = null;
    SupportKafkaMetricsEnhanced[] container = decoder.deserialize(SupportKafkaMetricsEnhanced.class, metrics);
    fullMetricsContainer.add(container[0]);
    list = customerMap.get(customerId);
    if (list == null) {
      list = new ArrayList<SupportKafkaMetricsEnhanced>();
      customerMap.put(customerId, list);
    }
    list.add(container[0]);
  }

  /**
   * Received an object as a byte array from AWS
   * @param bucketName
   * @param key
   */
  private void processObject(String bucketName, String key) {
    System.out.println("Processing object with key " + key);
    String customerId = getCustomerID(key);
    if (customerId.isEmpty()) {
      return;
    }
    S3Object object = s3client.getObject(new GetObjectRequest(bucketName, key));
    byte[] byteArray = null;
    try {
      byteArray = IOUtils.toByteArray(object.getObjectContent());
      if (SupportConfig.isAnonymousUser(customerId)) {
        processBasicMetrics(key, byteArray);
      } else if (SupportConfig.isConfluentCustomer(customerId) && !SupportConfig.isTestUser(customerId)) {
        processFullMetrics(key, byteArray);
      }
    } catch (IOException e) {
      System.err.println("IOException: " + e.getMessage());
      return;
    }
  }

  /**
   * Goes over all objects that match a prefix and initiates their processing
   * @param bucketName S3 bucketname
   * @param prefix S3 object key prefix, e.g., "anonymous"
   * @param startDate starting date.
   * @param endDate ending date.
   */
  public void processMatchingObjects(String bucketName, String prefix, Calendar startDate, Calendar endDate) {
    System.out.println("Processing objects");
    try {
      ListObjectsRequest listObjectsRequest = null;

      if (prefix != null && !prefix.isEmpty()) {
        listObjectsRequest = new ListObjectsRequest()
            .withBucketName(bucketName)
            .withPrefix(prefix);
      } else {
        listObjectsRequest = new ListObjectsRequest()
            .withBucketName(bucketName);
      }
      ObjectListing objectListing;
      do {
        objectListing = s3client.listObjects(listObjectsRequest);
        for (S3ObjectSummary objectSummary :
            objectListing.getObjectSummaries()) {
          Calendar date = getDate(objectSummary.getKey());
          if (date == null) {
            System.err.println("No Date encoded in key " + objectSummary.getKey());
            continue;
          }
          if (date.equals(startDate) || date.equals(endDate) ||
              (date.after(startDate) && date.before(endDate))) {
            System.out.println("Processing - " + objectSummary.getKey() + "  " +
                "(size = " + objectSummary.getSize() +
                ")");
            processObject(bucketName, objectSummary.getKey());
          }
        }
        listObjectsRequest.setMarker(objectListing.getNextMarker());
      } while (objectListing.isTruncated());
    } catch (AmazonServiceException ase) {
      System.out.println("Caught an AmazonServiceException, " +
          "which means your request made it " +
          "to Amazon S3, but was rejected with an error response " +
          "for some reason.");
      System.out.println("Error Message:    " + ase.getMessage());
      System.out.println("HTTP Status Code: " + ase.getStatusCode());
      System.out.println("AWS Error Code:   " + ase.getErrorCode());
      System.out.println("Error Type:       " + ase.getErrorType());
      System.out.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
      System.out.println("Caught an AmazonClientException, " +
          "which means the client encountered " +
          "an internal error while trying to communicate" +
          " with S3, " +
          "such as not being able to access the network.");
      System.out.println("Error Message: " + ace.getMessage());
    }
  }

  public void simpleStats(String bucketName, String prefix, Calendar startDate, Calendar endDate) {

    // process all objects first
    processMatchingObjects(bucketName, prefix, startDate, endDate);

    // get some stats
    printAnonymousStats();
    printCustomerStats();
  }

  /**
   * Lists all the objects of a bucket on stdout
   * @param bucketName S3 bucketname
   * @param prefix S3 object key prefix, e.g., "anonymous"
   * @param startDate starting date.
   * @param endDate ending date.
   */
  public void listObjects(String bucketName, String prefix, Calendar startDate, Calendar endDate) {
    try {
      System.out.println("Listing objects");
      ListObjectsRequest listObjectsRequest = null;

      if (prefix != null && !prefix.isEmpty()) {
        listObjectsRequest = new ListObjectsRequest()
            .withBucketName(bucketName)
            .withPrefix(prefix);
      } else {
        listObjectsRequest = new ListObjectsRequest()
            .withBucketName(bucketName);
      }
      ObjectListing objectListing;
      do {
        objectListing = s3client.listObjects(listObjectsRequest);
        for (S3ObjectSummary objectSummary :
            objectListing.getObjectSummaries()) {
          Calendar date = getDate(objectSummary.getKey());
          if (date == null) {
            System.err.println("No Date encoded in key " + objectSummary.getKey());
            continue;
          }
          if (date.equals(startDate) || date.equals(endDate) ||
              (date.after(startDate) && date.before(endDate))) {
            System.out.println(" - " + objectSummary.getKey() + "  " +
                "(size = " + objectSummary.getSize() +
                ")");
          }
        }
        listObjectsRequest.setMarker(objectListing.getNextMarker());
      } while (objectListing.isTruncated());
    } catch (AmazonServiceException ase) {
      System.out.println("Caught an AmazonServiceException, " +
          "which means your request made it " +
          "to Amazon S3, but was rejected with an error response " +
          "for some reason.");
      System.out.println("Error Message:    " + ase.getMessage());
      System.out.println("HTTP Status Code: " + ase.getStatusCode());
      System.out.println("AWS Error Code:   " + ase.getErrorCode());
      System.out.println("Error Type:       " + ase.getErrorType());
      System.out.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
      System.out.println("Caught an AmazonClientException, " +
          "which means the client encountered " +
          "an internal error while trying to communicate" +
          " with S3, " +
          "such as not being able to access the network.");
      System.out.println("Error Message: " + ace.getMessage());
    }
  }

  private static void showUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "ConfluentReceiver", options );
  }

  public static void main(String[] args) throws IOException {
    Options options = new Options();
    options.addOption("bucket", true, "S3 bucket name [REQUIRED]");
    options.addOption("help", false, "Show usage");
    options.addOption("list", false, "List all objects in the bucket (WARNING: could be slow, limit by date range)");
    options.addOption("listprefix", true, "List all objects in the bucket that match a prefix (WARNING: could be slow, limit by date range)");
    options.addOption("stats", true, "Provide basic statistics for objects that match a prefix (WARNING: could be slow, limit by date range)");
    options.addOption("startdate", true, "Starting date in format yyyy-mm-dd [REQUIRED]");
    options.addOption("enddate", true, "Ending date in format yyyy-mm-dd [REQUIRED]");
    ConfluentReceiver receiver = new ConfluentReceiver();
    // create the command line parser
    CommandLineParser parser = new DefaultParser();
    // parse the command line arguments
    try {
      Calendar startDate = Calendar.getInstance();
      Calendar endDate = Calendar.getInstance();
      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
      CommandLine line = parser.parse(options, args);
      if (line.getOptions() == null || line.getOptions().length == 0) {
        showUsage(options);
        return;
      }
      if (line.hasOption("bucket")) {
        bucketName = line.getOptionValue("bucket");
      } else {
        System.err.println("No bucket specified. Please correct");
        showUsage(options);
        return;
      }
      if (line.hasOption("startdate")) {
        startDate.setTime(format.parse(line.getOptionValue("startdate")));
      } else {
        System.err.println("No start date specified. Please correct");
        showUsage(options);
        return;
      }
      if (line.hasOption("enddate")) {
        endDate.setTime(format.parse(line.getOptionValue("enddate")));
      } else {
        System.err.println("No end date specified. Please correct");
        showUsage(options);
        return;
      }
      if (startDate.after(endDate)) {
        System.err.println("End date is before start date. Please correct.");
        showUsage(options);
        return;
      }
      if (line.hasOption("list")) {
        receiver.listObjects(bucketName, null, startDate, endDate);
      }
      if (line.hasOption("listprefix")) {
        receiver.listObjects(bucketName, line.getOptionValue("listprefix"), startDate, endDate);
      }
      if (line.hasOption("help")) {
        showUsage(options);
      }
      if (line.hasOption("stats")) {
        receiver.simpleStats(bucketName, line.getOptionValue("stats"), startDate, endDate);
      }

    } catch (ParseException exp) {
      System.out.println("Unexpected exception:" + exp.getMessage());
    } catch (java.text.ParseException exp) {
      System.out.println("Unexpected exception:" + exp.getMessage());
    }

  }
}