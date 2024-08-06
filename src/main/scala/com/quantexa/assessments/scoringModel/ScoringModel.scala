package com.quantexa.assessments.scoringModel

import com.quantexa.assessments.accounts.AccountAssessment.AccountData
import com.quantexa.assessments.customerAddresses.CustomerAddress.{AddressData, spark}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}


/***
  * Part of the Quantexa solution is to flag high risk countries as a link to these countries may be an indication of
  * tax evasion.
  *
  * For this question you are required to populate the flag in the ScoringModel case class where the customer has an
  * address in the British Virgin Islands.
  *
  * This flag must be then used to return the number of customers in the dataset that have a link to a British Virgin
  * Islands address.
  */

object ScoringModel extends App {


  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("ScoringModel").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]
  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)
  val config = ConfigFactory.load()
  val customerDocumentPath = config.getString("app.customerDocumentPath")

  /**
   * Represents a customer's document containing personal details, accounts, and addresses.
   *
   * @param customerId Unique ID for each customer.
   * @param forename Customer's first name.
   * @param surname Customer's last name.
   * @param accounts Sequence of accounts associated with the customer.
   * @param addresses Sequence of addresses associated with the customer.
   */
  case class CustomerDocument(
                               customerId: String,
                               forename: String,
                               surname: String,
                               //Accounts for this customer
                               accounts: Seq[AccountData],
                               //Addresses for this customer
                               address: Seq[AddressData]
                             )
  /**
   * Represents the scoring model for a customer, which includes a flag for linkage to the British Virgin Islands (BVI).
   *
   * @param customerId Unique ID for each customer.
   * @param forename Customer's first name.
   * @param surname Customer's last name.
   * @param accounts Sequence of accounts associated with the customer.
   * @param addresses Sequence of addresses associated with the customer.
   * @param linkToBVI Boolean flag indicating if the customer has an address in the British Virgin Islands.
   */
  case class ScoringModel(
                           customerId: String,
                           forename: String,
                           surname: String,
                           //Accounts for this customer
                           accounts: Seq[AccountData],
                           //Addresses for this customer
                           address: Seq[AddressData],
                           linkToBVI: Boolean
                         )


  val customerDocumentDS: Dataset[CustomerDocument] = spark.read.parquet(customerDocumentPath).as[CustomerDocument]

  /**
   * Function to check if an address is in the British Virgin Islands (BVI).
   *
   * @param addresses Sequence of AddressData.
   * @return Boolean indicating if any address is in the BVI.
   */
  def isAddressInBVI(addresses: Seq[AddressData]): Boolean = {
    addresses.exists(_.country.exists(_.equalsIgnoreCase("British Virgin Islands")))
  }

  // Create the ScoringModel dataset by mapping over the CustomerDocument dataset
  val scoringModelDS: Dataset[ScoringModel] = customerDocumentDS.map { customer =>
    val linkToBVI = isAddressInBVI(customer.address)
    ScoringModel(
      customerId = customer.customerId,
      forename = customer.forename,
      surname = customer.surname,
      accounts = customer.accounts,
      address = customer.address,
      linkToBVI = linkToBVI
    )
  }

  // Show the resulting ScoringModel dataset
  scoringModelDS.show(1000, truncate = false)

  // Count the number of customers linked to the British Virgin Islands
  val customersLinkedToBVI = scoringModelDS.filter(data => data.linkToBVI).count()
  println(s"Number of customers linked to the British Virgin Islands: $customersLinkedToBVI")

  //END GIVEN CODE

}
