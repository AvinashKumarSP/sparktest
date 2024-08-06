package com.quantexa.assessments.customerAddresses

import com.quantexa.assessments.accounts.AccountAssessment.{AccountData, CustomerAccountOutput}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/***
  * A problem we have at Quantexa is where an address is populated with one string of text. In order to use this information
  * in the Quantexa product, this field must be "parsed".
  *
  * The purpose of parsing is to extract information from an entry - for example, to extract a forename and surname from
  * a 'full_name' field. We will normally place the extracted fields in a case class which will sit as an entry in the
  * wider case class; parsing will populate this entry.
  *
  * The parser on an address might yield the following "before" and "after":
  *
  * +-----------------------------------------+-------+--------------------+-------+--------+
  * |Address                                  |number |road                |city   |country |
  * +-----------------------------------------+-------+--------------------+-------+--------+
  * |109 Borough High Street, London, England |null   |null                |null   |null    |
  * +-----------------------------------------+-------+--------------------+-------+--------+
  *
  * +-----------------------------------------+-------+--------------------+-------+--------+
  * |Address                                  |number |road                |city   |country |
  * +-----------------------------------------+-------+--------------------+-------+--------+
  * |109 Borough High Street, London, England |109    |Borough High Street |London |England |
  * +-----------------------------------------+-------+--------------------+-------+--------+
  *
  *
  * You have been given addressData. This has been read into a DataFrame for you and then converted into a
  * Dataset of the given raw case class.
  *
  * You have been provided with a basic address parser which must be applied to the CustomerDocument model.
  *

  * Example Answer Format:
  *
  * val customerDocument: Dataset[CustomerDocument] = ???
  * customerDocument.show(1000,truncate = false)
  *
  * +----------+-----------+-------+--------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------+
  * |customerId|forename   |surname|accounts                                                            |address                                                                                                                               |
  * +----------+-----------+-------+--------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------+
  * |IND0001   |Christopher|Black  |[]                                                                  |[[ADR360,IND0001,762, East 14th Street, New York, United States of America,762, East 14th Street, New York, United States of America]]|
  * |IND0002   |Madeleine  |Kerr   |[[IND0002,ACC0155,323], [IND0002,ACC0262,60]]                       |[[ADR139,IND0002,675, Khao San Road, Bangkok, Thailand,675, Khao San Road, Bangkok, Thailand]]                                        |
  * |IND0003   |Sarah      |Skinner|[[IND0003,ACC0235,631], [IND0003,ACC0486,400], [IND0003,ACC0540,53]]|[[ADR318,IND0003,973, Blue Jays Way, Toronto, Canada,973, Blue Jays Way, Toronto, Canada]]                                            |
  * | ...
  */

object CustomerAddress extends App {

  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("CustomerAddress").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]

  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)
  val config = ConfigFactory.load()
  val addressDataInputPath = config.getString("app.addressDataInputPath")
  val customerAccountPath = config.getString("app.customerAccountPath")
  val customerDocumentPath = config.getString("app.customerDocumentPath")

  /**
   * Case class representing raw address data.
   *
   * @param addressId  Unique identifier for the address.
   * @param customerId Unique identifier for the customer.
   * @param address    The raw address string.
   */
  case class AddressRawData(
                             addressId: String,
                             customerId: String,
                             address: String
                           )

  /**
   * Case class representing parsed address data.
   *
   * @param addressId  Unique identifier for the address.
   * @param customerId Unique identifier for the customer.
   * @param address    The raw address string.
   * @param number     The house number extracted from the address.
   * @param road       The road name extracted from the address.
   * @param city       The city name extracted from the address.
   * @param country    The country name extracted from the address.
   */
  case class AddressData(
                          addressId: String,
                          customerId: String,
                          address: String,
                          number: Option[Int] = None,
                          road: Option[String] = None,
                          city: Option[String] = None,
                          country: Option[String] = None
                        )

  //Expected Output Format
  /**
   * Case class representing customer documents.
   *
   * @param customerId Unique identifier for the customer.
   * @param forename   Customer's forename.
   * @param surname    Customer's surname.
   * @param accounts   List of accounts associated with the customer.
   * @param addresses  List of addresses associated with the customer.
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
   * Parses a sequence of AddressData to extract number, road, city, and country.
   *
   * @param unparsedAddresses Sequence of AddressData with raw addresses.
   * @return Sequence of AddressData with parsed fields.
   */
  def addressParser(unparsedAddress: Seq[AddressData]): Seq[AddressData] = {
    unparsedAddress.map(address => {
      val split = address.address.split(", ")

      address.copy(
        number = Some(split(0).toInt),
        road = Some(split(1)),
        city = Some(split(2)),
        country = Some(split(3))
      )
    }
    )
  }

  val addressDF: DataFrame = spark.read.option("header", "true").csv(addressDataInputPath)
  val addressDS: Dataset[AddressRawData] = addressDF.as[AddressRawData]

  val customerAccountDS = spark.read.parquet(customerAccountPath).as[CustomerAccountOutput]

  // Transform raw address data to AddressData
  val addressData: Dataset[AddressData] = addressDS.map { addr =>
    AddressData(
      addressId = addr.addressId,
      customerId = addr.customerId,
      address = addr.address
    )
  }

  // Parse the addresses
  val parsedAddressData: Dataset[AddressData] = addressData.mapPartitions { iter =>
    addressParser(iter.toSeq).toIterator
  }

  // Join customers with parsed addresses
  val customerDocumentWithAddress: Dataset[CustomerDocument] = joinAndAggregateWithAddress(customerAccountDS, parsedAddressData)
  customerDocumentWithAddress.coalesce(1).write.mode("overwrite").parquet(customerDocumentPath)

  /**
   * Joins customer accounts with parsed address data and aggregates the results into a hierarchical structure.
   *
   * @param customerAccountDS The dataset of customer accounts.
   * @param parsedAddressData The dataset of parsed address data.
   * @return A dataset of CustomerDocument with aggregated address information.
   */
  def joinAndAggregateWithAddress(
                                   customerAccountDS: Dataset[CustomerAccountOutput],
                                   parsedAddressData: Dataset[AddressData]
                                 ): Dataset[CustomerDocument] = {

    // Perform a left outer join between customerAccountDS and parsedAddressData on customerId.
    val joinedDS: Dataset[(CustomerAccountOutput, Option[AddressData])] = customerAccountDS
      .joinWith(parsedAddressData, customerAccountDS("customerId") === parsedAddressData("customerId"), "left_outer")
      .map { case (customer, address) => (customer, Option(address)) }

    // Group the joined dataset by customer.
    val customerDocumentWithAddress: Dataset[CustomerDocument] = joinedDS
      .groupByKey { case (customer, _) => customer }
      .mapGroups { case (customer, groupIterator) =>
        val address = groupIterator.flatMap { case (_, addressOpt) => addressOpt }.toSeq
        CustomerDocument(
          customerId = customer.customerId,
          forename = customer.forename,
          surname = customer.surname,
          accounts = customer.accounts,
          address = address
        )
      }

    // Return the resulting dataset of CustomerDocument.
    customerDocumentWithAddress
  }
  //END GIVEN CODE
}