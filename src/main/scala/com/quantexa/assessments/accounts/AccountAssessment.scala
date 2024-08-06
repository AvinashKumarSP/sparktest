package com.quantexa.assessments.accounts

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/***
 * A common problem we face at Quantexa is having lots of disjointed raw sources of data and having to aggregate and collect
 * relevant pieces of information into hierarchical case classes which we refer to as Documents. This exercise simplifies
 * the realities of this with just two sources of high quality data however reflects the types of transformations we must
 * perform.
 *
 * You have been given customerData and accountData. This has been read into a DataFrame for you and then converted into a
 * Dataset of the given case classes.
 *
 * If you run this App you will see the top 20 lines of the Datasets provided printed to console
 *
 * This allows you to use the Dataset API which includes .map/.groupByKey/.joinWith ect.
 * But a Dataset also includes all of the DataFrame functionality allowing you to use .join ("left-outer","inner" ect)
 *
 * https://spark.apache.org/docs/latest/sql-programming-guide.html
 *
 * The challenge here is to group, aggregate, join and map the customer and account data given into the hierarchical case class
 * customerAccountoutput. We would prefer you to write using the Scala functions instead of spark.sql() if you choose to perform
 * Dataframe transformations. We also prefer the use of the Datasets API.
 *
 * Example Answer Format:
 *
 * val customerAccountOutputDS: Dataset[customerAccountOutput] = ???
 * customerAccountOutputDS.show(1000,truncate = false)
 *
 * +----------+-----------+----------+---------------------------------------------------------------------+--------------+------------+-----------------+
 * |customerId|forename   |surname   |accounts                                                             |numberAccounts|totalBalance|averageBalance   |
 * +----------+-----------+----------+---------------------------------------------------------------------+--------------+------------+-----------------+
 * |IND0001   |Christopher|Black     |[]                                                                   |0             |0           |0.0              |
 * |IND0002   |Madeleine  |Kerr      |[[IND0002,ACC0155,323], [IND0002,ACC0262,60]]                        |2             |383         |191.5            |
 * |IND0003   |Sarah      |Skinner   |[[IND0003,ACC0235,631], [IND0003,ACC0486,400], [IND0003,ACC0540,53]] |3             |1084        |361.3333333333333|
 * ...
 */

object
AccountAssessment extends App {

  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("AccountAssignment").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]
  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)

  val config = ConfigFactory.load()
  val customerInputPath = config.getString("app.customerInputPath")
  val accountInputPath = config.getString("app.accountInputPath")
  val customerAccountPath = config.getString("app.customerAccountPath")

  /**
   * Case class representing a CustomerData.
   * @param customerId Unique identifier for the customer.
   * @param forename Forename of the customer.
   * @param surname Surname of the customer.
   */
  case class CustomerData(
                           customerId: String,
                           forename: String,
                           surname: String
                         )

  /**
   * Case class representing a AccountData.
   * @param customerId Unique identifier for the customer.
   * @param accountId Account id of the customer.
   * @param balance Balance of the account.
   */
  case class AccountData(
                          customerId: String,
                          accountId: String,
                          balance: Long
                        )

  //Expected Output Format
  /**
   * Case class representing the hierarchical output of a customer and their accounts.
   * @param customerId Unique identifier for the customer.
   * @param forename Forename of the customer.
   * @param surname Surname of the customer.
   * @param accounts Sequence of accounts belonging to the customer.
   * @param numberAccounts Total numberAccounts belonging to the customer.
   * @param totalBalance Accounts totalBalance belonging to the customer.
   * @param averageBalance Accounts averageBalance belonging to the customer.
   */
  case class CustomerAccountOutput(
                                    customerId: String,
                                    forename: String,
                                    surname: String,
                                    //Accounts for this customer
                                    accounts: Seq[AccountData],
                                    //Statistics of the accounts
                                    numberAccounts: BigInt,
                                    totalBalance: Long,
                                    averageBalance: Double
                                  )

  //Create DataFrames of sources
  val customerDF: DataFrame = spark.read.option("header","true")
    .csv(customerInputPath)
  val accountDF = spark.read.option("header","true")
    .csv(accountInputPath)


  //Create Datasets of sources
  val customerDS: Dataset[CustomerData] = customerDF
    .as[CustomerData]
  val accountDS: Dataset[AccountData] = accountDF
    .withColumn("balance",'balance.cast("long")).as[AccountData]

  // Join datasets and aggregate
  val customerAccountOutputDS: Dataset[CustomerAccountOutput] = joinAndAggregateDatasets(customerDS, accountDS)
  customerAccountOutputDS.coalesce(1).write.mode("overwrite").parquet(customerAccountPath)

  /**
   * Joins the customer and account datasets on the customer ID and aggregates the accounts for each customer.
   * @param customerDS Dataset of customers.
   * @param accountDS Dataset of account data.
   * @return Dataset of CustomerAccountOutput with aggregated accounts for each customer.
   */
  def joinAndAggregateDatasets(customerDS: Dataset[CustomerData], accountDS: Dataset[AccountData]): Dataset[CustomerAccountOutput] = {

    // Perform a left outer join between customers and accounts
    val joinedDS: Dataset[(CustomerData, Option[AccountData])] = customerDS
      .joinWith(accountDS, customerDS("customerId") === accountDS("customerId"), "left_outer")
      .map { case (customer, account) => (customer, Option(account)) }

    // Join customer and account datasets
    val groupedDS: Dataset[CustomerAccountOutput] = joinedDS
      .groupByKey { case (customer, _) => customer }
      .mapGroups { case (customer, groupIterator) =>
        val accounts = groupIterator.flatMap { case (_, accountOpt) => accountOpt }.toSeq
        val numberAccounts = accounts.size
        val totalBalance = accounts.map(_.balance).sum
        val averageBalance = if (numberAccounts > 0) totalBalance.toDouble / numberAccounts else 0.0
        CustomerAccountOutput(
          customerId = customer.customerId,
          forename = customer.forename,
          surname = customer.surname,
          accounts = accounts,
          numberAccounts = numberAccounts,
          totalBalance = totalBalance,
          averageBalance = averageBalance
        )
      }

    groupedDS
  }

  //END GIVEN CODE



}