import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import java.util.Scanner

object Project1Ex {

  def main(args: Array[String]): Unit = {
    // This block of code is all necessary for spark/hive/hadoop
    System.setSecurityManager(null)
    System.setProperty(
      "hadoop.home.dir",
      "C:\\hadoop\\"
    ) // change if winutils.exe is in a different bin folder
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Project1Ex") // Change to whatever app name you want
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val hiveCtx = new HiveContext(sc)
    import hiveCtx.implicits._

    //This block to connect to mySQL
    val driver = "com.mysql.cj.jdbc.Driver"
    val url =
      "jdbc:mysql://localhost:3306/p1" // Modify for whatever port you are running your DB on
    val username = "root"
    val password = "database1234" // Update to include your password
    var connection: Connection = null

    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)

    var scanner = new Scanner(System.in)


    //user enters main menu, mainMenu() method is run
    var endProgram = false
    while (!endProgram) {
      mainMenu(scanner, connection)
    }
    //mainMenu shows menuchoices and uses match case to handle user input and errors
    def mainMenu(scanner: Scanner, connection: Connection): Unit = {

      menutext()
      var menuChoice = scanner.nextInt()
      scanner.nextLine()

      menuChoice match {
        case 1 =>
          createAccount(scanner, connection)
        case 2 =>
          userLogin(scanner, connection)
        case 3 =>
          adminLogin(scanner, connection)
        case 4 =>
          endProgram = true
        case _ =>
          println("Invalid choice try again")
      }

    }
    //quick menutext method to keep clean code
    def menutext(): Unit = {
      println("*********************")
      println("What's that Pokemon!")
      println("Choose an option")
      println("1. Create an account")
      println("2. User Login")
      println("3. Admin Login")
      println("4. Exit")
      println("*********************")
    }
    //createAccount method. only user accounts will be created, one admin account will already exist.
    def createAccount(scanner: Scanner, connection: Connection): Unit = {
      println("*********************")
      println("Thanks for being a new user.")
      println("Please enter your desired username")
      println("*********************")
      var username = scanner.nextLine()
      println("*********************")
      println("Please enter your desired password")
      var password = scanner.nextLine()
      println("*********************")
      println("Confirm your desired password")
      var passwordConfirm = scanner.nextLine()

      //checking to see if password matches confirmation, if successful brings user to usermenu
      if (password == passwordConfirm) {
        println("Account successfully created")
        println("*********************")
        userMenu(scanner, connection, username, password)

        //if unsuccessful, recursively calls createAccount to allow user to try again
      } else if (password != passwordConfirm) {
        println("*********************")
        println("Passwords don't match, try again")
        createAccount(scanner, connection)
        //same as above, different case
      } else if (password == null) {
        println("Password can't be blank")
        createAccount(scanner, connection)
      }
      val statement = connection.createStatement()
      val resultSet = statement.executeUpdate(
        s"INSERT INTO useraccounts(username, password) VALUES ('$username', '$password');"
      )
      // val resultSet stuff here yadda yadda yadda

    }
    //userlogin method will check userpassword vs existing userdata
    def userLogin(scanner: Scanner, connection: Connection): Unit = {
      println("*********************")
      println("Please enter your username")
      var username = scanner.nextLine()
      println("Please type your password")
      var password = scanner.nextLine()
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(
        s"SELECT COUNT(*) FROM useraccounts WHERE username ='$username' AND password = '$password';"
      )
      while (resultSet.next()) {
        if (resultSet.getString(1) == "1") {
          println("Login Successful")
          userMenu(scanner, connection, username, password)
        } else {
          println("Username/password combo not found. Try again!")
          userLogin(scanner, connection)
        }
      }

    }

    def userMenu(
        scanner: Scanner,
        connection: Connection,
        username: String,
        password: String
    ): Unit = {
      println("*********************")
      println("Welcome to Pokelytics!")
      println("Choose an option")
      println("1. Query PokeData")
      println("2. Update Login Info")
      println("3. Main Menu")
      println("4. Exit")
      println("*********************")

      var menuChoice = scanner.nextInt()
      scanner.nextLine()
      menuChoice match {
        case 1 =>
          queryMenu(scanner, connection)
        case 2 =>
          updateLogin(scanner, connection, username, password)
        case 3 =>
          mainMenu(scanner, connection)
        case 4 =>
          endProgram = true
      }
    }

    def adminLogin(scanner: Scanner, connection: Connection): Unit = {
      println("********************")
      println("Enter your username")
      var adminUserName = scanner.nextLine()
      println("Enter your password")
      var adminPassword = scanner.nextLine()
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(
        s"SELECT COUNT(*) FROM adminaccounts WHERE username = '$adminUserName' AND password = '$adminPassword'"
      )
      while (resultSet.next()) {
        if (resultSet.getString(1) == "1") {
          println("Login Successful")
          adminMenu(scanner, connection, adminUserName, adminPassword)
        } else {
          println("Username/password combo not found. Try again!")
          adminLogin(scanner, connection)
        }
      }
      println("********************")

    }

    def adminMenu(
        scanner: Scanner,
        connection: Connection,
        adminUserName: String,
        adminPassword: String
    ): Unit = {
      println("*********************")
      println("Welcome to Pokelytics!")
      println("Choose an option")
      println("1. Query PokeData")
      println("2. Update Login Info")
      println("3. View existing users")
      println("4. Delete an existing user")
      println("5. Main Menu")
      println("6. Exit")
      println("*********************")

      var menuChoice = scanner.nextInt()
      scanner.nextLine()
      menuChoice match {
        case 1 =>
          queryMenu(scanner, connection)
        case 2 =>
          updateAdminLogin(scanner, connection, adminUserName, adminPassword)
        case 3 =>
          viewExistingUsers(scanner, connection, adminUserName, adminPassword)
        case 4 =>
          deleteUser(scanner, connection, adminUserName, adminPassword)
        case 5 =>
          mainMenu(scanner, connection)
        case 6 =>
          endProgram = true
      }
    }
    def queryMenu(scanner:Scanner, connection:Connection): Unit = {
      // insertPokemonData(hiveCtx)
      println("********************")
      println("A Pokemon Trainer's best weapon is knowledge! What would you like to know?")
      println("1. Pokemon with highest base stats.")
      println("2. Pokemon with lowest base stats.")
      println("3. Count of different variations and what type has the most")
      println("4. Pokemon with most different variations")
      println("5. Amount of Pokemon with type as primary")
      println("6. Amount of Pokemon with type as secondary")
      println("7. Go Back")
      println("********************")
      var menuChoice = scanner.nextInt()
      scanner.nextLine()
      menuChoice match {
        case 1 =>
          highestBasePokemon(hiveCtx)
          queryMenu(scanner,connection)
        case 2 =>
          lowestBasePokemon(hiveCtx)
          queryMenu(scanner,connection)
        case 3 =>
          pokemonVariationCount(hiveCtx)
          queryMenu(scanner,connection)
        case 4 =>
          mostVariationsPokemon(hiveCtx)
          queryMenu(scanner,connection)
        case 5 =>
          pokemonPrimTypeCount(hiveCtx)
          queryMenu(scanner,connection)
        case 6 =>
          pokemonSecoTypeCount(hiveCtx)
          queryMenu(scanner,connection)
        case 7 =>
          mainMenu(scanner, connection)
        case _ =>
          println("Invalid Choice, Try Again")
          queryMenu(scanner,connection)

      }

    }
    def updateLogin(
        scanner: Scanner,
        connection: Connection,
        username: String,
        password: String
    ): Unit = {
      println("********************")
      println("Would you like to update your username or password?")
      println("1. Username")
      println("2. Password")
      println("3. Go Back")
      println(username, password, "this is the updatelogin username consolelog")
      var menuChoice = scanner.nextInt()
      scanner.nextLine()
      val statement = connection.createStatement()
      menuChoice match {
        case 1 =>
          println("Enter a new username")
          var newUsername = scanner.nextLine()

          val resultSet = statement.executeUpdate(
            s"UPDATE useraccounts SET username = '$newUsername' WHERE username = '$username'"
          )
          username == newUsername

          updateLogin(scanner, connection, username, password)
        case 2 =>
          println("Enter a new password")
          var newPassword = scanner.nextLine()
          println("Confirm your new password")
          var newPasswordConfirm = scanner.nextLine()

          if (newPassword == newPasswordConfirm) {
            println("Password successfully updated")
            println("*********************")
            password == newPassword
            //if unsuccessful, recursively calls createAccount to allow user to try again
          } else if (newPassword != newPasswordConfirm) {
            println("*********************")
            println("Passwords don't match, try again")
            updateLogin(scanner, connection, username, password)
            //same as above, different case
          } else if (newPassword == null) {
            println("New password can't be blank")
            updateLogin(scanner, connection, username, password)
          }

          val resultSet2 = statement.executeUpdate(
            s"UPDATE useraccounts SET password = '$newPassword' WHERE password = '$password'"
          )

          updateLogin(scanner, connection, username, password)
        case 3 =>
          println("Returning to User Menu")
          userMenu(scanner, connection, username, password)
        case _ =>
          println("Invalid choice, try again")

      }
      println("********************")
    }
    def updateAdminLogin(
        scanner: Scanner,
        connection: Connection,
        adminUserName: String,
        adminPassword: String
    ): Unit = {
      println("********************")
      println("Would you like to update your username or password?")
      println("1. Username")
      println("2. Password")
      println("3. Go Back")
      var menuChoice = scanner.nextInt()
      scanner.nextLine()
      val statement = connection.createStatement()
      menuChoice match {
        case 1 =>
          println("Enter a new username")
          var newUsername = scanner.nextLine()

          val resultSet = statement.executeUpdate(
            s"UPDATE adminaccounts SET username = '$newUsername' WHERE username = '$adminUserName'"
          )
          adminUserName == newUsername

          updateAdminLogin(scanner, connection, adminUserName, adminPassword)
        case 2 =>
          println("Enter a new password")
          var newPassword = scanner.nextLine()
          println("Confirm your new password")
          var newPasswordConfirm = scanner.nextLine()

          if (newPassword == newPasswordConfirm) {
            println("Password successfully updated")
            println("*********************")
            adminPassword == newPassword
            //if unsuccessful, recursively calls createAccount to allow user to try again
          } else if (newPassword != newPasswordConfirm) {
            println("*********************")
            println("Passwords don't match, try again")
            updateAdminLogin(scanner, connection, adminUserName, adminPassword)
            //same as above, different case
          } else if (newPassword == null) {
            println("New password can't be blank")
            updateAdminLogin(scanner, connection, adminUserName, adminPassword)
          }

          val resultSet2 = statement.executeUpdate(
            s"UPDATE adminaccounts SET password = '$newPassword' WHERE password = '$adminPassword'"
          )

          updateAdminLogin(scanner, connection, adminUserName, adminPassword)
        case 3 =>
          println("Returning to User Menu")
          adminMenu(scanner, connection, adminUserName, adminPassword)
        case _ =>
          println("Invalid choice, try again")

      }
      println("********************")
    }
    def viewExistingUsers(
        scanner: Scanner,
        connection: Connection,
        adminUserName: String,
        adminPassword: String
    ): Unit = {
      val statement = connection.createStatement()
      println("********************")
      val resultSet = statement.executeQuery("SELECT * FROM useraccounts")
      while (resultSet.next()) {
        print(resultSet.getString(1) + " " + resultSet.getString(2))
        println()
      }
      println("********************")
      adminMenu(scanner, connection, adminUserName, adminPassword)
    }

    def deleteUser(
        scanner: Scanner,
        connection: Connection,
        adminUserName: String,
        adminPassword: String
    ): Unit = {
      val statement = connection.createStatement()
      println("********************")
      println("Which user would you like to delete?")
      var deleteUser = scanner.nextLine()
      val resultSet = statement.executeUpdate(
        s"DELETE FROM useraccounts WHERE username = '$deleteUser' "
      )
      println(s"Successfully deleted '$deleteUser'")
      println("********************")
      adminMenu(scanner, connection, adminUserName, adminPassword)
    }

    def insertPokemonData(hiveCtx: HiveContext): Unit = {
      //hiveCtx.sql("LOAD DATA LOCAL INPATH 'input/set1EnUS.txt' OVERWRITE INTO TABLE pokemonData")
      //hiveCtx.sql("INSERT INTO pokemonData VALUES (1, 'date', 'California', 'US', 'update', 10, 1, 0)")

      // This statement creates a DataFrameReader from your file that you wish to pass in. We can infer the schema and retrieve
      // column names if the first row in your csv file has the column names. If not wanted, remove those options. This can
      // then be
      val output = hiveCtx.read
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        // .load("input/covid_19_data.csv")
        .load("input/PokemonDb.csv")
      output.limit(15).show() // Prints out the first 15 lines of the dataframe
      // output.printSchema
      println("output limit")
      // output.registerTempTable("data2") // This will create a temporary table from your dataframe reader that can be used for queries.

      // These next three lines will create a temp view from the dataframe you created and load the data into a permanent table inside
      // of Hadoop. Thus, we will have data persistence, and this code only needs to be ran once. Then, after the initializatio, this
      // code as well as the creation of output will not be necessary.

      //   hiveCtx.sql("CREATE TABLE IF NOT EXISTS pokemonData("assets/0/gameAbsolutePath" STRING, "assets/0/fullAbsolutePath" STRING, "region" STRING, "regionRef" STRING, "regions/0" STRING, "regionRefs/0" STRING, attack INT, cost INT, health INT, "description" STRING, "descriptionRaw" STRING, "levelupDescription" STRING, "levelupDescriptionRaw" STRING, "flavorText" STRING, "artistName" STRING, "name" STRING, "pokemonCode" STRING, "keywords/0" STRING, "keywordRefs/0" STRING, "spellSpeed" STRING, "spellSpeedRef" STRING, "rarity" STRING, "rarityRef" STRING, "subtype" STRING, "supertype" STRING, "type" STRING, "collectible" STRING, "set" STRING, "associatedpokemonRefs/0" STRING, "associatedpokemonRefs/1" STRING, "subtypes/0" STRING, "keywords/1" STRING, "keywordRefs/1" STRING, "associatedpokemonRefs/2" STRING, "associatedpokemonRefs/3" STRING, "associatedpokemonRefs/4" STRING, "keywords/2" STRING, "keywordRefs/2" STRING, "regions/1" STRING, "regionRefs/1" STRING, "associatedpokemonRefs/5" STRING, "associatedpokemonRefs/6" STRING, "associatedpokemonRefs/7" STRING, "associatedpokemonRefs/8" STRING, "associatedpokemonRefs/9" STRING, "associatedpokemonRefs/10" STRING )")
      // output.show()

      output.createOrReplaceTempView("temp_data")
      hiveCtx.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
      hiveCtx.sql("SET hive.enforce.bucketing=false")
      hiveCtx.sql("SET hive.enforce.sorting=false")
      // hiveCtx.sql("DROP TABLE pokemonData")
      hiveCtx.sql(
        "CREATE TABLE IF NOT EXISTS pokemonData(name STRING, variation STRING, type1 STRING, type2 STRING, total INT, hp INT, attack INT, defense INT, specialatk INT, specialdef INT, speed INT)"
      )
      hiveCtx.sql("INSERT OVERWRITE TABLE pokemonData SELECT * FROM temp_data")
      // hiveCtx.sql("DROP TABLE pokemonDataPartitioned")
      hiveCtx.sql(
        "CREATE TABLE IF NOT EXISTS pokemonDataPartitioned (name STRING, variation STRING, hp INT, attack INT, defense INT, specialatk INT, specialdef INT, speed INT, type1 STRING, type2 STRING) PARTITIONED BY (total INT) CLUSTERED BY (name) INTO 10 BUCKETS stored as textfile"
      )
      hiveCtx.sql(
        "INSERT OVERWRITE TABLE pokemonDataPartitioned SELECT name STRING, variation STRING, hp INT, attack INT, defense INT, specialatk INT, specialdef INT, speed INT, type1 STRING, type2 STRING, total INT FROM pokemonData"
      )
      // To query the pokemonData table. When we make a query, the result set ius stored using a dataframe. In order to print to the console,
      // we can use the .show() method.
      val summary = hiveCtx.sql("SELECT * FROM pokemonDataPartitioned LIMIT 10")
      summary.show()
      println("summary show")

    }

    def highestBasePokemon(hiveCtx:HiveContext): Unit = {
      println("highestBasePokemon")
      val result = hiveCtx.sql("SELECT name AS HighestBaseStatPokemon, total FROM pokemonDataPartitioned WHERE variation IS NULL ORDER BY total DESC")
      result.show()
      result.write.csv("results/highestBasePokemon")
    
    }
    def lowestBasePokemon(hiveCtx:HiveContext): Unit = {
      println("lowestBasePokemon")
      val result = hiveCtx.sql("SELECT name AS LowestBaseStatPokemon, total FROM pokemonDataPartitioned WHERE variation IS NULL ORDER BY total ASC")
      result.show()
      // result.write.csv("results/lowestBasePokemon")
    
    }
    def pokemonVariationCount(hiveCtx:HiveContext): Unit = {
      println("pokemonVariationCount")
      val result = hiveCtx.sql("SELECT COUNT(variation), variation FROM pokemonDataPartitioned WHERE variation IS NOT NULL GROUP BY variation ORDER BY COUNT(variation) DESC")
      result.show()
      // result.write.csv("results/pokemonVariationCount")
    
    }
    def mostVariationsPokemon(hiveCtx:HiveContext): Unit = {
      println("mostVariationsPokemon")
      val result = hiveCtx.sql("SELECT name AS MostVariationsPokemon, COUNT(variation) FROM pokemonDataPartitioned WHERE variation IS NOT NULL GROUP BY NAME ORDER BY COUNT(variation) DESC")
      result.show()
      // result.write.csv("results/mostVariationsPokemon")
    
    }
    def pokemonPrimTypeCount(hiveCtx:HiveContext): Unit = {
      println("pokemonPrimTypeCount")
      val result = hiveCtx.sql("SELECT type1 AS PrimaryType, Count(type1) AS TypeCount FROM pokemonDataPartitioned GROUP BY type1 ORDER BY COUNT(type1) DESC ")
      result.show()
      // result.write.csv("results/pokemonPrimTypeCount")
    
    }
    def pokemonSecoTypeCount(hiveCtx:HiveContext): Unit = {
      println("pokemonSecoTypeCount")
      val result = hiveCtx.sql("SELECT type2 AS SecondaryType, Count(type2) AS TypeCount FROM pokemonDataPartitioned WHERE type2 IS NOT NULL GROUP BY type2 ORDER BY COUNT(type2) DESC")
      result.show()
      // result.write.csv("results/pokemonSecoTypeCount")
    
    }
    //  highestBasePokemon()
    //       queryMenu(scanner,connection)
    //     case 2 =>
    //       lowestBasePokemon()
    //       queryMenu(scanner,connection)
    //     case 3 =>
    //       highestMegaPokemon()
    //       queryMenu(scanner,connection)
    //     case 4 =>
    //       lowestMegaPokemon()
    //       queryMenu(scanner,connection)
    //     case 5 =>
    //       pokemonPrimTypeCount()
    //       queryMenu(scanner,connection)
    //     case 6 =>
    //       pokemonSecoTypeCount()
    //       queryMenu(scanner,connection)
    //pseudo legendaries
    //select by userinput type1
    //select userinput type 2
    //select strongest mega pokemon

  }
}
