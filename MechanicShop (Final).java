/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddCustomer(MechanicShop esql){//1
		 try {
                        String query1 = "INSERT INTO Customer\n";
                        String cCount = esql.executeQueryAndReturnResult("SELECT COUNT(*) FROM Customer").get(0).get(0);
                        System.out.print("\tEnter First Name: ");
                        String fname = in.readLine();
                        System.out.print("\tEnter Last Name: ");
                        String lname = in.readLine();
                        System.out.print("\tEnter Address: ");
                        String address = in.readLine();
                        System.out.print("\tEnter Phone Number: ");
                        String phone = in.readLine();
			
			String check = "SELECT * FROM Customer WHERE fname='"+fname+"' AND lname='"+lname+ "'AND phone='"+ phone+"' AND address='" + address+"'";
			//if "!" removed it throws error message no matter what
			//with "!" does not throw error message no matter what
			if(!esql.executeQueryAndReturnResult(check).isEmpty()){
				throw new Exception("Customer already exists.\n");
			}


                        String query = query1 + "VALUES("+Integer.parseInt(cCount)+",'"+ fname +"','"+ lname +"','"+ phone +"','"+address+"')";
                        esql.executeUpdate(query);
                
			query = "SELECT * FROM Customer WHERE id=" + Integer.parseInt(cCount);
                        esql.executeQueryAndPrintResult(query);

                }catch(Exception e){
                        System.err.println(e.getMessage());
                }

	}
	
	public static void AddMechanic(MechanicShop esql){//2

		try{
                        String query1 = "INSERT INTO Mechanic\n";
                        String cCount = esql.executeQueryAndReturnResult("SELECT COUNT(*) FROM Mechanic").get(0).get(0);
                        System.out.print("\tEnter First Name: ");
                        String fname = in.readLine();
                        System.out.print("\tEnter Last Name: ");
                        String lname = in.readLine();
                        System.out.print("\tEnter Experience(Years): ");
                        String exp = in.readLine();
 			
			String check = "SELECT * FROM Mechanic WHERE fname='"+fname+"' AND lname='"+lname+ "'AND experience = "+exp;
                        if(!esql.executeQueryAndReturnResult(check).isEmpty()){
                                throw new Exception("Mechanic already exists.\n");
                        }


                        String query = query1 + "VALUES("+Integer.parseInt(cCount)+",'"+ fname +"','"+ lname +"',"+ exp +")";
                        esql.executeUpdate(query);

			query = "SELECT * FROM Mechanic WHERE id=" + Integer.parseInt(cCount);
                        esql.executeQueryAndPrintResult(query);

                }catch(Exception e){
                        System.err.println(e.getMessage());
                }

	}
	
	public static void AddCar(MechanicShop esql){//3
		try{
                        String query1 = "INSERT INTO Car\n";
                        System.out.print("\tEnter vin: ");
                        String vin = in.readLine();
                        System.out.print("\tEnter make: ");
                        String make = in.readLine();
                        System.out.print("\tEnter model: ");
                        String model = in.readLine();
                        System.out.print("\tEnter year: ");
                        String year = in.readLine();

                        String check = "SELECT * FROM Car WHERE vin = '" + vin + "'";
                        if(!esql.executeQueryAndReturnResult(check).isEmpty()){
                                throw new Exception("Car VIN already exists.\n");
                        }

                        String query = query1 + "VALUES('"+ vin +"','"+ make +"','"+ model +"',"+ year +")";
                        esql.executeUpdate(query);

                        query = "SELECT * FROM Car WHERE vin= '" + vin + "'\n";
                        esql.executeQueryAndPrintResult(query);

                }catch(Exception e){
                        System.err.println(e.getMessage());
                }

	}
	
	public static void InsertServiceRequest(MechanicShop esql){//4
		try{
			System.out.print("\tEnter last name: ");
                        String lname = in.readLine();
                        String existCustomer = "SELECT * FROM Customer WHERE lname='"+lname+"'";
                        int cnt = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT COUNT(*) FROM Customer WHERE lname='"+lname+"'").get(0).get(0));

                        List<List<String>> customer = esql.executeQueryAndReturnResult(existCustomer);

                        if(cnt == 0){
                                System.out.print("\tCustomer does not exist. Would you like to add a new customer?(0 = yes/1 = no) ");
                                String newC = in.readLine();
                                if(Integer.parseInt(newC) == 0){
                                        AddCustomer(esql);
                                }else if(Integer.parseInt(newC) == 1){
                                        throw new Exception("Returning to main menu.");
                                }

			}

                                esql.executeQueryAndPrintResult(existCustomer);
                                System.out.print("\tEnter Customer ID, from above, of desired customer: ");
                                String customerID = in.readLine();

                                int cnt1 = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT COUNT(*) FROM Customer WHERE lname='"+lname+"' AND id="+customerID).get(0).get(0));
                                if(cnt1 == 0){
                                        throw new Exception("The customer ID provided was incorrect.");
                                }

                                String customerCars = "SELECT car_vin FROM Owns WHERE customer_id=" + customerID;
                                List<List<String>> ownedCars = esql.executeQueryAndReturnResult(customerCars);
                                int cnt2 = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT COUNT(*) FROM Owns WHERE customer_id="+customerID).get(0).get(0));

                                if(cnt2 == 0){
                                        System.out.print("\tNo cars found. Would you like to add a car?(0 = yes/1 = no)");
                                        int addCar = Integer.parseInt(in.readLine());
                                        if(addCar == 0){
                                                AddCar(esql);
                                                int cntOwns = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT COUNT(*) FROM Owns").get(0).get(0));

                                                System.out.print("\tEnter the VIN of the car needing service: ");
                                                String carVin = in.readLine();

                                                String updateOwns = "INSERT INTO Owns\nVALUES("+cntOwns+","+customerID+",'"+carVin+"')";
                                                esql.executeUpdate(updateOwns);
                                                String rid = esql.executeQueryAndReturnResult("SELECT COUNT(*) FROM Service_Request").get(0).get(0);
                                                System.out.print("\tEnter Date (MM/DD/YYYY): ");
                                                String date = in.readLine();
                                                System.out.print("\tEnter odometer value: ");
                                                String od = in.readLine();
                                                System.out.print("\tEnter complaint/issue with car: ");
                                                String complain = in.readLine();

                                                String query = "INSERT INTO Service_Request\nVALUES("+rid+","+customerID+",'"+carVin+"','"+date+"',"+od+",'"+complain+"')";
                                                esql.executeUpdate(query);

                                                query = "SELECT * FROM Service_Request WHERE rid="+rid;
                                                esql.executeQueryAndPrintResult(query);
                                        }

			   		else if(addCar == 1){
                                                throw new Exception("Returning to main menu.");
                                        }
                                }else if(cnt2 > 0){
                                        for(int i = 0;i < ownedCars.size();i++){
                                                for(int x = 0; x < ownedCars.get(i).size();x++){
                                                        String vins = "SELECT * FROM Car WHERE vin='"+ownedCars.get(i).get(x)+"'";
                                                        esql.executeQueryAndPrintResult(vins);
                                                }
                                        }

                                        System.out.print("\tIs the car you wish to service shown above?(0 = yes/1 = no)");
                                        int carShown = Integer.parseInt(in.readLine());
                                        if(carShown == 1){
                                                System.out.print("\tPlease fill out information on the car you wish to be serviced.");
                                                AddCar(esql);

                                        }
                                        System.out.print("\tEnter the VIN of the car needing service: ");
                                        String carVin = in.readLine();

                                        if(carShown == 1){
                                                int cntOwns = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT COUNT(*) FROM Owns").get(0).get(0));
                                                String updateOwns = "INSERT INTO Owns\nVALUES("+cntOwns+","+customerID+",'"+carVin+"')";
                                                esql.executeUpdate(updateOwns);
                                        }
					String check10 = "SELECT * FROM Owns WHERE customer_ID = " + customerID + " AND car_vin = '" + carVin + "'";

					if(esql.executeQueryAndReturnResult(check10).isEmpty()){
                               			throw new Exception("Customer doesn't own this car.\n");
                        		}					

                                        String rid = esql.executeQueryAndReturnResult("SELECT COUNT(*) FROM Service_Request").get(0).get(0);
                                        System.out.print("\tEnter Date: ");
                                        String date = in.readLine();
                                        System.out.print("\tEnter odometer value: ");
                                        String od = in.readLine();
                                        System.out.print("\tEnter complaint/issue with car: ");
                                        String complain = in.readLine();

                                        String query = "INSERT INTO Service_Request\nVALUES("+rid+","+customerID+",'"+carVin+"','"+date+"',"+od+",'"+complain+"')";
                                        esql.executeUpdate(query);

                                        query = "SELECT * FROM Service_Request WHERE rid="+rid;
                                        esql.executeQueryAndPrintResult(query);

                                }
                        


		}catch(Exception e){
                        System.err.println(e.getMessage());
                }
	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
		try{
			System.out.print("\tEnter service request number: ");
			String rid = in.readLine();
			
			String check = "SELECT * FROM Service_Request WHERE rid = " + rid ;
                        if(esql.executeQueryAndReturnResult(check).isEmpty()){
                                throw new Exception("Service Request with that record id does not exist.\n");
                        }
			String check3 = "SELECT * FROM Closed_Request WHERE rid = " + rid ;
                        if(!esql.executeQueryAndReturnResult(check3).isEmpty()){
                                throw new Exception("Closed Request with that record id already exists.\n");
                        }
			System.out.print("\tEnter employee id: ");
			String empid = in.readLine();
			
			String check1 = "SELECT * FROM Mechanic WHERE id = " + empid ;
                        if(esql.executeQueryAndReturnResult(check1).isEmpty()){
                                throw new Exception("Mechanic with that id does not exist.\n");
                        }
			System.out.print("\tEnter closing date (MM/DD/YYYY): ");
			String cdate = in.readLine();

			String check2 = "SELECT * FROM Service_Request WHERE rid = " + rid + " AND date < '" + cdate +"'" ;
                        if(esql.executeQueryAndReturnResult(check2).isEmpty()){
                                throw new Exception("Closing date is not after open date.\n");
                        }
			System.out.print("\tEnter comment: ");
			String comm = in.readLine();
			System.out.print("\tEnter bill amount: ");
			String bill = in.readLine();
			//int wid = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT COUNT(*) FROM Closed_Request).get(0).get(0));
			
			String query = "INSERT INTO Closed_Request VALUES (" +rid+ ", "+ rid + ", " + empid + ", '" + cdate + "', '" + comm + "', " + bill + ")";
			esql.executeUpdate(query);
                
			query = "SELECT * FROM Closed_Request WHERE rid=" + rid;
                        esql.executeQueryAndPrintResult(query);

		}catch(Exception e){
                        System.err.println(e.getMessage());
                }	
	}

	 public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
                try{
                        String query = "SELECT c1.fname, c1.lname, c.date, c.comment, c.bill FROM Closed_Request c, Service_Request s, Customer c1 WHERE c.rid = s.rid AND s.customer_id = c1.id AND c.bill < 100";
                        int rowCount = esql.executeQueryAndPrintResult(query);
                        System.out.println ("total row(s): " + rowCount + "\n");
                }catch(Exception e){
                System.err.println (e.getMessage());
                }
        }

        public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
                try{
                        String query = "SELECT DISTINCT fname, lname FROM Customer, Owns WHERE id = customer_id AND customer_id IN (SELECT COUNT(*) FROM Owns GROUP BY customer_id HAVING COUNT(*) > 20)";
                        int rowCount = esql.executeQueryAndPrintResult(query);
                        System.out.println ("total row(s): " + rowCount + "\n");
                }catch(Exception e){
                        System.err.println(e.getMessage());
                }
        }

        public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
                try{
                        String query = " SELECT make, model, year FROM Car, Service_Request WHERE vin = car_vin AND year < 1995 AND odometer < 50000";

                        int rowCount = esql.executeQueryAndPrintResult(query);
                        System.out.println ("total row(s): " + rowCount + "\n");
                }catch(Exception e){ //will print 'exception' from the database
                        System.err.println (e.getMessage());
                }
        }

	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		//
		try{
			String query = "SELECT COUNT(*),car_vin\nFROM Service_Request\nGROUP BY car_vin\nORDER BY COUNT(*) DESC";
                        System.out.print("\tEnter a postive non-zero number: ");
                        int k = Integer.parseInt(in.readLine());

                        List<List<String>> cntVin = esql.executeQueryAndReturnResult(query);
                        if(k > cntVin.size()){
                                throw new Exception("K value must be smaller than "+cntVin.size());
                        }else if(k <= 0){
                                throw new Exception("K value must be greater than 0.");
                        }
                        for(int i = 0; i < k;i++ ){
                                int key = Integer.parseInt(cntVin.get(i).get(0));
                                String vin = cntVin.get(i).get(1);

                                String getCar = "SELECT make,model FROM Car WHERE vin='"+vin+"'";
                                List<List<String>> getCarStuff = esql.executeQueryAndReturnResult(getCar);
                                for(int x = 0; x < getCarStuff.size();x++){
                                        System.out.print(getCarStuff.get(x).get(0)+"\t"+getCarStuff.get(x).get(1)+"\t"+key+"\n");
                                }
                        }

		}catch(Exception e){
                        System.err.println(e.getMessage());
                }	
	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//10
		//
		 try{
                        String query = "SELECT fname, lname, SUM(bill) FROM ((Service_Request INNER JOIN Closed_Request ON Service_Request.rid = Closed_Request.rid) INNER JOIN Customer ON Service_Request.customer_id = Customer.id) GROUP BY Customer.id ORDER BY SUM(bill) DESC";
                        int rowCount = esql.executeQueryAndPrintResult(query);
                        System.out.println("total row(s): " + rowCount + "\n");
                  }catch(Exception e){ //will print 'exception' from the database
                        System.err.println (e.getMessage());
                }

	}
	
}