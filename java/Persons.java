/*
 * Praktikum Datenbanken/Datenmanagement
 * Lehrstuhl Datenbank- und Informationssysteme
 * BTU Cottbus - Senftenberg
 *
 * @author Marcel Zierenberg & Tobias Killer
 */

import java.util.Scanner;
import java.io.InputStreamReader;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class Persons
{
	private Connection connection; // Verbindung zum Datenbanksystem

	public Persons( Connection connection )
	{
		this.connection = connection;
	}

	private void showQueryResult( ResultSet result ) throws SQLException
	{
		// Tabellenkopf ausgeben
		ResultSetMetaData metaData = result.getMetaData();
		int numColumns = metaData.getColumnCount(); // Anzahl der Tabellenspalten

		for( int i = 1; i <= numColumns; i++ ) // Tabellenspalten ausgeben
		{
			String name = metaData.getColumnName( i );
			System.out.format( "%-20s ", name );
		}
		System.out.println( "\n================================================================================" );

		// Tabelleninhalt ausgeben
		while( result.next() ) // Tabelleninhalt zeilenweise ausgeben
		{
			for( int i = 1; i <= numColumns; ++i )
			{
				String value = result.getString( i );
				System.out.format( "%-20s ", value );
			}
			System.out.println();
		}
	}

	public void createPersonsTable()
	{
		String sql = "CREATE TABLE PERSON (PID number PRIMARY KEY, NACHNAME varchar(100) NOT NULL, VORNAME varchar(100) NOT NULL, LIEBLINGSCOCKTAIL number, FOREIGN KEY (LIEBLINGSCOCKTAIL) REFERENCES COCKTAIL (CID));";

		try
		{
			PreparedStatement statement = connection.prepareStatement( sql );
			ResultSet result = statement.executeQuery();
			showQueryResult( result );
		}
		catch( SQLException e ) // Fehler bei der Ausfuehrung
		{
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
	}

	public void showAllPersons()
	{
		String sql = "SELECT * FROM Person";

		try
		{
			PreparedStatement statement = connection.prepareStatement( sql );
			ResultSet result = statement.executeQuery();
			showQueryResult( result );
		}
		catch( SQLException e ) // Fehler bei der Ausfuehrung
		{
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
	}

	public void findPerson()
	{
		// Gibt alle Personen mit bestimmtem Vornamen aus.
		String sql = "SELECT * FROM Person WHERE vorname = ?";
		String name = "";

		// Eingaben sammeln
		Scanner scanner = new Scanner( new InputStreamReader( System.in ) );
		try
		{
			System.out.print( "Gesuchten Vornamen eingeben: " );
			name = scanner.nextLine();
		}
		catch( Exception e )
		{
			System.out.println( "Fehlerhafte Eingabe!" );
			return;
		}

		try
		{
			PreparedStatement statement = connection.prepareStatement( sql );
			statement.setString( 1, name );
			ResultSet result = statement.executeQuery();
			showQueryResult( result );
		}
		catch( SQLException e ) // Fehler bei der Ausfuehrung
		{
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
		scanner.close();
	}


	public void addPerson()
	{
		// Fuegt eine neue Person hinzu.
		String sql = "INSERT INTO PERSON VALUES (pid=?, nachname=?, vorname=?, lieblingscocktail=?); ";
		int pid = 0;
		String nachname = "";
		String vorname = "";
		int lieblingscocktail = 0;

		// Eingaben sammeln
		Scanner scanner = new Scanner( new InputStreamReader( System.in ) );

		// Die Attribute PID, Vorname, Nachname und Lieblingscocktail werden vom Nutzer eingegeben.
		try {
			System.out.print( "PID eingeben: " );
			pid = scanner.nextInt();
		} catch (Exception e) {
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
		try {
			System.out.print( "Nachnamen eingeben: " );
			nachname = scanner.nextLine();
		} catch (Exception e) {
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
		try {
			System.out.print( "Vornamen eingeben: " );
			vorname = scanner.nextLine();
		} catch (Exception e) {
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
		try {
			System.out.print( "CocktailID eingeben: " );
			lieblingscocktail = scanner.nextInt();
		} catch (Exception e) {
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
		scanner.close();

		try
		{
			PreparedStatement statement = connection.prepareStatement( sql );
			statement.setInt( 1, pid );
			statement.setString(2, nachname);
			statement.setString(3, vorname);
			statement.setInt(4, lieblingscocktail);
			ResultSet result = statement.executeQuery();
			showQueryResult( result );
		}
		catch( SQLException e ) // Fehler bei der Ausfuehrung
		{
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}

	}

	public void deleteAllPersons()
	{
		// Loescht alle Personen.
		String sql = "DELETE FROM Person";

		try
		{
			PreparedStatement statement = connection.prepareStatement( sql );
			ResultSet result = statement.executeQuery();
			showQueryResult( result );
		}
		catch( SQLException e ) // Fehler bei der Ausfuehrung
		{
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
	}

	public void deletePerson()
	{
		// Loescht eine Person mit bestimmtem Vornamen.
		String sql = "DELETE FROM Person WHERE vorname = ?";
		String name = "";

		// Eingaben sammeln
		Scanner scanner = new Scanner( new InputStreamReader( System.in ) );
		try
		{
			System.out.print( "Gesuchten Vornamen eingeben: " );
			name = scanner.nextLine();
		}
		catch( Exception e )
		{
			System.out.println( "Fehlerhafte Eingabe!" );
			return;
		}

		try
		{
			PreparedStatement statement = connection.prepareStatement( sql );
			statement.setString( 1, name );
			ResultSet result = statement.executeQuery();
			showQueryResult( result );
		}
		catch( SQLException e ) // Fehler bei der Ausfuehrung
		{
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
		scanner.close();
	}

	public void dropPersonsTable()
	{
		// Loescht die Tabelle 'Person'.
		String sql = "DROP TABLE Person";

		// Befehl ausfuehren
		try
		{
			Statement statement = connection.createStatement();
			statement.execute( sql );
			System.out.println( "Tabelle erfolgreich entfernt!" );
		}
		catch( SQLException e ) // Fehler bei der Ausfuehrung
		{
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
	}
}
