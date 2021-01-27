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

	// Erstellt die Tabelle 'Person'.
	public void createPersonsTable()
	{
		 String sql = "CREATE TABLE PERSON " +
					 "(PID number PRIMARY KEY," +
				     " NACHNAME varchar(100) NOT NULL," +
					 " VORNAME varchar(100) NOT NULL," +
				     " LIEBLINGSCOCKTAIL number," +
					 " FOREIGN KEY (LIEBLINGSCOCKTAIL) REFERENCES COCKTAIL (CID))";
		
		// Befehl ausfuehren 
		try
		{
			Statement statement = connection.createStatement();
			statement.execute( sql );
			System.out.println("Tabelle PERSON erstellt");
		}
		catch( SQLException e ) // Fehler bei der Ausfuehrung
		{
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
	}

	// Listet alle Personen mit allen Attributen auf.
	public void showAllPersons()
	{
		String sql = "SELECT * FROM PERSON";
	
		try
		{
			PreparedStatement statement = connection.prepareStatement( sql );
			ResultSet result = statement.executeQuery();
			showQueryResult( result );	//Anzeigen des Ergebis
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

		// Befehl ausfuehren
		try
		{
			PreparedStatement statement = connection.prepareStatement( sql );
			statement.setString( 1, name );
			ResultSet result = statement.executeQuery();
			showQueryResult( result );	//Anzeigen des Ergebis
			statement.executeQuery();
		}
		catch( SQLException e ) // Fehler bei der Ausfuehrung
		{
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
	}

	// Fuegt eine neue Person hinzu.
	// Die Attribute PID, Vorname, Nachname und Lieblingscocktail werden vom Nutzer eingegeben.			
	public void addPerson()
	{
		String sql = "INSERT INTO PERSON VALUES (?, ?, ?, ?)";
		Integer pid;
		String nachname;
		String vorname;
		Integer lieblingscocktail;
		
		// Eingaben sammeln
		Scanner scannerPid = new Scanner( new InputStreamReader( System.in ) );
		try
		{
			System.out.print( "Gewuenschte PersonenID (PID) eingeben: " );
			pid = scannerPid.nextInt();
		}
		catch( Exception e )
		{
			System.out.println( "Fehlerhafte Eingabe!" );
			return;
		}
		
		Scanner scannerNachname = new Scanner( new InputStreamReader( System.in ) );
		try
		{
			System.out.print( "Gewuenschten Nachnamen eingeben: " );
			nachname = scannerNachname.nextLine();
		}
		catch( Exception e )
		{
			System.out.println( "Fehlerhafte Eingabe!" );
			return;
		}
		
		Scanner scannerVorname = new Scanner( new InputStreamReader( System.in ) );
		try
		{
			System.out.print( "Gewuenschten Vornamen eingeben: " );
			vorname = scannerVorname.nextLine();
		}
		catch( Exception e )
		{
			System.out.println( "Fehlerhafte Eingabe!" );
			return;
		}
		
		Scanner scannerLieblingscocktail = new Scanner( new InputStreamReader( System.in ) );
		try
		{
			System.out.print( "Nummer des Lieblingscocktail eingeben (0 wenn keiner vorhanden): " );
			lieblingscocktail = scannerLieblingscocktail.nextInt();
		}
		catch( Exception e )
		{
			System.out.println( "Fehlerhafte Eingabe!" );
			return;
		}
		
		// Befehl ausfuehren
		try
		{
			PreparedStatement statement = connection.prepareStatement( sql );
			statement.setInt( 1, pid );
			statement.setString( 2, nachname );
			statement.setString( 3, vorname );
			//Lieblingscocktail ist optional, deswegen wird Eingabe 0 zu null
			if(lieblingscocktail != 0) {
				statement.setInt( 4, lieblingscocktail );
			} else {
				statement.setNull( 4, java.sql.Types.INTEGER );
			}
			statement.executeUpdate();
			System.out.println( "Person " + vorname + " " + nachname + " wurde hinzugefuegt." );
		}
		catch( SQLException e ) // Fehler bei der Ausfuehrung
		{
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
		
	
	}

	// Loescht alle Personen.
	public void deleteAllPersons()
	{
		String sql = "DELETE FROM PERSON";
		
		// Befehl ausfuehren
		try
		{
			PreparedStatement statement = connection.prepareStatement( sql );
			statement.executeUpdate();
			System.out.println( "Alle Personen wurden geloescht." );
		}
		catch( SQLException e ) // Fehler bei der Ausfuehrung
		{
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
	}

	// Loescht eine Person mit bestimmtem Vornamen.
	public void deletePerson()
	{
		String sql = "DELETE FROM Person WHERE vorname = ?";
		String name = "";

		// Eingaben sammeln
		Scanner scanner = new Scanner( new InputStreamReader( System.in ) );
		try
		{
			System.out.print( "Vornamen der zu loeschenden Person eingeben: " );
			name = scanner.nextLine();
		}
		catch( Exception e )
		{
			System.out.println( "Fehlerhafte Eingabe!" );
			return;
		}

		// Befehl ausfuehren
		try
		{
			PreparedStatement statement = connection.prepareStatement( sql );
			statement.setString( 1, name );
			statement.executeUpdate();
			System.out.println( "Person mit Vornamen " + name + " wurde geloescht." );
		}
		catch( SQLException e ) // Fehler bei der Ausfuehrung
		{
			System.out.println( "ERROR: " + e.getMessage() );
			return;
		}
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
