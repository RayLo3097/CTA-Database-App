"""
Raymond Lo
CS 341 
Project 1

This project allows the user to use various commands
to analyze the CTA L ridership data from the CTA2_L_daily_ridership.db file.
The user can also plot the data for certain commands.
"""

import sqlite3
import matplotlib.pyplot as plt


class station_location:
    def __init__(self, station_name, latitude, longitude):
        self.station_name = station_name
        self.latitude = latitude
        self.longitude = longitude

def check_station(dbConn, stationName):
    """
    This function checks if the station name exists in the database.

    Args:
        dbConn: connection to the CTA database
        stationName: the name of the station to check

    Returns:
        A tuple with the station ID and station name if the station exists
        "none" if the station does not exist
        "multiple" if multiple stations exist
    """
    dbCursor = dbConn.cursor()
    if(stationName.find('_') or stationName.find('%')):
        sql_query = "Select Station_ID, Station_Name From Stations Where Station_Name Like ?;"
    else:
        sql_query = "Select Station_ID, Station_Name From Stations Where Station_Name = ?;"

    dbCursor.execute(sql_query, [stationName])
    station_names = dbCursor.fetchall()

    if(len(station_names) == 0):
        print("**No station found...")
        print() # new line
        return ("none", "none")
    elif(len(station_names) > 1):
        print("**Multiple stations found...")
        print() # new line
        return ("multiple", "multiple")
    else:
        return (station_names[0][0], station_names[0][1])

##################################################################  
#
# print_stats
#
# Given a connection to the CTA database, executes various
# SQL queries to retrieve and output basic stats.
#
def print_stats(dbConn):
    dbCursor = dbConn.cursor()
    
    print("General Statistics:")

    dbCursor.execute("Select count(*) From Stations;")
    row = dbCursor.fetchone()
    print("  # of stations:", f"{row[0]:,}")

    dbCursor.execute("Select count(Stop_ID) From Stops;")
    row = dbCursor.fetchone()
    print("  # of stops:", f"{row[0]:,}")

    dbCursor.execute("Select count(*) From Ridership;")
    row = dbCursor.fetchone()
    print("  # of ride entries:", f"{row[0]:,}")

    dbCursor.execute("Select strftime('%Y-%m-%d', Ride_Date) AS Date From Ridership Order By Date ASC Limit 1;")
    date1 = dbCursor.fetchone()
    dbCursor.execute("Select strftime('%Y-%m-%d', Ride_Date) AS Date From Ridership Order By Date DESC Limit 1;")
    date2 = dbCursor.fetchone()
    print("  date range:", date1[0], "-", date2[0])

    dbCursor.execute("Select SUM(Num_Riders) From Ridership;")
    row = dbCursor.fetchone()
    print("  Total ridership:", f"{row[0]:,}")
    print() # new line

def findStation(dbConn, stationName):
    """
    This function finds all stations that match the given station name or match the wildcards given.

    Args:
        dbConn: connection to the CTA database
        stationName: the name of the station to find
    """
    dbCursor = dbConn.cursor()
    if(stationName.find('_') or stationName.find('%')):
        sql_query = f"Select Station_ID, Station_Name From Stations Where Station_Name Like '{stationName}' Order By Station_Name ASC;"
    else:
        sql_query = f"Select Station_ID, Station_Name From Stations Where Station_Name = '{stationName}' Order By Station_Name ASC;"

    dbCursor.execute(sql_query)
    rows = dbCursor.fetchall()
    if(len(rows) == 0):
        print("**No stations found...")
        print() # new line
    else:
        for row in rows:
            print(f"{row[0]} : {row[1]}") #prints station id and station name
        print() # new line

def analyze_station_weekStats(dbConn, stationName):
    """
    This function analyzes the ridership for the weekdays, weekends, and holidays for the given station.

    Args:
        dbConn: connection to the CTA database
        stationName: the name of the station to analyze
    """
    dbCursor = dbConn.cursor()

    #Check if the station exists
    sql_query = f"""Select Sum(Num_Riders) From Ridership 
                    Where Station_ID = (
                        Select Station_ID
                          From Stations
                         Where Station_Name = '{stationName}'
                    );"""
    dbCursor.execute(sql_query)
    total = dbCursor.fetchone()
    if(total[0] == None):
        print("**No data found...", end="\n\n")
        return
    
    print(f"Percentage of ridership for the {stationName} station:")
    dbCursor.execute(sql_query)

    #Find weekday ridership
    sql_query = f"""Select Sum(Num_Riders) From Ridership 
                    Where Station_ID = (
                        Select Station_ID
                          From Stations
                         Where Station_Name = '{stationName}'
                    ) AND Type_of_Day = 'W';"""
    
    dbCursor.execute(sql_query)
    weekday = dbCursor.fetchone()
    percentage = (weekday[0]/total[0])*100
    print(f"  Weekday ridership: {weekday[0]:,} ({percentage:.2f}%)")
    
    #Find Saturday ridership
    sql_query = f"""Select Sum(Num_Riders) From Ridership 
                    Where Station_ID = (
                        Select Station_ID
                          From Stations
                         Where Station_Name = '{stationName}'
                    ) AND Type_of_Day = 'A';"""
    dbCursor.execute(sql_query)
    saturday = dbCursor.fetchone()
    percentage = (saturday[0]/total[0])*100
    print(f"  Saturday ridership: {saturday[0]:,} ({percentage:.2f}%)")

    #Find Sunday and holiday ridership
    sql_query = f"""Select Sum(Num_Riders) From Ridership 
                    Where Station_ID = (
                        Select Station_ID
                          From Stations
                         Where Station_Name = '{stationName}'
                    ) AND Type_of_Day = 'U';"""
    dbCursor.execute(sql_query)
    sunday_and_holiday = dbCursor.fetchone()
    percentage = (sunday_and_holiday[0]/total[0])*100
    print(f"  Sunday/holiday ridership: {sunday_and_holiday[0]:,} ({percentage:.2f}%)")

    #Find total ridership
    sql_query = f"""Select Sum(Num_Riders) From Ridership 
                    Where Station_ID = (
                        Select Station_ID
                          From Stations
                         Where Station_Name = '{stationName}'
                    );"""
    dbCursor.execute(sql_query)
    total = dbCursor.fetchone()
    print(f"  Total ridership: {total[0]:,}")
    print() # new line

def weekday_ridership(dbConn):
    """
    This function finds the total ridership for weekdays for all stations

    Args:
        dbConn: connection to the CTA database
    """
    dbCursor = dbConn.cursor()

    #Find total ridership for weekdays
    sql_query = f"""SELECT SUM(Num_Riders)
        FROM Ridership
        WHERE Type_of_Day = 'W';"""
    dbCursor.execute(sql_query)
    total = dbCursor.fetchone()

    #Find ridership for each station
    sql_query = f"""SELECT Stations.Station_Name AS Name,
        SUM(Ridership.Num_Riders) AS Total
        FROM Stations
        JOIN Ridership ON Stations.Station_ID=Ridership.Station_ID
        WHERE Ridership.Type_of_Day = 'W'
        GROUP BY Name
        ORDER BY Total DESC;"""
    dbCursor = dbConn.cursor()
    dbCursor.execute(sql_query)
    rows = dbCursor.fetchall()
    print("Ridership on Weekdays for Each Station")
    for row in rows:
        percentage = (row[1]/total[0])*100
        print(f"{row[0]} : {row[1]:,} ({percentage:.2f}%)") #prints station name and total ridership and percentage
    print() # new line

def direction_conversion(direction):
    """
    This function converts the direction to the appropriate format.

    Args:
        direction: the direction to convert

    Returns:
        The direction in the appropriate format
    """

    if(direction == 'North'):
        return 'N'
    elif(direction == 'South'):
        return 'S'
    elif(direction == 'East'):
        return 'E'
    elif(direction == 'West'):
        return 'W'    
    else:
        return direction

def check_direction(direction):
    """
    This function checks if the direction is valid.

    Args:
        direction: the direction to check

    Returns:
        True if the direction is valid
        False if the direction is invalid
    """
    if(direction == 'N' or direction == 'S' or direction == 'E' or direction == 'W'):
        return True
    else:
        return False

def capitalize_color(line_color):
    """
    This function capitalizes the line color

    Args:
        line_color: the line color to capitalize

    Returns:
        The line color in the appropriate format
    """

    index = line_color.find('-')
    if(index != -1): #if there is a dash capitalize the letter after the dash
        line_color = line_color[:index+1].capitalize() + line_color[index+1].upper() + line_color[index+2:]
        return line_color
    else:
        return line_color.capitalize()

def line_accessibility(dbConn):
    """
    This function finds the stops for a given line color and direction and checks if they are handicap accessible.

    Args:
        dbConn: connection to the CTA database
    """

    dbCursor = dbConn.cursor()
    user_color = input("Enter a line color (e.g. Red or Yellow): ").lower()
    user_color = capitalize_color(user_color)
    
    #Check if the line color exists
    dbCursor.execute(f"Select Color From Lines Where Color='{user_color}';")
    if(dbCursor.fetchone() == None):
        print("**No such line...", end="\n\n")
        return
    
    user_direction = input("Enter a direction (N/S/W/E): ").lower().capitalize()
    user_direction = direction_conversion(user_direction) #convert direction to appropriate format

    #Find the stops for the given line color and direction
    sql_query = f"""Select Stops.Stop_Name
        From Lines
        Join StopDetails ON Lines.Line_ID = StopDetails.Line_ID
        Join Stops ON StopDetails.Stop_ID = Stops.Stop_ID
        Where Lines.Color = '{user_color}' And Direction = '{user_direction}'
        Order By Stops.Stop_Name ASC;"""
    dbCursor.execute(sql_query)
    rows = dbCursor.fetchall()

    if(len(rows) == 0): #check if the line runs in the direction chosen
        print("**That line does not run in the direction chosen...", end="\n\n")
        return
    
    for row in rows:
        dbCursor.execute("Select ADA From Stops Where Stop_Name = ?", (row[0],)) #get the handicap accessibility for the stop
        accessibility = dbCursor.fetchone()
        #Prints the stop name and if it is handicap accessible
        if(accessibility[0] == 1):
            print(f"{row[0]} : direction = {user_direction} (handicap accessible)")
        else:
            print(f"{row[0]} : direction = {user_direction} (not handicap accessible)")
    print() # new line
    
def number_stops(dbConn):
    """
    This function finds the number of stops for each color by direction.

    Args:
        dbConn: connection to the CTA database
    """
    dbCursor = dbConn.cursor()

    print("Number of Stops For Each Color By Direction")

    sql_query = """Select COUNT(Stop_ID) From Stops;""" #find the total number of stops
    dbCursor.execute(sql_query)
    total = dbCursor.fetchone()
    
    #Find the number of stops for each color by direction
    sql_query = """Select Lines.Color As Color, 
            Stops.Direction,
            COUNT(Stops.Stop_ID) 
        From Lines
        Join StopDetails ON StopDetails.Line_ID = Lines.Line_ID
        Join Stops ON StopDetails.Stop_ID = Stops.Stop_ID
        Group By Color, Stops.Direction
        Order By Color ASC;"""
    dbCursor.execute(sql_query)
    rows = dbCursor.fetchall()
    for row in rows:
        percentage = (row[2]/total[0])*100
        print(f"{row[0]} going {row[1]} : {row[2]} ({percentage:.2f}%)") #prints the color, direction, number of stops, and percentage
    print() # new line

def plot_yearly_ridership(stationName, x_coords, y_coords):
    """
    This function plots the yearly ridership for the given station.

    Args:
        stationName: the name of the station
        x_coords: the years
        y_coords: the number of riders
    """
    plt.xlabel("Year")
    plt.ylabel("Number of Riders")
    plt.title(f"Yearly Ridership at {stationName} Station")
    plt.plot(x_coords, y_coords)
    plt.ioff()
    plt.show()

def yearly_ridership(dbConn, stationName):
    """
    This function finds the yearly ridership for the given station.

    Args:
        dbConn: connection to the CTA database
        stationName: the name of the station
    """
    dbCursor = dbConn.cursor()

    #check if the station exists
    valid_station = check_station(dbConn, stationName)
    if(valid_station[0] == "none"):
        return
    elif(valid_station[0] == "multiple"):
        return

    #check if the given station name contains wildcards
    if(stationName.find('_') or stationName.find('%')):
        sql_query = f"""Select strftime('%Y', Ride_Date) as Year, SUM(Num_Riders) as Total
                        From Ridership
                        Where Station_ID=(Select Station_ID From Stations Where Station_Name Like '{stationName}')
                        Group By Year
                        Order By Year ASC;"""
    else:
        sql_query = f"""Select strftime('%Y', Ride_Date) as Year, SUM(Num_Riders) as Total
                        From Ridership
                        Where Station_ID=(Select Station_ID From Stations Where Station_Name = '{stationName}')
                        Group By Year
                        Order By Year ASC;"""

    dbCursor.execute(sql_query)
    rows = dbCursor.fetchall()

    x_coords = []
    y_coords = []

    print(f"Yearly Ridership at {valid_station[1]}")
    for row in rows:
        x_coords.append(row[0]) #add the year to the x_coords list
        y_coords.append(row[1]) #add the number of riders to the y_coords list
        print(f"{row[0]} : {row[1]:,}")

    print() # new line
    user_input = input("Plot? (y/n) ")
    if(user_input == 'y'):
        plot_yearly_ridership(valid_station, x_coords, y_coords) #plot the yearly ridership

    print() # new line

def plot_monthly_ridership(stationName, year, x_coords, y_coords):
    """
    This function plots the monthly ridership for the given station.

    Args:
        stationName: the name of the station
        year: the year
        x_coords: the months
        y_coords: the number of riders
    """
    plt.xlabel("Month")
    plt.ylabel("Number of Riders")
    plt.title(f"Monthly Ridership at {stationName} ({year})")
    plt.plot(x_coords, y_coords)
    plt.ioff()
    plt.show()

def monthly_ridership(dbConn, stationName):
    """
    This function finds the monthly ridership for the given station.

    Args:
        dbConn: connection to the CTA database
        stationName: the name of the station
    """
    dbCursor = dbConn.cursor()

    valid_station = check_station(dbConn, stationName) #check if the station exists
    if(valid_station[0] == "none"):
        return
    elif(valid_station[0] == "multiple"):
        return
    valid_station_name = valid_station[1] #get the station name
    
    user_year = user_year = input("Enter a year: ")

    sql_query = f"""Select strftime('%m/%Y', Ride_Date) as Date, SUM(Num_Riders) as Total
                    From Ridership
                    Where Station_ID=(Select Station_ID From Stations Where Station_Name = ?) and strftime('%Y', Ride_Date) = '{user_year}'
                    Group By Date
                    Order By Date ASC;"""

    dbCursor.execute(sql_query, [valid_station_name])
    rows = dbCursor.fetchall()

    x_coords = []
    y_coords = []

    print(f"Monthly Ridership at {valid_station[1]} for {user_year}")
    for row in rows:
        x_coords.append(row[0][:2]) #add the month to the x_coords list
        y_coords.append(row[1]) #add the number of riders to the y_coords list
        print(f"{row[0]} : {row[1]:,}")

    print() # new line
    user_input = input("Plot? (y/n) ")
    if(user_input == 'y'):
        plot_monthly_ridership(valid_station_name, user_year, x_coords, y_coords) #plot the monthly ridership
    print() # new line

def plot_comparison(station1_name, station2_name, year, x_coords1, y_coords1, x_coords2, y_coords2):
    """
    This function plots the comparison of the ridership for the two stations.

    Args:
        station1_name: the name of the first station
        station2_name: the name of the second station
        year: the year
        x_coords1: the days for the first station
        y_coords1: the number of riders for the first station
        x_coords2: the days for the second station
        y_coords2: the number of riders for the second station
    """
    plt.xlabel("Day")
    plt.ylabel("Number of Riders")
    plt.title(f"Ridership Each Day of {year}")
    plt.plot(x_coords1, y_coords1, label=station1_name)
    plt.plot(x_coords2, y_coords2, label=station2_name)
    plt.legend()
    plt.ioff()
    plt.show()

def compare_stats(dbConn, user_year):
    """
    This function compares the ridership for two stations for the given year.

    Args:
        dbConn: connection to the CTA database
        user_year: the year to compare the ridership
    """
    dbCursor = dbConn.cursor()

    print() # new line
    user_station1 = input("Enter station 1 (wildcards _ and %): ").replace(" ", "") #get the first station and remove any spaces
    valid_station1 = check_station(dbConn, user_station1) #check if the first station exists
    if(valid_station1[0] == "none"):
        return
    elif(valid_station1[0] == "multiple"):
        return
    station1_id = valid_station1[0] #get the first station id
    station1_name = valid_station1[1] #get the first station name

    print() # new line

    user_station2 = input("Enter station 2 (wildcards _ and %): ").replace(" ", "") #get the second station and remove any spaces
    valid_station2 = check_station(dbConn, user_station2) #check if the second station exists
    if(valid_station2[0] == "none"): 
        return
    elif(valid_station2[0] == "multiple"):
        return
    station2_id = valid_station2[0] #get the second station id
    station2_name = valid_station2[1] #get the second station name

    #Find the first 5 days of the year
    sql_query1 = """Select strftime('%Y-%m-%d', Ride_Date) as Date, Sum(Num_Riders)
                    From Ridership
                    Where Station_ID = (Select Station_ID From Stations Where Station_Name = ?) and strftime('%Y', Ride_Date) = ?
                    Group By Date
                    Order By Date ASC
                    Limit 5;"""
    
    #Find the last 5 days of the year
    sql_query2 = """Select strftime('%Y-%m-%d', Ride_Date) as Date, Sum(Num_Riders)
                    From Ridership
                    Where Station_ID = (Select Station_ID From Stations Where Station_Name = ?) and strftime('%Y', Ride_Date) = ?
                    Group By Date
                    Order By Date DESC
                    Limit 5;"""
    dbCursor.execute(sql_query1, [station1_name, user_year]) #get the first 5 days of the year
    rows1 = dbCursor.fetchall()
    print(f"Station 1: {station1_id} {station1_name}") 
    #Print the first 5 days of the year
    for row in rows1:
        print(f"{row[0]} {row[1]}") 

    dbCursor.execute(sql_query2, [station1_name, user_year]) #get the last 5 days of the year
    rows1 = dbCursor.fetchall()
    for row in reversed(rows1): #Print the last 5 days of the year
        print(f"{row[0]} {row[1]}")

    dbCursor.execute(sql_query1, [station2_name, user_year]) #get the first 5 days of the year
    rows2 = dbCursor.fetchall()
    print(f"Station 2: {station2_id} {station2_name}")
    for row in rows2: #Print the first 5 days of the year
        print(f"{row[0]} {row[1]}")

    dbCursor.execute(sql_query2, [station2_name, user_year]) #get the last 5 days of the year
    rows2 = dbCursor.fetchall() 
    for row in reversed(rows2): #Print the last 5 days of the year
        print(f"{row[0]} {row[1]}")
    
    print() # new line
    user_input = input("Plot? (y/n) ")
    if(user_input == 'y'):
        #Find the ridership for each day of the year for the two stations
        sql_query = """Select strftime('%Y-%m-%d', Ride_Date) as Date, Sum(Num_Riders)
                        From Ridership
                        Where Station_ID = (Select Station_ID From Stations Where Station_Name = ?) and strftime('%Y', Ride_Date) = ?
                        Group By Date
                        Order By Date ASC;"""
        rows1 = dbCursor.execute(sql_query, [station1_name, user_year]).fetchall()
        x_coords1 = []
        y_coords1 = []
        days = 0
        for row in rows1:
            x_coords1.append(days)
            y_coords1.append(row[1]) #add the number of riders to the y_coords1 list
            days += 1
        
        rows2 = dbCursor.execute(sql_query, [station2_name, user_year]).fetchall()
        x_coords2 = []
        y_coords2 = []
        days = 0
        for row in rows2:
            x_coords2.append(days)
            y_coords2.append(row[1]) #add the number of riders to the y_coords2 list
            days += 1
        
        plot_comparison(station1_name, station2_name, user_year, x_coords1, y_coords1, x_coords2, y_coords2) #plot the comparison
    print() # new line

def plot_stations_nearby(station_objects):
    """
    This function plots the stations nearby the given latitude and longitude on an image.

    Args:
        station_objects: the station object containing the station name, latitude, and longitude
    """
    image = plt.imread("chicago.png")
    xydims = [-87.9277, -87.5569, 41.7012, 42.0868]
    plt.imshow(image, extent=xydims)
    for object in station_objects:
        plt.plot(object.longitude, object.latitude, 'bo') #plot the stations
        plt.annotate(object.station_name, (object.longitude, object.latitude)) #annotate the picture with the station name
    plt.xlim([-87.9277, -87.5569])
    plt.ylim([41.7012, 42.0868])
    plt.show()
        
def stations_nearby(dbConn):
    """
    This function finds the stations nearby the given latitude and longitude within a mile.

    Args:
        dbConn: connection to the CTA database
    """
    dbCursor = dbConn.cursor()

    user_latitute = input("Enter a latitude: ")
    user_latitute = float(user_latitute) #convert the latitude to a decimal
    if(user_latitute < 40 or user_latitute > 43): #check if the latitude is within bounds of Chicago
        print("**Latitude entered is out of bounds...")
        print() #new line
        return
    
    user_longitude = input("Enter a longitude: ")
    user_longitude = float(user_longitude) #convert the longitude to a decimal
    if(user_longitude < -88 or user_longitude > -87): #check if the longitude is within bounds of Chicago
        print("**Longitude entered is out of bounds...")
        print()
        return
    
    lat_degree = 1/69 #1 mile in latitude
    lon_degree = 1/51 #1 mile in longitude

    #Bounds for latitude and longitude
    lat_min = round(user_latitute - lat_degree, 3)
    lat_max = round(user_latitute + lat_degree, 3)

    lon_min = round(user_longitude - lon_degree, 3)
    lon_max = round(user_longitude + lon_degree, 3)

    #Find the stations within a mile
    sql_query= """Select Stations.Station_Name as Name, Stops.Latitude as Latitude, Stops.Longitude as Longitude
                    From Stations
                    Join Stops on Stations.Station_ID=Stops.Station_ID
                    Where (Stops.Latitude > ? AND Stops.Latitude < ?) AND
                        (Stops.Longitude > ? AND Stops.Longitude < ?)
                    Group By Name, Latitude, Longitude
                    Order By Name ASC, Latitude DESC, Longitude DESC;"""
    dbCursor.execute(sql_query, [lat_min, lat_max, lon_min, lon_max])
    rows = dbCursor.fetchall()
    if(len(rows) == 0): #check if there are any stations within a mile
        print("**No stations found...")
        print() # new line
        return
    
    print() # new line
    print("List of Stations Within a Mile")

    station_objects = []
    for row in rows:
        station_objects.append(station_location(row[0], row[1], row[2]))
        print(f"{row[0]} : ({row[1]}, {row[2]})")
    print() # new line

    user_input = input("Plot? (y/n) ")
    if(user_input == 'y'):
        plot_stations_nearby(station_objects) #plot the stations nearby
    print() # new line

    

##################################################################  
#
# main
#Clark/Lake
print('** Welcome to CTA L analysis app **', end="\n\n")

dbConn = sqlite3.connect('CTA2_L_daily_ridership.db') #connect to the CTA database

print_stats(dbConn) #print the general statistics

while True:
    user_input = input("Please enter a command (1-9, x to exit): ").replace(" ", "")
    if(user_input == 'x'):
        break
    elif(user_input == '1'):
        print() # new line
        user_station = input("Enter partial station name (wildcards _ and %): ").replace(" ", "")
        findStation(dbConn, user_station)
    elif(user_input == '2'):
        print() # new line
        user_station = input("Enter the name of the station you would like to analyze: ")
        analyze_station_weekStats(dbConn, user_station)
    elif(user_input == '3'):
        weekday_ridership(dbConn)
    elif(user_input == '4'):
        print() # new line
        line_accessibility(dbConn)
    elif(user_input == '5'):
        number_stops(dbConn)
    elif(user_input == '6'):
        print() # new line
        user_input = input("Enter a station name (wildcards _ and %): ")
        yearly_ridership(dbConn, user_input)
    elif(user_input == '7'):
        print() # new line
        user_input = input("Enter a station name (wildcards _ and %): ")
        monthly_ridership(dbConn, user_input)
    elif(user_input == '8'):
        print() # new line
        user_year = input("Year to compare against? ")
        compare_stats(dbConn, user_year)
    elif(user_input == '9'):
        print() # new line
        stations_nearby(dbConn)
    else:
        print("**Error, unknown command, try again...", end="\n\n")

dbConn.close()
#
# done
#