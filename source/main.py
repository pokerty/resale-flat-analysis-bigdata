datafile = "..//ResalePricesSingapore.csv"
import pandas as pd
import os
from column_store import ColumnStore
import csv
import pprint
# Import the dictionary of town names, months and years.
from query_info import town_names, months, years, query_category
import time
import statistics

def retrieve_details(input_str):
    ''' Retireve month, year and town details from user Matriculation id'''
    # Check if input string has correct length
    if len(input_str) != 9:
        raise ValueError("Input string length should be 9 characters.")
    try:
        # Extract values for x, y, and z
        x = int(input_str[5])
        y = int(input_str[6])
        z = int(input_str[7])
    except ValueError:
        raise ValueError("x, y, and z must be integers.")
    
    return x, y, z

def read_csv_to_dict(filename):
    ''' Read a CSV file and return a dictionary with column headers as keys and lists of data as values.'''
    with open(filename, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        data_dict = {}
        for row in reader:
            for column, value in row.items():
                data_dict.setdefault(column, []).append(value)
    return data_dict

def write_to_csv(data, filename):
    """Write data to a CSV file."""
    file_exists = os.path.isfile(filename)
    headers = ['Year','Month','Town','Category','Value']
    with open(filename, mode='a+', newline='') as file:
        writer = csv.writer(file)
        # Add header for the file if creating a new file for data
        if not file_exists:
            writer.writerow(headers)

        print(f"Writing data to CSV file: {filename}\n")
        writer.writerow(data)

def calculate_statistics(data, category):
    """Calculate minimum, average, and standard deviation of area and price."""
    area_values = [int(entry['floor_area_sqm']) for entry in data]
    price_values = [int(entry['resale_price']) for entry in data]

    if category in query_category:
        if category == "0":
            return min(area_values)
        elif category == "1":
            return sum(area_values) / len(area_values)
        elif category == "2":
            return statistics.stdev(area_values)
        elif category == "3":
            return min(price_values)
        elif category == "4":
            return sum(price_values) / len(price_values)
        elif category == "5":
            return statistics.stdev(price_values)
    else:
        print("Invalid category. Please choose a valid category.")
        return None


def main():

    # Read and Load CSV file as a dictionary
    print(f"Retrieving data from: {datafile}\n")
    rawData = read_csv_to_dict(os.path.join(os.path.dirname(__file__), datafile))

    # Consisting of rawData stored in ColumnStore
    data = ColumnStore(rawData)

    # Sorted and index create on town in ColumnStore.
    sorted_data = ColumnStore(rawData)
    sorted_data.sort_column('town')
    sorted_data.create_index_on_towns()
    # Ask the user for the three-digit input
    matriculation_no = input("Enter your matriculation number (e.g., U2120866H): ")
    town,month,year = retrieve_details(matriculation_no)
    # Get the name of the town from the dictionary
    town_name = town_names.get(town)
    # Get the name of the month from the dictionary
    month_string = months.get(month)
    # Get the name of the year from the dictionary
    year_string = years.get(year)

    query_month_string_start_date = year_string + "-" + month_string

    while (True):
        print("Select a query category:")
        print("0: Minimum Area")
        print("1: Average Area")
        print("2: Standard Deviation of Area")
        print("3: Minimum Price")
        print("4: Average Price")
        print("5: Standard Deviation of Price")
        print("6: Exit")
        choice = input("Enter your choice: ")
        if(int(choice) == 6):
            print("Exiting program")
            exit()

        if choice not in query_category:
            print("Invalid choice. Please select a valid query category.")
            continue

        # Assuming year_string and month are already defined
        query_month_string_end_date = f"{year_string}-{month+2:02d}"

        print(f"\nQuery: Scanning resale HDB flats from {town_name} from {query_month_string_start_date} to {query_month_string_end_date}\n")

        # Function that uses indexing to find data
        start_time = time.time()
        matching_rows, count = sorted_data.query_with_indexing(town_name, query_month_string_start_date, query_month_string_end_date)
        elapsed_time = time.time() - start_time

        # Function that uses sequential scan on unsorted, unindexed data
        start_time2 = time.time()
        matching_rows2, count2 = data.query_without_indexing("town", "month", town_name, query_month_string_start_date, query_month_string_end_date)
        elapsed_time2 = time.time() - start_time2

        # No Results returned
        if len(matching_rows) == 0 or len(matching_rows2) == 0:
            print("No Result")
            print("Time taken for the query: {:.6f} seconds".format(elapsed_time))
            continue

        print(f"Without Sorting and indexing -> Rows accessed: {count2}, Time taken: {elapsed_time2:.6f} seconds")
        print(f"With Sorting and indexing -> Rows accessed: {count}, Time taken: {elapsed_time:.6f} seconds")

        results = calculate_statistics(matching_rows, choice)
        print(f"\nQuery Category: {query_category.get(choice)} | Result: {results}")
        results = [year_string,month_string,town_name,query_category.get(choice),results]
        write_to_csv(results, f'ScanResult_{matriculation_no}.csv')

if __name__ == '__main__':
    main()
