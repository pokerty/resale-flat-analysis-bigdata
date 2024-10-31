import datetime, csv
from query_info import town_names, months, years, required_columns
class ColumnStore:
    def __init__(self, data=None):
        """Initialize the column store with data, filtering rows based on town names."""
        self.store = {}
        if data is not None and town_names is not None:
            # Filter rows based on town names present in the dictionary
            filtered_data = {column_name: [] for column_name in required_columns}
            for i in range(len(data['town'])):
                town = data['town'][i]
                if town in town_names.values():
                    for column_name in data:
                        if column_name in required_columns:
                            filtered_data[column_name].append(data[column_name][i])
            self.store = filtered_data
         
    def get_column(self, column_name):
        """Retrieve a column's values."""
        return self.store.get(column_name, [])
    
    def query_with_indexing(self, town_name, start_month, end_month):
        """Perform a query based on town_name and month range, and return the matching rows."""
        # Check if the town name exists in the index
        if town_name in self.index_on_towns:
            start_index = self.index_on_towns[town_name]
            
            # Get the index of the current town in the list of keys
            current_town_index = list(self.index_on_towns.keys()).index(town_name)

            # If there are more keys available, set the end index to one less than the start index of the next town
            if current_town_index + 1 < len(self.index_on_towns):
                next_town_name = list(self.index_on_towns.keys())[current_town_index + 1]
                end_index = self.index_on_towns[next_town_name] - 1
            else:
                # If there are no more keys, set the end index to the length of the town column
                end_index = len(self.store['town'])
        else:
            # Town name not found in index, return empty result
            return [], 0

        # Convert start_month and end_month to datetime objects
        start_month_dt = datetime.datetime.strptime(start_month, '%Y-%m')
        end_month_dt = datetime.datetime.strptime(end_month, '%Y-%m')

        # Initialize counter for rows accessed
        count = 0

        # Perform the query within the determined index range and return matching rows
        matching_rows = []

        for i in range(start_index, end_index):
            current_month = datetime.datetime.strptime(self.store['month'][i], '%Y-%m')
            if start_month_dt <= current_month <= end_month_dt:
                row = {col_name: self.store[col_name][i] for col_name in self.store}
                matching_rows.append(row)
            count += 1
        # If no matching rows found, return an empty list
        if not matching_rows:
            return [], count

        return matching_rows, count

    def query_without_indexing(self, town_column_name, month_column_name, town_name, start_month, end_month):
        """Perform a query based on town_name and month range, and return the matching rows."""
        town_column = self.get_column(town_column_name)
        month_column = self.get_column(month_column_name)
        # Convert start_month and end_month to datetime objects
        start_month_dt = datetime.datetime.strptime(start_month, '%Y-%m')
        end_month_dt = datetime.datetime.strptime(end_month, '%Y-%m')

        # Initialize counter for rows accessed
        row_access_count = 0
        # Perform the query and return matching rows
        matching_rows = []
        for i in range(len(town_column)):
            current_month = datetime.datetime.strptime(month_column[i], '%Y-%m')
            if town_name == town_column[i] and start_month_dt <= current_month <= end_month_dt:
                row = {col_name: self.store[col_name][i] for col_name in self.store}
                matching_rows.append(row)
            row_access_count += 1  # Increment row access count

        # If no matching rows found, return an empty list
        if not matching_rows:
            return [], row_access_count

        return matching_rows, row_access_count

    def sort_column(self, column_name):
        """Sort the store by the specified column and rearrange all columns to maintain row integrity."""
        target_column = self.store[column_name]
        sorted_indices = sorted(range(len(target_column)), key=lambda i: target_column[i])
        for col_name in self.store:
            self.store[col_name] = [self.store[col_name][i] for i in sorted_indices]

    def create_index_on_towns(self):
        """Create an index on the 'towns' column to store the position of the first occurrence of each town."""
        towns_column = self.get_column('town')
        self.index_on_towns = {}
        current_town = None
        for i, town in enumerate(towns_column):
            if town != current_town:
                self.index_on_towns[town] = i
                current_town = town

    # For printing a single column result in Terminal (in chunks due to limitation of terminal prints)
    def print_column_in_chunks(self, column_name, chunk_size=100):
        '''Used for debugging purposes to print a column in chunks'''
        column_data = self.get_column(column_name)
        for i in range(0, len(column_data), chunk_size):
            print(column_data[i:i+chunk_size])
            input("Press Enter to continue...")  # Pause between chunks
    
