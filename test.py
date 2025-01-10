import csv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def connect_and_save_to_csv():
    # Provide your Cassandra cluster nodes (replace with your actual nodes)
    contact_points = ['127.0.0.1']  # Example for a local cluster; replace with your cluster's IPs
    port = 9042  # Default Cassandra port

    # (Optional) Provide credentials if your cluster requires authentication
    username = 'cassandra'
    password = 'cassandra'

    # Initialize authentication provider if required
    auth_provider = PlainTextAuthProvider(username, password)

    # Create a cluster connection
    cluster = Cluster(contact_points=contact_points, port=port, auth_provider=auth_provider)

    # Connect to the cluster
    session = cluster.connect()

    # Set the default keyspace if desired
    session.set_keyspace('spark_streams')

    print("Connected to Cassandra successfully!")

    # Perform the query
    query = 'SELECT * FROM location;'  # Replace with your query
    rows = session.execute(query)

    # Save results to a CSV file
    output_file = 'location.csv'
    with open(output_file, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        # Write header row
        writer.writerow(rows.column_names)

        # Write data rows
        for row in rows:
            writer.writerow(row)

    print(f"Data saved to {output_file} successfully!")

    # Close the connection
    cluster.shutdown()

if __name__ == "__main__":
    try:
        connect_and_save_to_csv()
    except Exception as e:
        print(f"An error occurred: {e}")
