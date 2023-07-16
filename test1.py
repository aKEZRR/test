import pandas as pd

# Read the CSV file
df = pd.read_csv(r"path_to_file")

# Convert the timestamp column to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Filter logs with status_code between 200 and 299
df = df[df['status_code'].between(200, 299)]

# Group the data by endpoint_path and 10-minute intervals
grouped = df.groupby(['endpoint_path', pd.Grouper(key='timestamp', freq='10T')])

# Calculate the average requests per unique caller_ip
avg_requests = grouped['caller_ip'].nunique().groupby('endpoint_path').sum() / grouped.size().groupby('endpoint_path').count()

# Create a new DataFrame with the results
result_df = pd.DataFrame({'endpoint_path': avg_requests.index, 'avg_requests': avg_requests.values})

print(result_df)

# Function to detect if a caller_ip sent more requests to an endpoint than the average
def detect_high_request_ips(df, endpoint_path):
    avg_requests = result_df[result_df['endpoint_path'] == endpoint_path]['avg_requests'].values[0]
    grouped = df[df['endpoint_path'] == endpoint_path].groupby('caller_ip')
    for caller_ip, group in grouped:
        if len(group) > avg_requests:
            print(f"Caller IP {caller_ip} sent more requests to Endpoint {endpoint_path} than the average.")


# List of the endpoints
endpoints = df['endpoint_path'].unique()

for endpoint in endpoints:
    detect_high_request_ips(df, endpoint)