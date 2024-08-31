# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Converts datasets downloaded from srcport.com to other formats.
# This script downloads a dataset from srcport.com and converts it to a CSV file.
# The script requires an API key and the ID of the dataset you want to download.
# The script will download the dataset shards and save them to a folder.
# The script will then convert the dataset shards to a single CSV file.
# The CSV file will be saved in the same folder as the dataset shards.
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import argparse, requests
import os, sys
import csv, json

if sys.stdout.encoding.lower() != 'utf-8':
    os.environ["PYTHONIOENCODING"] = "utf-8"
    os.execv(sys.executable, ['python'] + sys.argv)

def main():
    parser = argparse.ArgumentParser(description="Process API key and data ID.")
    
    parser.add_argument('--api-key', required=True, type=str, help='Enter your srcport.com account API key. You can find this by logging into your account on the profile page.')
    parser.add_argument('--data-id', required=True, type=int, help='Enter the ID of the dataset you want to download.')
    parser.add_argument('--data-format', required=False, type=str, default='csv', help='Enter the format you want the data in.')
    parser.add_argument('--join-shards', required=False, type=bool, default=False, help='If true all dataset shards will be joined into a single file.')
    parser.add_argument('--hostname', required=False, type=str, default='https://srcport.com', help='Enter the hostname of the API server.')
    parser.add_argument('--output-dir', required=False, type=str, default='.', help='Enter the directory where you want to save the dataset.')
        
    # Parse the arguments
    args = parser.parse_args()
    response = request_download_links(args.hostname, args.data_id, args.api_key)
    abs_shard_list = []
    
    # Create a folder in the output directory to store the dataset
    output_dir = os.path.join(args.output_dir, f"dataset_{args.data_id}")
    os.makedirs(output_dir, exist_ok=True)
    
    # If output_dir is . make it absolute
    if args.output_dir == ".":
        output_dir = os.path.abspath(output_dir)
    
    count = 0
    # Loop "download_links" and download each file
    for signed_url in response["download_links"]:
        name = f"shard_{count}"
        print(f"[+] Downloading {name} to {output_dir}")
        abs_shard = download_shard(signed_url, output_dir, name)
        abs_shard_list.append(abs_shard)
        count += 1
        
    if args.data_format == 'csv':
        json_to_csv(abs_shard_list, os.path.join(output_dir, f"dataset_{args.data_id}.csv"))
        print(f"[+] CSV file saved to {output_dir}")

# Download the dataset shard from Google Cloud using the signed URL
# Save the dataset shard to the output directory
# Return the absolute path of the downloaded dataset shard
def download_shard(url, output_dir, name) -> str:
    response = requests.get(url, stream=True)
    file_path = os.path.join(output_dir, f"{name}.json")
    
    with open(file_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=128):
            file.write(chunk)
    
    return file_path

# Make the API call and returns JSON response
def request_download_links(hostname, data_id, api_key):
    url = f"{hostname}/datasets/api/retrieval/{data_id}"
    headers = {
        'X-API-KEY': api_key
    }
    
    response = requests.get(url, headers=headers, verify=False)
    
    if response.status_code == 200:
        print(f"[+] Request successful: {response.status_code}")
        return response.json()
    else:
        try:
            error_message = response.json()
        except ValueError:
            error_message = response.text
            
        print(f"[-] Request failed: {response.status_code} - {error_message}")
        return None

# Generate the endpoint URL
def get_endpoint(hostname, data_id):
    return f"{hostname}/datasets/api/retrieval/{data_id}"

# Convert a nested JSON object to a flat dictionary
def flatten_json(y, parent_key='', sep='.'):
    """
    Function to flatten a nested json file
    """
    items = []
    for k, v in y.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_json(item, f"{new_key}[{i}]", sep=sep).items())
                else:
                    items.append((f"{new_key}[{i}]", item))
        else:
            items.append((new_key, v))
    return dict(items)

# Converts a list of JSON files to a single CSV file
def json_to_csv(json_files, csv_file):
    headers = set()
    rows = []

    # Read and flatten all JSON files
    for json_file in json_files:
        with open(json_file, 'r', encoding='utf-8') as file:
            data_list = json.load(file)
            
            # Ensure the loaded data is a list
            if not isinstance(data_list, list):
                raise ValueError(f"Expected a list of objects in {json_file}, but got {type(data_list).__name__}")
            
            for data in data_list:
                # Flatten each JSON object in the array
                flat_data = flatten_json(data)
                
                # Track all headers
                headers.update(flat_data.keys())
                rows.append(flat_data)

    # Write to CSV with dynamic headers
    headers = sorted(headers)
    with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        
        for row in rows:
            # Fill in missing fields with None or an empty string
            full_row = {header: row.get(header, None) for header in headers}
            writer.writerow(full_row)

if __name__ == "__main__":
    main()
