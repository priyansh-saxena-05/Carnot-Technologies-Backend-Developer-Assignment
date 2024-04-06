from flask import Flask, jsonify, request
from datetime import datetime
import csv

app = Flask(__name__)

class CustomRedis:
    def __init__(self):
        self.data = {}

    def set(self, key, value):
        self.data[key] = value

    def get(self, key):
        return self.data.get(key, None)

    def delete(self, key):
        if key in self.data:
            del self.data[key]
            return True
        return False

custom_redis = CustomRedis()

# Read the CSV file and store the latest data for each device ID
def load_data_to_redis(csv_file):
    with open(csv_file, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            device_id = row['device_fk_id']
            timestamp = row['time_stamp']
            if custom_redis.get(device_id):
                stored_timestamp = custom_redis.get(device_id)['time_stamp']
                if timestamp > stored_timestamp:
                    custom_redis.set(device_id, row)
            else:
                custom_redis.set(device_id, row)

load_data_to_redis('data.csv')

@app.route('/latest_info/<device_id>', methods=['GET'])
def get_latest_info(device_id):
    data = custom_redis.get(device_id)
    if data:
        return jsonify(data)
    else:
        return jsonify({"error": "Device ID not found"}), 404

@app.route('/start_end_location/<device_id>', methods=['GET'])
def get_start_end_location(device_id):
    data = custom_redis.get(device_id)
    if data:
        start_location = (float(data['latitude']), float(data['longitude']))
        end_location = (float(data['latitude']), float(data['longitude']))
        return jsonify({"start_location": start_location, "end_location": end_location})
    else:
        return jsonify({"error": "Device ID not found"}), 404

@app.route('/location_points/<device_id>', methods=['GET'])
def get_location_points(device_id):
    start_time_str = request.args.get('start_time')
    end_time_str = request.args.get('end_time')
    if not (start_time_str and end_time_str):
        return jsonify({"error": "Start time and end time are required parameters"}), 400

    start_time = datetime.strptime(start_time_str, '%Y-%m-%dT%H:%M:%SZ')
    end_time = datetime.strptime(end_time_str, '%Y-%m-%dT%H:%M:%SZ')

    data = custom_redis.get(device_id)
    if data:
        location_points = []
        timestamp = datetime.strptime(data['time_stamp'], '%Y-%m-%dT%H:%M:%SZ')
        if start_time <= timestamp <= end_time:
            latitude = float(data['latitude'])
            longitude = float(data['longitude'])
            location_points.append({"latitude": latitude, "longitude": longitude, "timestamp": data['time_stamp']})
        return jsonify(location_points)
    else:
        return jsonify({"error": "Device ID not found"}), 404

if __name__ == '__main__':
    app.run(debug=True)
