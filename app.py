#__author__ = ritvikareddy2
#__date__ = 2019-02-06

import ast
import json
import logging
from flask import Flask, render_template, request
from pymongo import MongoClient


app = Flask(__name__)
client = MongoClient('localhost', 27017)
db = client['sample_gps']
data = db['gps_data']


# https://stackoverflow.com/questions/379906/how-do-i-parse-a-string-to-a-float-or-int-in-python
def num(s):
    try:
        return int(s)
    except ValueError:
        return float(s)


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/add', methods=['POST'])
def add_data():
    """
    receives and stores an array of GPS sensor data
    """
    # data.drop()
    if 'data' in request.args:
        gps_data_str = request.args['data']
        gps_data = ast.literal_eval(gps_data_str)
        res = data.insert_many(gps_data)

        logging.info(data.count())

        return "success", 200

    return "No data provided to add", 400


@app.route('/points', methods=['GET'])
def get_gps_points():
    """
    returns GPS points based on a time range.
    assuming time range also comes in unix epoch format
    """

    if 'start' in request.args and 'end' in request.args:
        start = num(request.args.get('start'))
        end = num(request.args.get('end'))
        single_point = data.find_one({'time_unix_epoch': 1498073665.3378906})
        print(single_point)
        gps_points = list(data.find(
            {'time_unix_epoch': {"$gte": start, "$lte": end}},
            {"_id": 0, 'latitude': 1, 'longitude': 1}))
        if gps_points:

            return json.dumps(gps_points), 200
        else:
            return "No such points exist", 200

    return "Missing required params", 404


@app.route('/aggregates', methods=['GET'])
def get_aggregates():
    """
    returns aggregates based on a time range
    assuming time range also comes in unix epoch format
    """

    if 'start' in request.args and 'end' in request.args:
        start = num(request.args.get('start'))
        end = num(request.args.get('end'))
        single_point = data.find_one({'time_unix_epoch': 1498073665.3378906})
        print(single_point)

        gps_points = list(data.aggregate(
            [
                {'$match': {
                    'time_unix_epoch': {"$gte": start, "$lte": end}
                }},
                {'$group': {
                    '_id': None,
                    "max_speed": {"$max": "$speed"},
                    "min_speed": {"$min": "$speed"},
                    'average_speed': {'$avg': "$speed"}
                }}
            ]
        ))

        if gps_points:
            gps_points = gps_points[0]
            gps_points.pop('_id')
            return json.dumps(gps_points), 200
        else:
            return "No values exist", 200

    return "Missing required params", 404


@app.route('/distance-travelled', methods=['GET'])
def get_distance_travelled():
    """
    the estimated distance travelled
    it is calculated as : average_speed * total_time
    """
    res = list(data.aggregate([
        {"$group": {
            "_id": None,
            "start_time": {"$min": "$time_unix_epoch"},
            "end_time": {"$max": "$time_unix_epoch"},
            'average_speed': {'$avg': "$speed"}
        }}
    ]))[0]
    avg_speed = res['average_speed']
    total_time = res['end_time'] - res['start_time']
    dist = avg_speed * total_time
    return '{0:.2f}'.format(dist), 200


@app.route('/distance', methods=['GET'])
def get_distance():
    """
    distance from a given coordinate at any specified point in time.
    """
    return '', 200


# helper func to use during development
def load_data():
    data.drop()
    with open('sample_gps.json') as f:
        gd = json.load(f)
    result = data.insert_many(gd)
    print(data.count())


if __name__ == '__main__':
    # load_data()
    app.run(debug=True)
