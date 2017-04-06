from flask import Flask
from flask import request
from flask import render_template
from ..scraper.lib.db_connect import NoSQLConnection

app = Flask(__name__)

print('connecting to database')
con = NoSQLConnection()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/streams/viewercount")
def get_viewercount():
    start_time = request.args.get('start', 0, type=int)
    end_time = request.args.get('end', 0, type=int)
    print("{0} - {0}".format(start_time, end_time))
    return start_time
    # return a json of streamname and viewercount


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
