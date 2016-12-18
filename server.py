from flask import Flask
from flask import render_template
from db_connect import NoSQLConnection

app = Flask(__name__)

print('connecting to database')
con = NoSQLConnection()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/streams/viewercount")
def get_viewercount():
    pass


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
