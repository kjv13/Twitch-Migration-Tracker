# Twitch-Migration-Tracker
Tracks user migration between channels

## Installation Procedures
1. `git clone https://github.com/mroseman95/Twitch-Migration-Tracker`
1. (optional) set up python virtualenv
	* `virtualenv -p python3 venv/`
	* `source venv/bin/activate`
1. install the required pip libraries
	* `pip install -r requirements.txt`
1. set up the database config
	* install mongodb and make sure the service is running
	* modify config/db.cfg.template to match your setup and then remove the .template from the filename
1. set up the api config
	* the api.cfg.template requires a client_id token which can gotten by registering the application as a connection under your twitch profile
	* again remove the .template from the filename
1. set up the irc config
	* the irc.cfg.template asks for an oauth token also gotten from twitch
	* make sure you are logged into twitch and go to [this site](https://twitchapps.com/tmi/) to get the oauth token
	* when done the config should look like `oauth: oauth:abcdefghijklmnopqrstuvwxyz1234567890`
1. create the database by running `python create_db.py` back in the root directory
1. running `python update_streams.py` will populate the database with which streams to monitor and running `python watching.py` will begin watching for any migrations amongst those monitored streams
