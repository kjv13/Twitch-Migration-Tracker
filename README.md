# Twitch Migration Tracker

This project tracks users migrations between twitch channels and stores these migrations in a database. The visualizations are handled in a seperate repository called Twitch-Migration-Website

This is done by creating a thread running for each monitored stream and having that thread constantly grabbing the list of current viewers. This can be done by using the NAMES command through IRC which gives a list of all users in a channel, or by using Twitches API. Both of these methods will give the same list of names, since Twitch considers a user as watching if they are in the chat.

Every time a new list of viewers is gotten the thread determines who has joined and who has left the stream. Then if there is a user who has left one stream and joined another within some time limit, it is considered a migration and stored in the database.

## Prerequisites

This project runs on python 3.6 and uses mongodb as a database

## Getting Started

1. Clone the repository
	```
    git clone https://github.com/mroseman95/Twitch-Migration-Tracker
    ```
1. Set up python virtualenv (optional)
	```
    cd twitch-migration-tracker
    virtualenv -p python3 venv/
	source venv/bin/activate
    ```
1. install the required pip libraries
	```
    pip install -r requirements.txt
    ```
1. set up the database config
	* install mongodb and make sure the service is running
	* modify config/db.cfg.template to match your setup and then remove the .template from the filename
1. set up the api config
	* the api.cfg.template requires a client_id token which can gotten by registering the application as a connection under your twitch profile
	* again remove the .template from the filename
1. set up the irc config
	* the irc.cfg.template asks for an oauth token also gotten from twitch
	* make sure you are logged into twitch and go to [this site](https://twitchapps.com/tmi/) to get the oauth token
	* when done the config should look like  
	`oauth: oauth:abcdefghijklmnopqrstuvwxyz1234567890`
1. create the database by running `python create_db.py` back in the root directory
1. running `python update_streams.py` will populate the database with which streams to monitor and running `python watching.py` will begin watching for any migrations amongst those monitored streams

## Database Structure

There are 3 collections in the database
1. monitored_streams
    ```
    {
        _id: unique id,
        list_category: main_list,
        streams: [
            {
                streamname: foo,
                last_updated: seconds since epoch
            },
            ...
        ]
    }
    ```
1. stream_migrations
    ```
    {
        _id: unique id,
        from_stream: foo,
        to_stream: bar,
        migrations: [
            {
                username: user who changed streams,
                leave_time: seconds since epoch,
                join_time: seconds since epoch
            },
            {
                username: ...,
                leave_time: ...,
                join_time: ...
            },
            ...
        ]
    },
    {
        _id: unique id,
        from_stream: bar,
        to_stream: bar,
        migrations: [
            ...
        ]
    },
    ...
    ```
1. stream_viewcount
    ```
    {
        _id: unique id,
        streamname: foo,
        user_count: number of users at this time
        time: seconds since epoch
    },
    {
        _id: unique id,
        streamname: bar,
        user_count: ...,
        time: ...
    },
    ...
    ```

## Testing

There are currently no automated tests for this project, though this is a future goal

## Authors

* **Steven Gates** - *Initial api code* - [Gatesyp](https://github.com/gatesyp)
* **Matthew Roseman** - *All current code and design* - [Mroseman95](https://github.com/mroseman95)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
