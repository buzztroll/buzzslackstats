import logging
import os
import urllib

import docopt
import slack_sdk
import sqlite3

logging.basicConfig(encoding="utf-8", level=logging.DEBUG)


DOC_OPT = """buzzstats

Usage: buzzstats [options] <command>

Options:
  -h --help                    Show this screen.
  --version                    Show version.
  --dry-run                    Gather the data but do not post results
  --channel=NAME               The name of the channel to gather stats [default: general]
  --user=NAME                  The name of the user that says stats [default: slackbot]
  --dbfile=FILE                The location of disk of the sqlite file for storing stats [default: taint.db]
"""


def get_channel_id(client, channel):
    c = client.conversations_list()
    if not c.data["ok"]:
        raise Exception(f"failed {c.data.error}")
    for convos in c.data["channels"]:
        if convos["name"] == channel:
            return convos["id"]
    raise Exception(f"{channel} not found")


def get_user_id(client, username):
    u = client.users_list()
    if not u.data["ok"]:
        raise Exception(f"failed {u.data['error']}")
    for m in u.data["members"]:
        if m["name"] == username:
            return m["id"]
    raise Exception(f"{username} not found")


def get_messages(client, channel_id, user_id, timestamp=None):
    has_more = True
    cursor = None

    while has_more:
        convo_lines = client.conversations_history(
            channel=channel_id, cursor=cursor, oldest=timestamp
        )
        timestamp = None
        if not convo_lines.data["ok"]:
            raise Exception(f"Failed to get convo {convo_lines.data['error']}")
        has_more = convo_lines.data["has_more"]
        try:
            cursor = convo_lines.data["response_metadata"]["next_cursor"]
        except KeyError:
            cursor = None
        for line in convo_lines.data["messages"]:
            try:
                if (
                    line["type"] == "message"
                    and line["subtype"] == "slackbot_response"
                    and line["user"] == user_id
                ):
                    yield line
            except KeyError:
                logging.info("Message type probably doesn't match %s", str(line))


def setup_db(dbfile):
    connection = sqlite3.connect(dbfile)
    connection.isolation_level = None
    cursor = connection.cursor()
    try:
        cursor.execute(
            "CREATE TABLE messages (message TEXT, user TEXT, timestamp REAL, UNIQUE(timestamp, message))"
        )
    except sqlite3.OperationalError:
        logging.info("messages table exists")

    connection.commit()
    return connection


def add_message(con, msg):
    txt = urllib.parse.quote(msg["text"])
    user_id = msg["user"]
    timestamp = float(msg["ts"])
    stmt = f"INSERT INTO messages(message, user, timestamp) VALUES('{txt}', '{user_id}', {timestamp})"
    con.execute(stmt)
    con.commit()


def get_latest_timestamp(con):
    stmt = "SELECT MAX(timestamp) from messages"
    a = con.execute(stmt)
    row = a.fetchone()
    if row is None:
        return None
    return row[0]


def get_stats(con):
    # "select count(distinct message) from messages"
    total_message_stmt = "select count(message) from messages"
    a = con.execute(total_message_stmt)
    row = a.fetchone()
    total_msg = row[0]
    message_group_stmt = "select count(message), message from messages group by message order by count(message) desc"
    a = con.execute(message_group_stmt)
    return_str = f"```Total tainted message: {total_msg}\n"
    return_str = return_str + "============================\n"
    percent_total = 0
    for row in a.fetchall():
        cnt = row[0]
        msg = urllib.parse.unquote(row[1])
        percent = float(cnt) / float(total_msg)
        outstr = "%4.2f%% : %4d :  %-50s\n" % (percent * 100.0, cnt, msg)
        return_str = return_str + outstr
        percent_total += percent
    return return_str + "```"


def main():
    args = docopt.docopt(DOC_OPT, version="1.0")

    username = args["--user"]
    channel_name = args["--channel"]
    dbfile = args["--dbfile"]
    dry_run = args["--dry-run"]
    slack_token = os.getenv("SLACK_API_KEY")
    client = slack_sdk.web.WebClient(token=slack_token)
    logging.info("Getting the user ID for %s", username)
    user_id = get_user_id(client, username)
    logging.info("Getting the channel ID for %s", channel_name)
    general_id = get_channel_id(client, channel_name)
    con = setup_db(dbfile)
    latest_ts = get_latest_timestamp(con)
    messages = get_messages(client, general_id, user_id, timestamp=latest_ts)
    for m in messages:
        add_message(con, m)
    stat_str = get_stats(con)
    logging.info(stat_str)
    if not dry_run:
        response = client.chat_postMessage(channel=general_id, text=stat_str)
        logging.info(response.status_code)


if __name__ == "__main__":
    main()
