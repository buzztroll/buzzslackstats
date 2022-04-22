import datetime
import logging
import os
import urllib

import docopt
import slack_sdk
import sqlite3

logging.basicConfig(level=logging.DEBUG)


DOC_OPT = """buzzstats

Usage: buzzstats [options]

Options:
  -h --help                    Show this screen.
  --version                    Show version.
  --dry-run                    Gather the data but do not post results
  --fetch                      Update database by syncing with the slack channel
  --channel=NAME               The name of the channel to gather stats [default: general]
  --post-channel=NAME          The name of the channel to to post stats to [default: random]
  --user=NAME                  The name of the user that says stats [default: slackbot]
  --start-date=DATE            yyyy-mm-dd date format for the first day to look
  --end-date=DATE              yyyy-mm-dd date format for the last day to look
  --days-back=INT              The number of days back to evaluate
  --dbfile=FILE                The location of disk of the sqlite file for storing stats [default: taint.db]
"""


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


class TaintReporter(object):
    def __init__(self, docopt_args):
        slack_token = os.getenv("SLACK_API_KEY")
        self.slack_client = slack_sdk.web.WebClient(token=slack_token)

        username = docopt_args["--user"]
        channel_name = docopt_args["--channel"]

        self.user_id = self._get_user_id(username)
        self.channel_id = self._get_channel_id(channel_name)

        self.dbfile = docopt_args["--dbfile"]
        self.dry_run = docopt_args["--dry-run"]
        self.fetch = docopt_args["--fetch"]
        self.post_channel_id = docopt_args['--post-channel']
        self.start_time = None
        self.end_time = None

        start_dt = None
        end_dt = None
        if docopt_args["--days-back"]:
            days_back = int(docopt_args["--days-back"])
            end_dt = datetime.datetime.now()
            start_dt = end_dt - datetime.timedelta(days=days_back)
        else:
            try:
                if docopt_args['--start-date'] is not None:
                    start_dt = datetime.datetime.strptime(docopt_args['--start-date'], "%Y-%m-%d")
                if docopt_args['--end-date'] is not None:
                    end_dt = datetime.datetime.strptime(docopt_args['--end-date'], "%Y-%m-%d")
            except ValueError:
                logging.error("date format must be yyyy-mm-dd")
                raise

        if start_dt:
            start_str = start_dt.strftime("%Y-%m-%d")
            self.start_time = start_dt.timestamp()
        else:
            start_str = "Beginning of time"
        if end_dt:
            end_str = end_dt.strftime("%Y-%m-%d")
            self.end_time = end_dt.timestamp()
        else:
            end_str = "today"
        self.range_message = f"({start_str} - {end_str})"

        self.setup_db()

    def setup_db(self):
        self.connection = sqlite3.connect(self.dbfile)
        self.connection.isolation_level = None
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                "CREATE TABLE messages (message TEXT, user TEXT, timestamp REAL, UNIQUE(timestamp, message))"
            )
        except sqlite3.OperationalError:
            logging.info("messages table exists")

        self.connection.commit()

    def _get_channel_id(self, channel_name):
        c = self.slack_client.conversations_list()
        if not c.data["ok"]:
            raise Exception(f"failed {c.data.error}")
        for convos in c.data["channels"]:
            if convos["name"] == channel_name:
                return convos["id"]
        raise Exception(f"{channel_name} not found")

    def _get_user_id(self, username):
        u = self.slack_client.users_list()
        if not u.data["ok"]:
            raise Exception(f"failed {u.data['error']}")
        for m in u.data["members"]:
            if m["name"] == username:
                return m["id"]
        raise Exception(f"{username} not found")

    def _get_messages(self, timestamp=None):
        has_more = True
        cursor = None
        total_new_messages = 0
        if timestamp is None:
            timestamp = get_latest_timestamp(self.connection)

        while has_more:
            convo_lines = self.slack_client.conversations_history(
                channel=self.channel_id, cursor=cursor, oldest=timestamp
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
                        and line["user"] == self.user_id
                    ):
                        total_new_messages += 1
                        yield line

                except KeyError:
                    logging.info("Message type probably doesn't match %s", str(line))
        logging.info("Added %d messages", total_new_messages)

    def _get_stats(self):
        # "select count(distinct message) from messages"
        total_message_stmt = "select count(message) from messages"
        where_clause = ""
        if self.start_time is not None:
            where_clause = f" where timestamp >= {self.start_time}"
        if self.end_time is not None:
            if where_clause:
                where_clause = where_clause + " AND "
            else:
                where_clause = " WHERE "
            where_clause = where_clause + f" timestamp <= {self.end_time}"
        total_message_stmt = total_message_stmt + where_clause
        a = self.connection.execute(total_message_stmt)
        row = a.fetchone()
        total_msg = row[0]
        message_group_stmt = f"select count(message), message from messages {where_clause} group by message order by count(message) desc"
        a = self.connection.execute(message_group_stmt)
        return_str = f"```Total tainted message: {total_msg}\n{self.range_message}\n"
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

    def fetch_messages(self):
        if not self.fetch:
            logging.info("Skipping updating messages from slack")
            return
        logging.info("Updating messages from slack")
        messages = self._get_messages()
        for m in messages:
            add_message(self.connection, m)

    def post_stats(self):
        stat_str = self._get_stats()
        logging.info(stat_str)
        if not self.dry_run:
            response = self.slack_client.chat_postMessage(channel=self.post_channel_id, text=stat_str)
            logging.info(response.status_code)


def main():
    args = docopt.docopt(DOC_OPT, version="1.0")
    tr = TaintReporter(args)
    tr.fetch_messages()
    tr.post_stats()


if __name__ == "__main__":
    main()
