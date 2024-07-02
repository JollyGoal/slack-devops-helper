import os
from pathlib import Path
from dotenv import load_dotenv
from slack_bolt import App
from flask import Flask, request
from slack_bolt.adapter.flask import SlackRequestHandler
import time
from celery import Celery
from utils import slack_code_block_to_text, slack_validate_sql_request, run_return_sql, slack_thread_query_success

APPROVED_PG_READ_USERS = {"U04MRH86771",}
# APPROVED_PG_READ_USERS = {"U04MRH86771", "U03TETZS9ED", "U034MC5MR9V", "U029UC3MUTZ"}
APPROVED_PG_WRITE_USERS = {"U04MRH86771", }
APPROVED_PG_DUMP_USERS = {"U04MRH86771",}

APPROVED_USERS = {
    "PG_READ": APPROVED_PG_READ_USERS,
    "PG_WRITE": APPROVED_PG_WRITE_USERS,
    "PG_DUMP": APPROVED_PG_DUMP_USERS,
}

COMMANDS = {
    "PG_READ",
    "PG_WRITE",
    # "PG_DUMP",
}
CHANNELS = {"C0658G8P3T3", "C03CHNB6M41", "C04UKSL4B1P"}

"""
This is a slack bot that will be used handle messages that mention this bot from users in a channel and:
1. Add an emoji reaction `:eyes:` to the message
2. List all jobs that are written in the message:
    1. Pop the line from the message where the bot was mentioned
    2. Get action to do, action is first pair of lines consisting of one of `COMMANDS` and one Code block. Any other lines are ignored.
        Examples: 
            * "> PG_READ <database_name>:" and a code block with an SQL query
            * "> PG_DUMP:" and a code block with a database_name
3. Reply in the thread with a message that says "Waiting for approval from <@user>...". Approval users correspond to the command.
4 If the user adds a `:white_check_mark:` emoji reaction to the message, then:
    1. Reply to the user with a message that says "I hear ya <@user>! Standby..."
    2. Remove the `:eyes:` emoji reaction from the message and add a `:60fps_parrot:` emoji reaction to the message
    3.1 If the command is "PG_WRITE", then:
        1. Run the SQL query against the database and get the results or error.
        2. Post the results as a code block to the channel.
    3.2 If the command is "PG_READ", then:
        1. Run the SQL query against the database and get the results or error.
        2.1 If the query was not successful, then post the output as a code block to the channel.
        2.2 If the query was successful, copy the results to a CSV file and upload the CSV file the dm channel of the user who requested the query.
    3.3 If the command is "PG_DUMP", then:
        1. Get the dump of the database
        2. Split file if it is too big
        3. Upload the dump to the dm channel of the user who requested the query.
    4. Remove the `:60fps_parrot:` emoji reaction from the message and add a `:white_check_mark:` emoji reaction to the message if the query was successful or a `:x:` emoji reaction if the query failed.
    5. Reply to the thread with a message that says "Done!"
    6. Add an emoji reaction `:white_check_mark:` to the message
Remarks:
1. Edited messages should be ignored.
2. Work only on specified CHANNELS
3. If the message has `:white_check_mark:` or `:x:` or `:60fps_parrot:` emoji reaction, then ignore the message.
"""

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

BOT_ID = os.environ['BOT_ID']

# Handle cache folder, create if not exists, clear if exists
CACHE_FOLDER = Path(".") / "worker-cache"
if not CACHE_FOLDER.exists():
    CACHE_FOLDER.mkdir()
else:
    for file in CACHE_FOLDER.iterdir():
        file.unlink()


slack_app = App(
    token=os.environ.get("SLACK_BOT_TOKEN"),
    signing_secret=os.environ.get("SLACK_SIGNING_SECRET")
)

flask_app = Flask(__name__)
app =  Celery(flask_app.name, broker='redis://localhost:6379')
handler = SlackRequestHandler(slack_app)

@flask_app.route("/slack/devops-helper/events", methods=["POST"])
def slack_events():
    return handler.handle(request)


@app.task
def pg_read(event, ts, database_name, code_block):
    message_ts, channel_id, parent_message = slack_validate_sql_request(slack_app, event, ts)
    if message_ts is None or channel_id is None or parent_message is None:
        return
    query = slack_code_block_to_text(code_block)
    print(f"Running read query: {query}")
    results = run_return_sql(slack_app, channel_id, message_ts, database_name, query, read_only=True, prod=True)
    if results is None:
        return
    elif results == "No results found":
        text = f"```{results}```"
        slack_app.client.chat_postMessage(
            channel=channel_id,
            text=text,
            thread_ts=message_ts
        )
    else:
        # Upload the CSV file and send to user
        csv_content = "\n".join([",".join([str(item) for item in result]) for result in results])
        slack_app.client.files_upload(
            channels=parent_message.get('user'),
            content=csv_content,
            filename=f"{database_name}.csv",
            filetype="csv",
        )
    slack_thread_query_success(slack_app, channel_id, message_ts)


@app.task
def pg_write(event, ts, database_name, code_block):
    message_ts, channel_id, parent_message = slack_validate_sql_request(slack_app, event, ts)

    query = slack_code_block_to_text(code_block)
    print(f"Running read query: {query}")
    results = run_return_sql(slack_app, channel_id, message_ts, database_name, query, read_only=False, prod=True)
    if results is None:
        return
    text = f"```{results}```"
    slack_app.client.chat_postMessage(
        channel=channel_id,
        text=text,
        thread_ts=message_ts
    )
    slack_thread_query_success(slack_app, channel_id, message_ts)

@app.task
def pg_dump(event, database_name, _=None):
    pass


COMMAND_ACTIONS = {
    "PG_READ": pg_read,
    "PG_WRITE": pg_write,
    "PG_DUMP": pg_dump,
}

def extract_command_and_code(text):
    # Get the lines of the message
    lines = text.split("\n")
    # Pop the first line
    first_line = lines.pop(0)
    # Get the command and the code block, keep in mind that there might be other lines in between in the code block
    command = None
    code_block = ""
    code_block_started = False
    for line in lines:
        if line.startswith("&gt; "):
            command = line
        if command is not None:
            if line.startswith("```") and line.endswith("```"):
                code_block = line
                break
            elif line.startswith("```") or line.endswith("```"):
                if code_block_started:
                    code_block = f"{code_block}\n{line}"
                    break
                code_block_started = True
            if code_block_started:
                code_block = f"{code_block}\n{line}"
    if command is None or command == "":
        raise Exception("No command found")
    if code_block is None or code_block == "":
        raise Exception("No code block found")

    # Get the command and the database name
    command = command.replace("&gt; ", "").strip()
    command, database_name = command.split(" ")
    database_name = database_name.replace(":", "")
    print(f"Command: {command}")
    print(f"Database name: {database_name}")
    return command, database_name, code_block


# Add adapter for mentions of bot
@slack_app.event("app_mention")
def app_mention(event, say):
    channel_id = event.get('channel')
    user_id = event.get('user')
    text = event.get('text')
    ts = event.get('ts')
    # Check if the message is edited
    if event.get('edited') is not None:
        print(f"Message is edited, ignoring.")
        return
    # Check if message author is bot
    if event.get('bot_id') is not None or user_id == BOT_ID:
        print(f"Message is from bot, ignoring.")
        return
    print(f"Received mention from user {user_id} in channel {channel_id} at {ts}. Message: {text}")
    if channel_id not in CHANNELS:
        print(f"Channel {channel_id} is not in CHANNELS, ignoring.")
        return
    
    command, database_name, code_block = None, None, None

    try:
        # Add emoji reaction to message
        slack_app.client.reactions_add(
            channel=channel_id,
            name="eyes",
            timestamp=ts
        )
        command, database_name, code_block = extract_command_and_code(text)
        # If there is a "DROP" in the query, then ignore the message
        if "DROP" in code_block.upper():
            print(f"DROP in query, ignoring.")

            text = f"`DROP` in query, very nasty! :alert:"
            say(text=text, thread_ts=ts)
            # Remove the `:eyes:` emoji reaction from the message and add a `:computerrage:` emoji reaction to the message
            slack_app.client.reactions_remove(
                channel=channel_id,
                name="eyes",
                timestamp=ts
            )
            try:
                slack_app.client.reactions_add(
                    channel=channel_id,
                    name="x",
                    timestamp=ts
                )
            except Exception as e:
                print(f"Failed to add reaction: {e}")
            return
    except Exception as e:
        # print(f"Failed to get command and database name: {e}")

        # Reply to the thread with a message that says "Failed to parse message" with the error in a code block
        text = f"Failed to parse message: {e}"
        say(text=text, thread_ts=ts)
        # Remove the `:eyes:` emoji reaction from the message and add a `:computerrage:` emoji reaction to the message
        slack_app.client.reactions_remove(
            channel=channel_id,
            name="eyes",
            timestamp=ts
        )
        # if message does not already have a `:computerrage:` emoji reaction
        try:
            slack_app.client.reactions_add(
                channel=channel_id,
                name="computerrage",
                timestamp=ts
            )
        except Exception as e:
            print(f"Failed to add reaction: {e}")
        return
    
    # Get the user who needs to approve the query
    approval_users = APPROVED_USERS.get(command, None)
    if approval_users is None:
        # Send a message to the channel that says "Invalid command <command>"
        text = f"Invalid command {command}"
        say(text=text)
        # Remove the `:eyes:` emoji reaction from the message and add a `:computerrage:` emoji reaction to the message
        slack_app.client.reactions_remove(
            channel=channel_id,
            name="eyes",
            timestamp=ts
        )
        try:
            slack_app.client.reactions_add(
                channel=channel_id,
                name="computerrage",
                timestamp=ts
            )
        except Exception as e:
            print(f"Failed to add reaction: {e}")
        return
    print(f"Approval user: {approval_users}")

    # Reply to the thread with a message that says task to do and append "Waiting for approval from <@user>..."
    text = f"Tasks to do:\n&gt; {command} {database_name}:"
    text = f"{text}\n{code_block}"
    if command in ("PG_READ", "PG_WRITE"):
        # Run the query in preprod and get the results and time taken
        query = slack_code_block_to_text(code_block)
        now = time.time()
        results = run_return_sql(slack_app, channel_id, ts, database_name, query, read_only=command=="PG_READ", prod=False)
        time_taken = time.time() - now
        if results is None:
            return
        if command == "PG_READ" and results != "No results found":
            # Get results count
            results_count = len(results) - 1
            results = f"Total rows: {results_count}"
        # Add the results and time taken to the message
        text = f"{text}\n\n&gt; Example output on preprod:\n```{results}```\n&gt; Time taken: {time_taken}"
    
    text_appendix = f"Waiting for approval from {', '.join([f'<@{user}>' for user in approval_users])}..."
    text = f"{text}\n\n{text_appendix}"
    say(text=text, thread_ts=ts)


# Add adapter for reactions to messages
@slack_app.event("reaction_added")
def reaction_added(event, say):
    channel_id = event.get('item', {}).get('channel')
    user_id = event.get('user')
    reaction = event.get('reaction')
    ts = event.get('item', {}).get('ts')
    # Check if message author is bot
    if event.get('bot_id') is not None or user_id == BOT_ID:
        print(f"Message is from bot, ignoring.")
        return
    print(f"Received reaction {reaction} from user {user_id} in channel {channel_id} at {ts}.")
    if channel_id not in CHANNELS:
        print(f"Channel {channel_id} is not in CHANNELS, ignoring.")
        return
    # Check if the reaction is not a `:white_check_mark:` emoji
    if reaction != "white_check_mark":
        print(f"Reaction is not a `:white_check_mark:` emoji, ignoring.")
        return
    else:
        # If there are multiple `:white_check_mark:` emoji reactions is this message, then ignore the reaction
        reactions = slack_app.client.reactions_get(
            channel=channel_id,
            timestamp=ts,
        )
        reactions = reactions.get('message', {}).get('reactions', [])
        white_check_mark_reactions_count = 0
        white_check_mark_reactions_user = ""
        for reaction in reactions:
            if reaction.get('name') == "white_check_mark":
                white_check_mark_reactions_count = reaction.get('count')
                white_check_mark_reactions_user = reaction.get('users')[0]
                break
        if white_check_mark_reactions_count != 1 or white_check_mark_reactions_user != user_id:
            print(f"Message already has multiple `:white_check_mark:` emoji reactions, ignoring.")
            return
    # Reaction must be added to bot's message
    if event.get('item_user') != BOT_ID:
        print(f"Reaction is not added to bot's message, ignoring.")
        return
    
    # Get the message
    message = slack_app.client.conversations_history(
        channel=channel_id,
        latest=ts,
        limit=1,
        inclusive=True,
    )
    message = message.get('messages', [])[0]
    message_ts = message.get('ts')
    text = message.get('text')
    # Check if the message is edited
    if message.get('edited') is not None:
        print(f"Message is edited, ignoring.")
        return
    
    # Check if the message already has a `:white_check_mark:` or `:x:` or `:60fps_parrot:` emoji reaction
    main_msg_reactions = message.get('reactions', [])
    for reaction in main_msg_reactions:
        if reaction.get('name') in ["white_check_mark", "x", "60fps_parrot"]:
            print(f"Message already has a `:white_check_mark:` or `:x:` or `:60fps_parrot:` emoji reaction, ignoring.")
            return
        
    # Check if the message is from the bot
    if message.get('bot_id') is not None or user_id == BOT_ID:
        print(f"Message is from bot, ignoring.")
        return

    command, argument, code_block = None, None, None

    try:
        # Add emoji reaction to message
        try:
            slack_app.client.reactions_add(
                channel=channel_id,
                name="eyes",
                timestamp=ts
            )
        except Exception as e:
            print(f"Failed to add reaction: {e}")
        command, argument, code_block = extract_command_and_code(text)
    except Exception as e:
        # print(f"Failed to get command and database name: {e}")

        # Reply to the thread with a message that says "Failed to parse message" with the error in a code block
        text = f"Failed to parse message: {e}"
        say(text=text, thread_ts=ts)
        # Remove the `:eyes:` emoji reaction from the message and add a `:computerrage:` emoji reaction to the message
        slack_app.client.reactions_remove(
            channel=channel_id,
            name="eyes",
            timestamp=ts
        )
        # if message does not already have a `:computerrage:` emoji reaction
        try:
            slack_app.client.reactions_add(
                channel=channel_id,
                name="computerrage",
                timestamp=ts
            )
        except Exception as e:
            print(f"Failed to add reaction: {e}")
        return
    
    # Check if reacted user is in appropriate approval users
    if user_id not in APPROVED_USERS[command]:
        print(f"User {user_id} is not in APPROVED_USERS[{command}], ignoring.")
        return
    
    # Reply to the thread with a message that says "I hear ya <@user>! Approved by <@{user_id}>. Standby..."
    text = f"I hear ya <@{message.get('user')}>! Approved by <@{user_id}>. Standby..."
    say(text=text, thread_ts=ts)

    # Do according actions and pass arguments
    COMMAND_ACTIONS[command](event, ts, argument, code_block)




if __name__ == "__main__":
    flask_app.run(debug=True, port=5001, host="0.0.0.0")

